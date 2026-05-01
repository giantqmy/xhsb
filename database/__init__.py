"""船弦号数据库 — 可插拔数据源 + FAISS 向量库 + 自动变更检测

支持两种数据后端（通过 config.yaml 中 database.backend 切换）：
  - csv   : 原始 CSV 文件（默认，完全向后兼容）
  - sqlite: SQLite 数据库（支持 Web CRUD）
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any, Mapping

from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings

from config import load_config

logger = logging.getLogger(__name__)

HASH_FILE_NAME = ".db_hash"


class DashScopeEmbeddings(Embeddings):
    """DashScope Embedding 封装，直接调用 OpenAI 兼容模式 API。"""

    def __init__(self, model: str, api_key: str, base_url: str):
        if not api_key or api_key.startswith("your-"):
            raise ValueError(
                "Embedding API Key 未配置。请在 config.yaml 中设置 embed.api_key，"
                "或在 .env 中设置 EMBED_API_KEY。"
            )
        self.model = model
        self.api_key = api_key
        self._url = f"{base_url.rstrip('/')}/embeddings"
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        import httpx
        import time

        max_retries = 3
        batch_size = 10
        all_embeddings: list[list[float]] = []

        for batch_start in range(0, len(texts), batch_size):
            batch = texts[batch_start : batch_start + batch_size]
            last_error: Exception | None = None

            for attempt in range(max_retries):
                try:
                    payload = {"model": self.model, "input": batch}
                    resp = httpx.post(
                        self._url,
                        headers=self._headers,
                        json=payload,
                        timeout=60,
                    )
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                        logger.warning("Embedding API 限流，%ds 后重试 (%d/%d)", retry_after, attempt + 1, max_retries)
                        time.sleep(retry_after)
                        continue
                    if resp.status_code >= 500:
                        logger.warning("Embedding API 服务错误 [%d]，%ds 后重试 (%d/%d)", resp.status_code, 2 ** attempt, attempt + 1, max_retries)
                        time.sleep(2 ** attempt)
                        continue
                    if not resp.is_success:
                        try:
                            err_body = resp.json()
                            err_msg = err_body.get("error", {}).get("message", resp.text[:300])
                        except Exception:
                            err_msg = resp.text[:300]
                        raise RuntimeError(
                            f"Embedding API 返回 {resp.status_code}: {err_msg}\n"
                            f"请检查 config.yaml 中 embed 配置（model / api_key / base_url）。"
                        )
                    data = resp.json()
                    batch_embeddings = [item["embedding"] for item in data["data"]]
                    all_embeddings.extend(batch_embeddings)
                    break
                except (httpx.TimeoutException, httpx.NetworkError) as e:
                    last_error = e
                    wait = 2 ** attempt
                    logger.warning("Embedding API 网络错误: %s，%ds 后重试 (%d/%d)", e, wait, attempt + 1, max_retries)
                    time.sleep(wait)
            else:
                raise RuntimeError(f"Embedding API 调用失败，已重试 {max_retries} 次") from last_error

        return all_embeddings

    def embed_query(self, text: str) -> list[float]:
        return self.embed_documents([text])[0]


def _create_source(config: dict[str, Any], db_path: str | None = None):
    """根据配置创建对应的数据源实例"""
    from .csv_source import CsvShipSource
    from .sql_source import SqlShipSource

    db_cfg = config.get("database", {})
    backend = db_cfg.get("backend", "csv")

    if backend == "sqlite":
        sql_path = db_path or db_cfg.get("sqlite_path", "./data/ships.db")
        logger.info("使用 SQLite 数据源: %s", sql_path)
        return SqlShipSource(sql_path)
    else:
        csv_path = db_path or config.get("app", {}).get("ship_db_path", "./data/ships.csv")
        logger.info("使用 CSV 数据源: %s", csv_path)
        return CsvShipSource(csv_path)


class ShipDatabase:
    """
    船弦号数据库 — 双通道检索：
      1. 精确查找（dict，O(1)）
      2. FAISS 向量语义检索（RAG）

    数据源：可插拔（CSV / SQLite）
    自动变更检测：通过 MD5 哈希比对，数据变更时自动重建向量库。
    """

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        db_path: str | None = None,
    ):
        if config is None:
            config = load_config()

        self._config = config

        embed_cfg = config.get("embed", {})
        retrieval_cfg = config.get("retrieval", {})
        vs_cfg = config.get("vector_store", {})

        # ── 创建数据源 ──
        self._source = _create_source(config, db_path)

        # ── 加载数据到内存缓存 ──
        self._data = self._source.load_all()

        # ── Embedding 配置（懒初始化，不影响 CRUD 操作）──
        self._embed_cfg = embed_cfg
        self._embeddings: Embeddings | None = None

        # ── 检索参数 ──
        self._top_k = retrieval_cfg.get("top_k", 3)
        self._score_threshold = retrieval_cfg.get("score_threshold", 0.5)

        # ── 向量库配置 ──
        self._persist_path = vs_cfg.get("persist_path", "./vector_store")
        self._auto_rebuild = vs_cfg.get("auto_rebuild", False)

        # ── 向量库（懒加载） ──
        self._vector_store: FAISS | None = None

    # ── 数据指纹（兼容 CSV 和 SQLite）────────────────────

    def _compute_data_hash(self) -> str:
        """计算当前数据的哈希值，用于变更检测"""
        content = "\n".join(f"{k}|{v}" for k, v in sorted(self._data.items()))
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def _load_saved_hash(self) -> str | None:
        hash_file = Path(self._persist_path) / HASH_FILE_NAME
        if hash_file.exists():
            return hash_file.read_text(encoding="utf-8").strip()
        return None

    def _save_hash(self, data_hash: str) -> None:
        persist_dir = Path(self._persist_path)
        persist_dir.mkdir(parents=True, exist_ok=True)
        (persist_dir / HASH_FILE_NAME).write_text(data_hash, encoding="utf-8")

    def _data_changed(self) -> bool:
        current_hash = self._compute_data_hash()
        saved_hash = self._load_saved_hash()
        changed = current_hash != saved_hash
        if changed:
            logger.info("数据变更检测: 数据已修改，将重建向量库")
        return changed

    # ── 向量库构建 ─────────────────────────────

    def _build_documents(self) -> list[Document]:
        docs = []
        for hn, desc in self._data.items():
            content = f"弦号 {hn}\n{desc}"
            docs.append(Document(
                page_content=content,
                metadata={"hull_number": hn, "description": desc},
            ))
        return docs

    def _load_or_build_vector_store(self) -> FAISS:
        persist_dir = Path(self._persist_path)
        index_file = persist_dir / "index.faiss"

        data_changed = self._data_changed()

        if not self._auto_rebuild and not data_changed and index_file.exists():
            try:
                logger.info("从 %s 加载向量库缓存…", persist_dir)
                vs = FAISS.load_local(
                    str(persist_dir),
                    self._get_embeddings(),
                    allow_dangerous_deserialization=True,
                )
                logger.info("向量库缓存加载成功")
                return vs
            except Exception as e:
                logger.warning("缓存加载失败（%s），将重新构建", e)

        # 数据变化时，重新加载
        if data_changed:
            self._data = self._source.load_all()

        docs = self._build_documents()
        logger.info("正在构建 FAISS 向量库（%d 条文档）…", len(docs))
        vs = FAISS.from_documents(docs, self._get_embeddings())

        persist_dir.mkdir(parents=True, exist_ok=True)
        vs.save_local(str(persist_dir))

        self._save_hash(self._compute_data_hash())
        logger.info("向量库已持久化到 %s，哈希已更新", persist_dir)

        return vs

    def _get_embeddings(self) -> Embeddings:
        """懒初始化 Embedding 客户端（首次调用语义检索时才创建）"""
        if self._embeddings is None:
            self._embeddings = DashScopeEmbeddings(
                model=self._embed_cfg.get("model", "Qwen3-Embedding-0.6B"),
                api_key=self._embed_cfg.get("api_key", ""),
                base_url=self._embed_cfg.get("base_url", "http://localhost:7891/v1"),
            )
        return self._embeddings

    @property
    def vector_store(self) -> FAISS:
        if self._vector_store is None or self._data_changed():
            self._vector_store = self._load_or_build_vector_store()
        return self._vector_store

    # ── 精确查找 ──────────────────────────────

    def lookup(self, hull_number: str) -> str | None:
        return self._source.lookup(hull_number)

    # ── 语义检索 ──────────────────────────────

    def semantic_search(self, query: str, top_k: int | None = None) -> list[dict]:
        k = top_k or self._top_k
        results_with_score = self.vector_store.similarity_search_with_score(query, k=k)

        results = []
        for doc, distance in results_with_score:
            score = float(1.0 / (1.0 + distance))
            results.append({
                "hull_number": doc.metadata["hull_number"],
                "description": doc.metadata["description"],
                "score": round(score, 4),
            })
        return results

    def semantic_search_filtered(self, query: str) -> list[dict]:
        results = self.semantic_search(query, top_k=self._top_k)
        return [r for r in results if r["score"] >= self._score_threshold]

    # ── CRUD 操作（委托给数据源）─────────────────────

    def add_ship(self, hull_number: str, description: str) -> bool:
        """新增船只，成功返回 True"""
        result = self._source.add(hull_number, description)
        if result:
            self._invalidate_cache()
        return result

    def update_ship(self, hull_number: str, description: str) -> bool:
        """更新船只描述，成功返回 True"""
        result = self._source.update(hull_number, description)
        if result:
            self._invalidate_cache()
        return result

    def delete_ship(self, hull_number: str) -> bool:
        """删除船只，成功返回 True"""
        result = self._source.delete(hull_number)
        if result:
            self._invalidate_cache()
        return result

    def reload(self) -> None:
        """强制重新加载数据并重建向量库"""
        self._data = self._source.load_all()
        self._vector_store = None  # 下次访问时重建

    def _invalidate_cache(self) -> None:
        """数据变更后使缓存失效"""
        self._data = self._source.load_all()
        self._vector_store = None

    # ── 属性 ──────────────────────────────────

    @property
    def source(self):
        """获取底层数据源实例（供 Web 层使用）"""
        return self._source

    @property
    def hull_numbers(self) -> list[str]:
        return list(self._data.keys())

    @property
    def descriptions(self) -> list[str]:
        return list(self._data.values())

    @property
    def items(self) -> Mapping[str, str]:
        return self._data

    def __len__(self) -> int:
        return len(self._data)
