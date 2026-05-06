"""船弦号数据库 — 可插拔数据源 + SQLite embedding 向量检索 + 自动变更检测

支持两种数据后端（通过 config.yaml 中 database.backend 切换）：
  - csv   : 原始 CSV 文件（完全向后兼容，embedding 存独立 SQLite）
  - sqlite: SQLite 数据库（embedding 直接存在同一库中）

语义检索：embedding 存储在 SQLite 中，查询时加载到内存计算余弦相似度。
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any, Mapping

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


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """计算两个向量的余弦相似度"""
    import math
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


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


def _get_embedding_store(config: dict[str, Any], source) -> "SqlShipSource | None":
    """获取 embedding 存储后端。
    - SQLite 后端：embedding 直接存在同一库中
    - CSV 后端：embedding 存在独立的 SQLite 文件中
    """
    from .sql_source import SqlShipSource

    db_cfg = config.get("database", {})
    backend = db_cfg.get("backend", "csv")

    if backend == "sqlite":
        # embedding 直接用同一个 SqlShipSource
        return source
    else:
        # CSV 后端：用独立的 SQLite 文件存 embedding
        embed_db_path = db_cfg.get("sqlite_path", "./data/ships.db")
        logger.info("CSV 后端使用独立 SQLite 存储 embedding: %s", embed_db_path)
        return SqlShipSource(embed_db_path)


class ShipDatabase:
    """
    船弦号数据库 — 双通道检索：
      1. 精确查找（dict，O(1)）
      2. SQLite embedding 语义检索（余弦相似度）

    embedding 存储在 SQLite 的 ship_embeddings 表中，
    查询时加载到内存计算余弦相似度。
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

        # ── 创建数据源 ──
        self._source = _create_source(config, db_path)

        # ── 加载数据到内存缓存 ──
        self._data = self._source.load_all()

        # ── Embedding 存储后端 ──
        self._embed_store = _get_embedding_store(config, self._source)

        # ── Embedding 配置（懒初始化）──
        self._embed_cfg = embed_cfg
        self._embeddings: Embeddings | None = None

        # ── 检索参数 ──
        self._top_k = retrieval_cfg.get("top_k", 3)
        self._score_threshold = retrieval_cfg.get("score_threshold", 0.5)

        # ── 内存 embedding 缓存 ──
        self._embedding_cache: dict[str, list[float]] | None = None

        # ── 数据指纹 ──
        self._persist_path = config.get("vector_store", {}).get("persist_path", "./vector_store")

    # ── 数据指纹（变更检测）─────────────────────

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
            logger.info("数据变更检测: 数据已修改")
        return changed

    # ── Embedding 管理 ──────────────────────────

    def _get_embeddings(self) -> Embeddings:
        """懒初始化 Embedding 客户端"""
        if self._embeddings is None:
            self._embeddings = DashScopeEmbeddings(
                model=self._embed_cfg.get("model", "Qwen3-Embedding-0.6B"),
                api_key=self._embed_cfg.get("api_key", ""),
                base_url=self._embed_cfg.get("base_url", "http://localhost:7891/v1"),
            )
        return self._embeddings

    def build_embeddings(self, force: bool = False) -> int:
        """为所有船只数据生成 embedding 并存入 SQLite。
        跳过已有 embedding 的记录（除非 force=True）。
        返回新生成的 embedding 数量。
        """
        self._data = self._source.load_all()
        if not self._data:
            logger.info("无数据，跳过 embedding 构建")
            return 0

        existing = self._embed_store.load_all_embeddings()

        # 找出需要生成 embedding 的记录
        if force:
            to_embed = dict(self._data)
        else:
            to_embed = {
                hn: desc for hn, desc in self._data.items()
                if hn not in existing
            }

        if not to_embed:
            logger.info("所有记录已有 embedding，跳过构建")
            return 0

        logger.info("需要生成 embedding 的记录: %d 条", len(to_embed))

        # 批量生成 embedding
        texts = [f"弦号 {hn}\n{desc}" for hn, desc in to_embed.items()]
        embeddings = self._get_embeddings().embed_documents(texts)

        # 存入 SQLite
        records = dict(zip(to_embed.keys(), embeddings))
        count = self._embed_store.store_embeddings_bulk(records)

        # 更新缓存
        self._embedding_cache = None

        logger.info("已生成并存储 %d 条 embedding", count)
        return count

    def _load_embedding_cache(self) -> dict[str, list[float]]:
        """加载 embedding 到内存缓存"""
        if self._embedding_cache is None:
            self._embedding_cache = self._embed_store.load_all_embeddings()
        return self._embedding_cache

    # ── 精确查找 ──────────────────────────────

    def lookup(self, hull_number: str) -> str | None:
        return self._source.lookup(hull_number)

    # ── 语义检索（SQLite embedding + 余弦相似度）──

    def semantic_search(self, query: str, top_k: int | None = None) -> list[dict]:
        """语义检索：用 query 的 embedding 与 SQLite 中存储的 embedding 计算余弦相似度"""
        k = top_k or self._top_k

        # 1. 生成 query 的 embedding
        query_embedding = self._get_embeddings().embed_query(query)

        # 2. 加载所有 embedding
        all_embeddings = self._load_embedding_cache()

        if not all_embeddings:
            logger.warning("无 embedding 数据，请先调用 build_embeddings()")
            return []

        # 3. 计算余弦相似度
        scored = []
        for hn, vec in all_embeddings.items():
            score = _cosine_similarity(query_embedding, vec)
            scored.append((hn, score))

        # 4. 按相似度降序排序，取 top_k
        scored.sort(key=lambda x: x[1], reverse=True)
        top_results = scored[:k]

        # 5. 组装结果
        results = []
        for hn, score in top_results:
            desc = self._data.get(hn) or self._source.lookup(hn) or ""
            results.append({
                "hull_number": hn,
                "description": desc,
                "score": round(score, 4),
            })

        return results

    def semantic_search_filtered(self, query: str) -> list[dict]:
        results = self.semantic_search(query, top_k=self._top_k)
        return [r for r in results if r["score"] >= self._score_threshold]

    # ── CRUD 操作 ──────────────────────────────

    def add_ship(self, hull_number: str, description: str) -> bool:
        result = self._source.add(hull_number, description)
        if result:
            self._invalidate_cache()
        return result

    def update_ship(self, hull_number: str, description: str) -> bool:
        result = self._source.update(hull_number, description)
        if result:
            self._invalidate_cache()
        return result

    def delete_ship(self, hull_number: str) -> bool:
        result = self._source.delete(hull_number)
        if result:
            # 同时删除 embedding
            if hasattr(self._embed_store, 'delete_embedding'):
                self._embed_store.delete_embedding(hull_number)
            self._invalidate_cache()
        return result

    def upsert_ship(self, hull_number: str, description: str) -> str:
        result = self._source.upsert(hull_number, description)
        self._invalidate_cache()
        return result

    def reload(self) -> None:
        self._data = self._source.load_all()
        self._embedding_cache = None

    def _invalidate_cache(self) -> None:
        self._data = self._source.load_all()
        self._embedding_cache = None

    # ── 属性 ──────────────────────────────────

    @property
    def source(self):
        return self._source

    @property
    def embed_store(self):
        """获取 embedding 存储后端"""
        return self._embed_store

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
