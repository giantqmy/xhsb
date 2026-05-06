"""SQLite 数据源后端 — 使用 SQLite 存储船只数据 + embedding 向量"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Mapping

from .base import ShipDataSource

logger = logging.getLogger(__name__)

DEFAULT_SHIPS = {
    "0014": "白色大型客轮，上层建筑为蓝色涂装，船尾有直升机停机坪",
    "0025": "黑色散货船，船体有红色水线，甲板上配有龙门吊",
    "0123": "白色邮轮，船身有红蓝条纹装饰，三座烟囱",
    "0256": "灰色军舰，隐身外形设计，舰首配有垂直发射系统",
    "0389": "红色渔船，船身有白色编号，甲板配有拖网绞车",
    "0455": "绿色集装箱船，船体涂有大型LOGO，配有四台岸桥吊",
    "0512": "黄色挖泥船，船体宽大，中部有大型绞吸臂",
    "0678": "蓝色油轮，双壳结构，船尾有大型舵机舱",
    "0789": "白色科考船，船尾有A型吊架，甲板有多个实验室舱",
}


class SqlShipSource(ShipDataSource):
    """SQLite 数据源"""

    def __init__(self, db_path: str | Path):
        self._db_path = Path(db_path).resolve()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """初始化数据库表结构"""
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ships (
                    hull_number TEXT PRIMARY KEY,
                    description TEXT NOT NULL DEFAULT '',
                    created_at  TEXT DEFAULT (datetime('now', 'localtime')),
                    updated_at  TEXT DEFAULT (datetime('now', 'localtime'))
                )
            """)
            # embedding 向量表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ship_embeddings (
                    hull_number TEXT PRIMARY KEY,
                    embedding   TEXT NOT NULL,
                    updated_at  TEXT DEFAULT (datetime('now', 'localtime')),
                    FOREIGN KEY (hull_number) REFERENCES ships(hull_number) ON DELETE CASCADE
                )
            """)
            # 检查是否为空库，空库则导入默认数据
            count = conn.execute("SELECT COUNT(*) FROM ships").fetchone()[0]
            if count == 0:
                logger.info("SQLite 数据库为空，导入默认船只数据 (%d 条)", len(DEFAULT_SHIPS))
                conn.executemany(
                    "INSERT INTO ships (hull_number, description) VALUES (?, ?)",
                    list(DEFAULT_SHIPS.items()),
                )
        logger.info("SQLite 数据库就绪: %s", self._db_path)

    # ── CRUD 操作 ──────────────────────────────

    def load_all(self) -> dict[str, str]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT hull_number, description FROM ships ORDER BY hull_number"
            ).fetchall()
        return {row["hull_number"]: row["description"] for row in rows}

    def lookup(self, hull_number: str) -> str | None:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT description FROM ships WHERE hull_number = ?",
                (hull_number.strip(),),
            ).fetchone()
        return row["description"] if row else None

    def add(self, hull_number: str, description: str) -> bool:
        hn = hull_number.strip()
        try:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT INTO ships (hull_number, description) VALUES (?, ?)",
                    (hn, description.strip()),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def upsert(self, hull_number: str, description: str) -> str:
        hn = hull_number.strip()
        with self._get_conn() as conn:
            existing = conn.execute(
                "SELECT 1 FROM ships WHERE hull_number = ?", (hn,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE ships SET description = ?, updated_at = datetime('now', 'localtime') "
                    "WHERE hull_number = ?",
                    (description.strip(), hn),
                )
                return "updated"
            else:
                conn.execute(
                    "INSERT INTO ships (hull_number, description) VALUES (?, ?)",
                    (hn, description.strip()),
                )
                return "inserted"

    def update(self, hull_number: str, description: str) -> bool:
        hn = hull_number.strip()
        with self._get_conn() as conn:
            cursor = conn.execute(
                "UPDATE ships SET description = ?, updated_at = datetime('now', 'localtime') "
                "WHERE hull_number = ?",
                (description.strip(), hn),
            )
        return cursor.rowcount > 0

    def delete(self, hull_number: str) -> bool:
        hn = hull_number.strip()
        with self._get_conn() as conn:
            cursor = conn.execute("DELETE FROM ships WHERE hull_number = ?", (hn,))
        return cursor.rowcount > 0

    def count(self) -> int:
        with self._get_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM ships").fetchone()[0]

    def exists(self, hull_number: str) -> bool:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM ships WHERE hull_number = ?",
                (hull_number.strip(),),
            ).fetchone()
        return row is not None

    def bulk_add(self, records: dict[str, str]) -> int:
        added = 0
        with self._get_conn() as conn:
            for hn, desc in records.items():
                hn = hn.strip()
                if not hn:
                    continue
                try:
                    conn.execute(
                        "INSERT INTO ships (hull_number, description) VALUES (?, ?)",
                        (hn, desc.strip()),
                    )
                    added += 1
                except sqlite3.IntegrityError:
                    continue
        return added

    def items(self) -> Mapping[str, str]:
        return self.load_all()

    def search_by_description(self, keyword: str) -> list[dict]:
        """按描述关键词模糊搜索"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT hull_number, description FROM ships WHERE description LIKE ?",
                (f"%{keyword}%",),
            ).fetchall()
        return [{"hull_number": row["hull_number"], "description": row["description"]} for row in rows]

    # ── Embedding 向量操作 ──────────────────────

    def store_embedding(self, hull_number: str, vector: list[float]) -> None:
        """存储一条 embedding 向量（upsert）"""
        hn = hull_number.strip()
        vec_json = json.dumps(vector, ensure_ascii=False)
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO ship_embeddings (hull_number, embedding, updated_at) "
                "VALUES (?, ?, datetime('now', 'localtime')) "
                "ON CONFLICT(hull_number) DO UPDATE SET embedding = excluded.embedding, "
                "updated_at = excluded.updated_at",
                (hn, vec_json),
            )

    def store_embeddings_bulk(self, records: dict[str, list[float]]) -> int:
        """批量存储 embedding 向量，返回成功数量"""
        count = 0
        with self._get_conn() as conn:
            for hn, vector in records.items():
                hn = hn.strip()
                if not hn:
                    continue
                vec_json = json.dumps(vector, ensure_ascii=False)
                conn.execute(
                    "INSERT INTO ship_embeddings (hull_number, embedding, updated_at) "
                    "VALUES (?, ?, datetime('now', 'localtime')) "
                    "ON CONFLICT(hull_number) DO UPDATE SET embedding = excluded.embedding, "
                    "updated_at = excluded.updated_at",
                    (hn, vec_json),
                )
                count += 1
        return count

    def load_all_embeddings(self) -> dict[str, list[float]]:
        """加载全部 embedding，返回 {hull_number: vector}"""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT hull_number, embedding FROM ship_embeddings"
            ).fetchall()
        result = {}
        for row in rows:
            try:
                result[row["hull_number"]] = json.loads(row["embedding"])
            except json.JSONDecodeError:
                logger.warning("embedding 解析失败: %s", row["hull_number"])
        return result

    def load_embedding(self, hull_number: str) -> list[float] | None:
        """加载单条 embedding"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT embedding FROM ship_embeddings WHERE hull_number = ?",
                (hull_number.strip(),),
            ).fetchone()
        if row:
            try:
                return json.loads(row["embedding"])
            except json.JSONDecodeError:
                return None
        return None

    def delete_embedding(self, hull_number: str) -> bool:
        """删除一条 embedding"""
        with self._get_conn() as conn:
            cursor = conn.execute(
                "DELETE FROM ship_embeddings WHERE hull_number = ?",
                (hull_number.strip(),),
            )
        return cursor.rowcount > 0

    def embedding_count(self) -> int:
        """返回 embedding 记录数"""
        with self._get_conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM ship_embeddings").fetchone()[0]

    def clear_embeddings(self) -> int:
        """清空全部 embedding，返回删除数量"""
        with self._get_conn() as conn:
            cursor = conn.execute("DELETE FROM ship_embeddings")
        return cursor.rowcount

    @property
    def db_path(self) -> Path:
        return self._db_path
