"""SQLite 数据源后端 — 使用 SQLite 存储船只数据"""

from __future__ import annotations

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
            # 检查是否为空库，空库则导入默认数据
            count = conn.execute("SELECT COUNT(*) FROM ships").fetchone()[0]
            if count == 0:
                logger.info("SQLite 数据库为空，导入默认船只数据 (%d 条)", len(DEFAULT_SHIPS))
                conn.executemany(
                    "INSERT INTO ships (hull_number, description) VALUES (?, ?)",
                    list(DEFAULT_SHIPS.items()),
                )
        logger.info("SQLite 数据库就绪: %s", self._db_path)

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
        """严格新增：已存在返回 False。"""
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
        """插入或更新：不存在则新增，已存在则覆盖描述。返回 'inserted' 或 'updated'。"""
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

    @property
    def db_path(self) -> Path:
        return self._db_path
