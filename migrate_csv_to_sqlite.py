"""
CSV → SQLite 数据迁移工具

用法：
    python migrate_csv_to_sqlite.py [--csv ./data/ships.csv] [--db ./data/ships.db]

功能：将 CSV 中的船只数据导入 SQLite 数据库
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path


def migrate(csv_path: str, db_path: str) -> None:
    csv_file = Path(csv_path)
    db_file = Path(db_path)

    if not csv_file.exists():
        print(f"❌ CSV 文件不存在: {csv_file}")
        sys.exit(1)

    # 读取 CSV
    data: dict[str, str] = {}
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            hn = (row.get("hull_number") or "").strip()
            desc = (row.get("description") or "").strip()
            if hn:
                data[hn] = desc

    if not data:
        print("⚠️  CSV 文件为空，无需迁移")
        return

    # 创建 SQLite 数据库
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ships (
            hull_number TEXT PRIMARY KEY,
            description TEXT NOT NULL DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now', 'localtime')),
            updated_at  TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)

    # 导入数据（跳过已存在的）
    added = 0
    skipped = 0
    for hn, desc in data.items():
        try:
            conn.execute(
                "INSERT INTO ships (hull_number, description) VALUES (?, ?)",
                (hn, desc),
            )
            added += 1
        except sqlite3.IntegrityError:
            skipped += 1

    conn.commit()
    conn.close()

    print(f"✅ 迁移完成！")
    print(f"   CSV: {csv_file} ({len(data)} 条)")
    print(f"   SQLite: {db_file}")
    print(f"   新增: {added} 条 | 跳过: {skipped} 条（已存在）")
    print()
    print(f"使用 SQLite 后端：修改 config.yaml 中 database.backend 为 \"sqlite\"")


def main():
    parser = argparse.ArgumentParser(description="CSV → SQLite 数据迁移工具")
    parser.add_argument("--csv", default="./data/ships.csv", help="CSV 文件路径")
    parser.add_argument("--db", default="./data/ships.db", help="SQLite 数据库路径")
    args = parser.parse_args()
    migrate(args.csv, args.db)


if __name__ == "__main__":
    main()
