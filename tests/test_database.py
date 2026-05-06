"""配置和数据库的单元测试（不需要 LLM / Embedding API）"""

import hashlib
from pathlib import Path

from config import load_config
from database import ShipDatabase
from database.csv_source import CsvShipSource
from database.sql_source import SqlShipSource


# ══════════════════════════════════════════════
#  Config 测试
# ══════════════════════════════════════════════

class TestLoadConfig:
    def test_defaults(self):
        """没有 config.yaml 时返回内置默认值"""
        c = load_config(config_path="/nonexistent/config.yaml")
        assert c["llm"]["model"] == "Qwen/Qwen3-VL-4B-AWQ"
        assert c["llm"]["api_key"] == "abc123"
        assert c["llm"]["temperature"] == 0.0
        assert c["embed"]["model"] == "Qwen3-Embedding-0.6B"
        assert c["embed"]["base_url"] == "http://localhost:7891/v1"
        assert c["retrieval"]["top_k"] == 3
        assert c["retrieval"]["score_threshold"] == 0.5
        assert c["vector_store"]["persist_path"] == "./vector_store"
        assert c["vector_store"]["auto_rebuild"] is False
        assert c["app"]["log_level"] == "INFO"
        assert c["database"]["backend"] == "sqlite"

    def test_load_from_yaml(self, tmp_path):
        """从 YAML 文件加载"""
        import yaml
        cfg_data = {
            "llm": {"model": "test-model", "api_key": "test-key"},
            "retrieval": {"top_k": 10},
        }
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump(cfg_data, allow_unicode=True), encoding="utf-8")

        c = load_config(config_path=str(cfg_file))
        assert c["llm"]["model"] == "test-model"
        assert c["llm"]["api_key"] == "test-key"
        assert c["retrieval"]["top_k"] == 10
        # 未覆盖的字段不存在（不会自动合并默认值）
        assert "embed" not in c or c.get("embed", {}).get("model") != "text-embedding-v4"


# ══════════════════════════════════════════════
#  CSV 数据源测试
# ══════════════════════════════════════════════

class TestCsvShipSource:
    def test_lookup_existing(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text(
            "hull_number,description\n"
            "0014,白色大型客轮，上层建筑为蓝色涂装，船尾有直升机停机坪\n"
            "0025,黑色散货船，船体有红色水线\n"
            "9999,测试船\n",
            encoding="utf-8",
        )
        source = CsvShipSource(csv_path)
        assert source.lookup("0014") == "白色大型客轮，上层建筑为蓝色涂装，船尾有直升机停机坪"

    def test_lookup_missing(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text("hull_number,description\n0014,测试船\n", encoding="utf-8")
        source = CsvShipSource(csv_path)
        assert source.lookup("XXXX") is None

    def test_lookup_whitespace(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text("hull_number,description\n0014,测试船\n", encoding="utf-8")
        source = CsvShipSource(csv_path)
        assert source.lookup("  0014  ") == "测试船"

    def test_count(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text(
            "hull_number,description\n0014,A\n0025,B\n9999,C\n",
            encoding="utf-8",
        )
        source = CsvShipSource(csv_path)
        assert source.count() == 3

    def test_add_and_exists(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text("hull_number,description\n", encoding="utf-8")
        source = CsvShipSource(csv_path)
        assert source.add("NEW01", "新船") is True
        assert source.exists("NEW01") is True
        assert source.add("NEW01", "重复") is False  # 已存在

    def test_update(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text("hull_number,description\n0014,旧描述\n", encoding="utf-8")
        source = CsvShipSource(csv_path)
        assert source.update("0014", "新描述") is True
        assert source.lookup("0014") == "新描述"
        assert source.update("XXXX", "不存在") is False

    def test_delete(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text("hull_number,description\n0014,测试\n", encoding="utf-8")
        source = CsvShipSource(csv_path)
        assert source.delete("0014") is True
        assert source.lookup("0014") is None
        assert source.delete("XXXX") is False

    def test_upsert(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text("hull_number,description\n0014,旧描述\n", encoding="utf-8")
        source = CsvShipSource(csv_path)
        assert source.upsert("0014", "新描述") == "updated"
        assert source.upsert("NEW01", "全新") == "inserted"

    def test_bulk_add(self, tmp_path):
        csv_path = tmp_path / "test_ships.csv"
        csv_path.write_text("hull_number,description\n0014,已有\n", encoding="utf-8")
        source = CsvShipSource(csv_path)
        added = source.bulk_add({"0014": "重复", "NEW01": "新1", "NEW02": "新2"})
        assert added == 2  # 跳过 0014

    def test_auto_create_default(self, tmp_path):
        """指向不存在的路径时，应自动创建默认 CSV"""
        csv_path = tmp_path / "new_dir" / "ships.csv"
        source = CsvShipSource(csv_path)
        assert csv_path.exists()
        assert source.count() > 0

    def test_csv_with_bom(self, tmp_path):
        csv_path = tmp_path / "bom_ships.csv"
        content = b'\xef\xbb\xbf' + "hull_number,description\nB001,BOM测试船\n".encode("utf-8")
        csv_path.write_bytes(content)
        source = CsvShipSource(csv_path)
        assert source.lookup("B001") == "BOM测试船"


# ══════════════════════════════════════════════
#  SQLite 数据源测试
# ══════════════════════════════════════════════

class TestSqlShipSource:
    def test_lookup_existing(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        # 默认数据包含 0014
        assert "客轮" in (source.lookup("0014") or "")

    def test_lookup_missing(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        assert source.lookup("XXXX") is None

    def test_add_and_exists(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        assert source.add("NEW01", "新船") is True
        assert source.exists("NEW01") is True
        assert source.add("NEW01", "重复") is False

    def test_update(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        source.add("UPD01", "旧描述")
        assert source.update("UPD01", "新描述") is True
        assert source.lookup("UPD01") == "新描述"
        assert source.update("XXXX", "不存在") is False

    def test_delete(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        source.add("DEL01", "待删")
        assert source.delete("DEL01") is True
        assert source.lookup("DEL01") is None
        assert source.delete("XXXX") is False

    def test_upsert(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        source.add("UP01", "旧")
        assert source.upsert("UP01", "新") == "updated"
        assert source.upsert("INS01", "全新") == "inserted"

    def test_bulk_add(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        source.add("EXIST", "已有")
        added = source.bulk_add({"EXIST": "重复", "NEW1": "新1", "NEW2": "新2"})
        assert added == 2

    def test_search_by_description(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        results = source.search_by_description("客轮")
        assert len(results) > 0
        assert any("客轮" in r["description"] for r in results)

    def test_count(self, tmp_path):
        db_path = tmp_path / "test.db"
        source = SqlShipSource(db_path)
        assert source.count() == 9  # 默认数据 9 条


# ══════════════════════════════════════════════
#  ShipDatabase 抽象层测试（双后端兼容）
# ══════════════════════════════════════════════

def _make_config_csv(tmp_path: Path) -> dict:
    """创建 CSV 后端测试配置"""
    return {
        "embed": {"model": "test", "api_key": "test", "base_url": "https://example.com/v1"},
        "retrieval": {"top_k": 3, "score_threshold": 0.5},
        "vector_store": {"persist_path": str(tmp_path / "vector_store"), "auto_rebuild": False},
        "database": {"backend": "csv", "sqlite_path": str(tmp_path / "ships.db")},
        "app": {"log_level": "INFO", "ship_db_path": str(tmp_path / "ships.csv")},
    }


def _make_config_sqlite(tmp_path: Path) -> dict:
    """创建 SQLite 后端测试配置"""
    return {
        "embed": {"model": "test", "api_key": "test", "base_url": "https://example.com/v1"},
        "retrieval": {"top_k": 3, "score_threshold": 0.5},
        "vector_store": {"persist_path": str(tmp_path / "vector_store"), "auto_rebuild": False},
        "database": {"backend": "sqlite", "sqlite_path": str(tmp_path / "ships.db")},
        "app": {"log_level": "INFO", "ship_db_path": str(tmp_path / "ships.csv")},
    }


class TestShipDatabaseCSV:
    """CSV 后端的 ShipDatabase 测试"""

    def test_lookup(self, tmp_path):
        cfg = _make_config_csv(tmp_path)
        db = ShipDatabase(config=cfg)
        assert db.lookup("0014") is not None

    def test_lookup_missing(self, tmp_path):
        cfg = _make_config_csv(tmp_path)
        db = ShipDatabase(config=cfg)
        assert db.lookup("XXXX") is None

    def test_len(self, tmp_path):
        cfg = _make_config_csv(tmp_path)
        db = ShipDatabase(config=cfg)
        assert len(db) == 9

    def test_add_and_invalidate(self, tmp_path):
        cfg = _make_config_csv(tmp_path)
        db = ShipDatabase(config=cfg)
        assert db.add_ship("NEW01", "新船") is True
        assert db.lookup("NEW01") == "新船"
        assert len(db) == 10

    def test_delete_and_invalidate(self, tmp_path):
        cfg = _make_config_csv(tmp_path)
        db = ShipDatabase(config=cfg)
        assert db.delete_ship("0014") is True
        assert db.lookup("0014") is None

    def test_upsert(self, tmp_path):
        cfg = _make_config_csv(tmp_path)
        db = ShipDatabase(config=cfg)
        assert db.upsert_ship("0014", "新描述") == "updated"
        assert db.upsert_ship("NEW99", "全新") == "inserted"

    def test_build_documents(self, tmp_path):
        cfg = _make_config_csv(tmp_path)
        db = ShipDatabase(config=cfg)
        docs = db._build_documents()
        assert len(docs) == 9
        for doc in docs:
            assert "弦号" in doc.page_content
            assert "hull_number" in doc.metadata

    def test_data_hash_detection(self, tmp_path):
        cfg = _make_config_csv(tmp_path)
        db = ShipDatabase(config=cfg)
        hash1 = db._compute_data_hash()
        db.add_ship("NEW01", "新船")
        hash2 = db._compute_data_hash()
        assert hash1 != hash2


class TestShipDatabaseSQLite:
    """SQLite 后端的 ShipDatabase 测试"""

    def test_lookup(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        assert "客轮" in (db.lookup("0014") or "")

    def test_lookup_missing(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        assert db.lookup("XXXX") is None

    def test_len(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        assert len(db) == 9

    def test_add_and_invalidate(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        assert db.add_ship("NEW01", "新船") is True
        assert db.lookup("NEW01") == "新船"
        assert len(db) == 10

    def test_delete_and_invalidate(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        assert db.delete_ship("0014") is True
        assert db.lookup("0014") is None

    def test_upsert(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        # 默认数据有 0014
        assert db.upsert_ship("0014", "新描述") == "updated"
        assert db.upsert_ship("NEW99", "全新") == "inserted"

    def test_search_by_description(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        # SQLite 后端支持模糊搜索
        source = db.source
        results = source.search_by_description("客轮")
        assert len(results) > 0

    def test_data_hash_detection(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        hash1 = db._compute_data_hash()
        db.add_ship("NEW01", "新船")
        hash2 = db._compute_data_hash()
        assert hash1 != hash2

    def test_source_type(self, tmp_path):
        cfg = _make_config_sqlite(tmp_path)
        db = ShipDatabase(config=cfg)
        assert isinstance(db.source, SqlShipSource)


class TestShipDatabaseBackendSwitch:
    """测试后端切换：同一套 API，不同 backend"""

    def test_csv_and_sqlite_same_results(self, tmp_path):
        """CSV 和 SQLite 后端的基本 CRUD 行为应一致"""
        for make_cfg, label in [(_make_config_csv, "CSV"), (_make_config_sqlite, "SQLite")]:
            cfg = make_cfg(tmp_path / label)
            db = ShipDatabase(config=cfg)

            # 初始数据
            assert len(db) == 9
            assert db.lookup("0014") is not None

            # 新增
            assert db.add_ship("T001", "测试船") is True
            assert db.lookup("T001") == "测试船"
            assert db.add_ship("T001", "重复") is False

            # 更新
            assert db.update_ship("T001", "新描述") is True
            assert db.lookup("T001") == "新描述"

            # 删除
            assert db.delete_ship("T001") is True
            assert db.lookup("T001") is None
