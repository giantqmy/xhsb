"""配置和数据库的单元测试（不需要 LLM / Embedding API）"""

import hashlib
import json
from pathlib import Path

from config import load_config
from database import ShipDatabase, _cosine_similarity
from database.csv_source import CsvShipSource
from database.sql_source import SqlShipSource


# ══════════════════════════════════════════════
#  Config 测试
# ══════════════════════════════════════════════

class TestLoadConfig:
    def test_defaults(self):
        c = load_config(config_path="/nonexistent/config.yaml")
        assert c["llm"]["model"] == "Qwen/Qwen3-VL-4B-AWQ"
        assert c["database"]["backend"] == "sqlite"

    def test_load_from_yaml(self, tmp_path):
        import yaml
        cfg_data = {"llm": {"model": "test-model"}, "retrieval": {"top_k": 10}}
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump(cfg_data, allow_unicode=True), encoding="utf-8")
        c = load_config(config_path=str(cfg_file))
        assert c["llm"]["model"] == "test-model"
        assert c["retrieval"]["top_k"] == 10


# ══════════════════════════════════════════════
#  余弦相似度测试
# ══════════════════════════════════════════════

class TestCosineSimilarity:
    def test_identical(self):
        assert _cosine_similarity([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == 1.0

    def test_opposite(self):
        assert _cosine_similarity([1.0, 0.0], [-1.0, 0.0]) == -1.0

    def test_orthogonal(self):
        assert _cosine_similarity([1.0, 0.0], [0.0, 1.0]) == 0.0

    def test_zero_vector(self):
        assert _cosine_similarity([0.0, 0.0], [1.0, 2.0]) == 0.0


# ══════════════════════════════════════════════
#  CSV 数据源测试
# ══════════════════════════════════════════════

class TestCsvShipSource:
    def test_lookup_existing(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("hull_number,description\n0014,白色客轮\n", encoding="utf-8")
        assert CsvShipSource(csv_path).lookup("0014") == "白色客轮"

    def test_lookup_missing(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("hull_number,description\n0014,测试\n", encoding="utf-8")
        assert CsvShipSource(csv_path).lookup("XXXX") is None

    def test_crud(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("hull_number,description\n", encoding="utf-8")
        s = CsvShipSource(csv_path)
        assert s.add("A", "船A") is True
        assert s.add("A", "重复") is False
        assert s.update("A", "新A") is True
        assert s.lookup("A") == "新A"
        assert s.delete("A") is True
        assert s.lookup("A") is None

    def test_upsert(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("hull_number,description\n0014,旧\n", encoding="utf-8")
        s = CsvShipSource(csv_path)
        assert s.upsert("0014", "新") == "updated"
        assert s.upsert("NEW", "全新") == "inserted"

    def test_auto_create(self, tmp_path):
        s = CsvShipSource(tmp_path / "new" / "ships.csv")
        assert s.count() > 0

    def test_bom(self, tmp_path):
        p = tmp_path / "bom.csv"
        p.write_bytes(b'\xef\xbb\xbf' + "hull_number,description\nB001,BOM船\n".encode("utf-8"))
        assert CsvShipSource(p).lookup("B001") == "BOM船"


# ══════════════════════════════════════════════
#  SQLite 数据源测试
# ══════════════════════════════════════════════

class TestSqlShipSource:
    def test_crud(self, tmp_path):
        s = SqlShipSource(tmp_path / "test.db")
        assert s.add("NEW", "新船") is True
        assert s.exists("NEW") is True
        assert s.update("NEW", "更新") is True
        assert s.lookup("NEW") == "更新"
        assert s.delete("NEW") is True
        assert s.lookup("NEW") is None

    def test_default_data(self, tmp_path):
        s = SqlShipSource(tmp_path / "test.db")
        assert s.count() == 9
        assert s.lookup("0014") is not None

    def test_search_by_description(self, tmp_path):
        s = SqlShipSource(tmp_path / "test.db")
        results = s.search_by_description("客轮")
        assert len(results) > 0

    def test_embedding_crud(self, tmp_path):
        """embedding 表的增删查"""
        s = SqlShipSource(tmp_path / "test.db")
        vec = [0.1, 0.2, 0.3]
        s.store_embedding("0014", vec)
        loaded = s.load_embedding("0014")
        assert loaded == vec
        assert s.embedding_count() == 1

        # 更新
        new_vec = [0.4, 0.5, 0.6]
        s.store_embedding("0014", new_vec)
        assert s.load_embedding("0014") == new_vec
        assert s.embedding_count() == 1

        # 删除
        assert s.delete_embedding("0014") is True
        assert s.load_embedding("0014") is None

    def test_embedding_bulk(self, tmp_path):
        s = SqlShipSource(tmp_path / "test.db")
        records = {"0014": [0.1, 0.2], "0025": [0.3, 0.4]}
        count = s.store_embeddings_bulk(records)
        assert count == 2
        all_emb = s.load_all_embeddings()
        assert len(all_emb) == 2

    def test_embedding_cascade_delete(self, tmp_path):
        """删除船只时 embedding 应级联删除"""
        s = SqlShipSource(tmp_path / "test.db")
        s.store_embedding("0014", [1.0, 2.0])
        assert s.embedding_count() == 1
        s.delete("0014")
        # 外键级联删除
        assert s.load_embedding("0014") is None


# ══════════════════════════════════════════════
#  ShipDatabase 抽象层测试
# ══════════════════════════════════════════════

def _make_config(tmp_path: Path, backend: str = "sqlite") -> dict:
    return {
        "embed": {"model": "test", "api_key": "test", "base_url": "https://example.com/v1"},
        "retrieval": {"top_k": 3, "score_threshold": 0.5},
        "vector_store": {"persist_path": str(tmp_path / "vs"), "auto_rebuild": False},
        "database": {"backend": backend, "sqlite_path": str(tmp_path / "ships.db")},
        "app": {"log_level": "INFO", "ship_db_path": str(tmp_path / "ships.csv")},
    }


class TestShipDatabaseSQLite:
    def test_lookup(self, tmp_path):
        db = ShipDatabase(config=_make_config(tmp_path))
        assert db.lookup("0014") is not None

    def test_crud(self, tmp_path):
        db = ShipDatabase(config=_make_config(tmp_path))
        assert db.add_ship("T1", "测试") is True
        assert db.lookup("T1") == "测试"
        assert db.update_ship("T1", "更新") is True
        assert db.delete_ship("T1") is True
        assert db.lookup("T1") is None

    def test_len(self, tmp_path):
        db = ShipDatabase(config=_make_config(tmp_path))
        assert len(db) == 9

    def test_embed_store_is_same_source(self, tmp_path):
        """SQLite 后端的 embed_store 应该是同一个 source"""
        db = ShipDatabase(config=_make_config(tmp_path))
        assert db.embed_store is db.source

    def test_data_hash(self, tmp_path):
        db = ShipDatabase(config=_make_config(tmp_path))
        h1 = db._compute_data_hash()
        db.add_ship("NEW", "新")
        h2 = db._compute_data_hash()
        assert h1 != h2


class TestShipDatabaseCSV:
    def test_lookup(self, tmp_path):
        cfg = _make_config(tmp_path, backend="csv")
        db = ShipDatabase(config=cfg)
        assert db.lookup("0014") is not None

    def test_crud(self, tmp_path):
        cfg = _make_config(tmp_path, backend="csv")
        db = ShipDatabase(config=cfg)
        assert db.add_ship("T1", "测试") is True
        assert db.delete_ship("T1") is True

    def test_embed_store_is_sqlite(self, tmp_path):
        """CSV 后端的 embed_store 应该是独立的 SqlShipSource"""
        cfg = _make_config(tmp_path, backend="csv")
        db = ShipDatabase(config=cfg)
        assert isinstance(db.embed_store, SqlShipSource)


class TestBackendSwitch:
    def test_both_backends_same_behavior(self, tmp_path):
        for backend in ["csv", "sqlite"]:
            cfg = _make_config(tmp_path / backend, backend=backend)
            db = ShipDatabase(config=cfg)
            assert len(db) == 9
            assert db.lookup("0014") is not None
            assert db.add_ship("T1", "测试") is True
            assert db.lookup("T1") == "测试"
            assert db.delete_ship("T1") is True
