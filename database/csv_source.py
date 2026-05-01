"""CSV 数据源后端 — 从 ships.csv 加载和管理船只数据"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from .base import ShipDataSource

logger = logging.getLogger(__name__)

DEFAULT_CSV_CONTENT = """hull_number,description
0014,白色大型客轮，上层建筑为蓝色涂装，船尾有直升机停机坪
0025,黑色散货船，船体有红色水线，甲板上配有龙门吊
0123,白色邮轮，船身有红蓝条纹装饰，三座烟囱
0256,灰色军舰，隐身外形设计，舰首配有垂直发射系统
0389,红色渔船，船身有白色编号，甲板配有拖网绞车
0455,绿色集装箱船，船体涂有大型LOGO，配有四台岸桥吊
0512,黄色挖泥船，船体宽大，中部有大型绞吸臂
0678,蓝色油轮，双壳结构，船尾有大型舵机舱
0789,白色科考船，船尾有A型吊架，甲板有多个实验室舱
"""


class CsvShipSource(ShipDataSource):
    """CSV 文件数据源"""

    def __init__(self, csv_path: str | Path):
        self._csv_path = Path(csv_path).resolve()
        self._data: dict[str, str] = {}
        self._ensure_file()
        self._load()

    def _ensure_file(self) -> None:
        """确保 CSV 文件存在，不存在则创建默认文件"""
        if not self._csv_path.exists():
            self._csv_path.parent.mkdir(parents=True, exist_ok=True)
            self._csv_path.write_text(DEFAULT_CSV_CONTENT.strip(), encoding="utf-8")
            logger.info("已创建默认 CSV 数据库: %s", self._csv_path)

    def _load(self) -> None:
        """从 CSV 加载数据到内存"""
        self._data.clear()
        with open(self._csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                logger.error("CSV 文件为空或无法解析表头: %s", self._csv_path)
                return
            if "hull_number" not in reader.fieldnames:
                logger.error("CSV 文件缺少 hull_number 列: %s", reader.fieldnames)
                return
            for row in reader:
                hn = (row.get("hull_number") or "").strip()
                desc = (row.get("description") or "").strip()
                if hn:
                    self._data[hn] = desc
        logger.info("从 CSV 加载了 %d 条船记录: %s", len(self._data), self._csv_path)

    def _save(self) -> bool:
        """原子写入 CSV"""
        tmp_path = self._csv_path.with_suffix(".csv.tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["hull_number", "description"])
                for hn, desc in self._data.items():
                    writer.writerow([hn, desc])
            tmp_path.replace(self._csv_path)
            return True
        except Exception as e:
            if tmp_path.exists():
                tmp_path.unlink()
            logger.error("写入 CSV 失败: %s", e)
            return False

    def load_all(self) -> dict[str, str]:
        self._load()
        return dict(self._data)

    def lookup(self, hull_number: str) -> str | None:
        return self._data.get(hull_number.strip())

    def add(self, hull_number: str, description: str) -> bool:
        hn = hull_number.strip()
        if hn in self._data:
            return False
        self._data[hn] = description.strip()
        return self._save()

    def update(self, hull_number: str, description: str) -> bool:
        hn = hull_number.strip()
        if hn not in self._data:
            return False
        self._data[hn] = description.strip()
        return self._save()

    def delete(self, hull_number: str) -> bool:
        hn = hull_number.strip()
        if hn not in self._data:
            return False
        del self._data[hn]
        return self._save()

    def count(self) -> int:
        return len(self._data)

    def exists(self, hull_number: str) -> bool:
        return hull_number.strip() in self._data

    def bulk_add(self, records: dict[str, str]) -> int:
        added = 0
        for hn, desc in records.items():
            hn = hn.strip()
            if hn and hn not in self._data:
                self._data[hn] = desc.strip()
                added += 1
        if added > 0:
            self._save()
        return added

    def items(self):
        return self._data

    @property
    def csv_path(self) -> Path:
        return self._csv_path
