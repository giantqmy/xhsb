"""数据源抽象基类 — 定义所有后端必须实现的接口"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Mapping


class ShipDataSource(ABC):
    """船只数据源抽象接口"""

    @abstractmethod
    def load_all(self) -> dict[str, str]:
        """加载全部数据，返回 {hull_number: description}"""

    @abstractmethod
    def lookup(self, hull_number: str) -> str | None:
        """精确查找，返回 description 或 None"""

    @abstractmethod
    def add(self, hull_number: str, description: str) -> bool:
        """新增一条记录。已存在返回 False，成功返回 True"""

    @abstractmethod
    def update(self, hull_number: str, description: str) -> bool:
        """更新一条记录的描述。不存在返回 False"""

    @abstractmethod
    def delete(self, hull_number: str) -> bool:
        """删除一条记录。不存在返回 False"""

    @abstractmethod
    def count(self) -> int:
        """返回记录总数"""

    @abstractmethod
    def exists(self, hull_number: str) -> bool:
        """检查弦号是否存在"""

    @abstractmethod
    def bulk_add(self, records: dict[str, str]) -> int:
        """批量添加，返回成功添加的数量（跳过已存在的）"""

    def upsert(self, hull_number: str, description: str) -> str:
        """插入或更新：不存在则新增，已存在则覆盖。返回 'inserted' 或 'updated'。"""
        hn = hull_number.strip()
        if hn in self.load_all():
            self.update(hn, description)
            return "updated"
        else:
            self.add(hn, description)
            return "inserted"

    def items(self) -> Mapping[str, str]:
        """返回只读映射（默认实现，子类可覆盖）"""
        return self.load_all()
