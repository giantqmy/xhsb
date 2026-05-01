"""
船只数据管理 Web 服务 — FastAPI

提供 REST API + 前端页面，让用户通过浏览器管理船只舷号数据。
独立运行：python -m web.app
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# 确保项目根目录在 sys.path 中
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from config import load_config
from database import ShipDatabase

logger = logging.getLogger(__name__)

# ── 全局数据库实例 ──
_db: ShipDatabase | None = None


def get_db() -> ShipDatabase:
    global _db
    if _db is None:
        config = load_config()
        _db = ShipDatabase(config=config)
    return _db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期：启动时初始化数据库"""
    logger.info("Web 服务启动，初始化数据库…")
    get_db()
    yield
    logger.info("Web 服务关闭")


app = FastAPI(
    title="船只舷号管理系统",
    description="通过 Web 界面管理船只舷号数据，支持 CSV 和 SQLite 后端",
    version="1.0.0",
    lifespan=lifespan,
)

# ── 静态文件 ──
_static_dir = Path(__file__).parent / "static"
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


# ── Pydantic 模型 ──

class ShipCreate(BaseModel):
    hull_number: str = Field(..., min_length=1, max_length=50, description="舷号")
    description: str = Field(..., min_length=1, max_length=2000, description="船只描述")


class ShipUpdate(BaseModel):
    description: str = Field(..., min_length=1, max_length=2000, description="船只描述")


class ShipBulkCreate(BaseModel):
    ships: dict[str, str] = Field(..., description="批量数据 {hull_number: description}")


class ApiResponse(BaseModel):
    success: bool
    message: str
    data: Any = None


# ── API 路由 ──

@app.get("/", response_class=HTMLResponse)
async def index():
    """返回前端页面"""
    html_path = Path(__file__).parent / "static" / "index.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>船只舷号管理系统</h1><p>前端页面未找到，请创建 web/static/index.html</p>")


@app.get("/api/ships")
async def list_ships():
    """获取所有船只列表"""
    db = get_db()
    data = db.source.load_all()
    ships = [
        {"hull_number": hn, "description": desc}
        for hn, desc in sorted(data.items())
    ]
    return {"total": len(ships), "ships": ships}


@app.get("/api/ships/{hull_number}")
async def get_ship(hull_number: str):
    """查询单条船只"""
    db = get_db()
    desc = db.lookup(hull_number)
    if desc is None:
        raise HTTPException(status_code=404, detail=f"未找到弦号: {hull_number}")
    return {"hull_number": hull_number, "description": desc}


@app.post("/api/ships", response_model=ApiResponse)
async def create_ship(ship: ShipCreate):
    """新增船只"""
    db = get_db()
    success = db.add_ship(ship.hull_number, ship.description)
    if not success:
        raise HTTPException(status_code=409, detail=f"弦号已存在: {ship.hull_number}")
    return ApiResponse(success=True, message=f"成功添加弦号: {ship.hull_number}")


@app.put("/api/ships/{hull_number}", response_model=ApiResponse)
async def update_ship(hull_number: str, ship: ShipUpdate):
    """更新船只描述"""
    db = get_db()
    success = db.update_ship(hull_number, ship.description)
    if not success:
        raise HTTPException(status_code=404, detail=f"未找到弦号: {hull_number}")
    return ApiResponse(success=True, message=f"成功更新弦号: {hull_number}")


@app.delete("/api/ships/{hull_number}", response_model=ApiResponse)
async def delete_ship(hull_number: str):
    """删除船只"""
    db = get_db()
    success = db.delete_ship(hull_number)
    if not success:
        raise HTTPException(status_code=404, detail=f"未找到弦号: {hull_number}")
    return ApiResponse(success=True, message=f"成功删除弦号: {hull_number}")


@app.post("/api/ships/bulk", response_model=ApiResponse)
async def bulk_create_ships(bulk: ShipBulkCreate):
    """批量添加船只"""
    db = get_db()
    added = db.source.bulk_add(bulk.ships)
    if added > 0:
        db.reload()
    return ApiResponse(
        success=True,
        message=f"成功添加 {added} 条（跳过 {len(bulk.ships) - added} 条已存在的）",
        data={"added": added, "skipped": len(bulk.ships) - added},
    )


@app.get("/api/search")
async def search_ships(q: str = ""):
    """按描述关键词搜索"""
    if not q.strip():
        raise HTTPException(status_code=400, detail="搜索关键词不能为空")
    db = get_db()
    source = db.source
    # SQLite 支持模糊搜索
    if hasattr(source, "search_by_description"):
        results = source.search_by_description(q)
    else:
        # CSV 后端：内存中过滤
        data = source.load_all()
        results = [
            {"hull_number": hn, "description": desc}
            for hn, desc in data.items()
            if q.lower() in desc.lower()
        ]
    return {"total": len(results), "results": results}


@app.get("/api/stats")
async def stats():
    """数据库统计信息"""
    db = get_db()
    source = db.source
    backend_type = "sqlite" if hasattr(source, "db_path") else "csv"
    return {
        "total_ships": db.source.count(),
        "backend": backend_type,
    }


# ── 启动入口 ──

def main():
    import uvicorn
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    config = load_config()
    web_cfg = config.get("web", {})
    host = web_cfg.get("host", "0.0.0.0")
    port = web_cfg.get("port", 8000)
    logger.info("启动 Web 服务: http://%s:%d", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
