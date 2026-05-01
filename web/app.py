"""
船只数据管理 Web 服务 — FastAPI

提供 REST API + 前端页面，让用户通过浏览器管理船只舷号数据。
独立运行：python -m web.app
"""

from __future__ import annotations

import base64
import json
import logging
import re
import sys
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
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
_config: dict | None = None


def get_config() -> dict:
    global _config
    if _config is None:
        _config = load_config()
    return _config


def get_db() -> ShipDatabase:
    global _db
    if _db is None:
        _db = ShipDatabase(config=get_config())
    return _db


# ── VLM 识别 ──

RECOGNITION_PROMPT = """请仔细观察这张图片，识别图中的船只。

请以 JSON 格式返回，包含以下字段：
{
  "hull_number": "船身上的弦号文字（如果看不清则留空字符串）",
  "description": "对船只外观的详细中文描述，包括：船体颜色、船型（客轮/货轮/军舰/渔船等）、大小特征、上层建筑特征、特殊标志等"
}

只返回 JSON，不要其他内容。"""


def _get_vlm_client() -> ChatOpenAI:
    """获取 VLM 客户端（懒初始化）"""
    cfg = get_config()
    llm_cfg = cfg.get("llm", {})
    return ChatOpenAI(
        model=llm_cfg.get("model", "Qwen/Qwen3-VL-4B-AWQ"),
        api_key=llm_cfg.get("api_key", "abc123"),
        base_url=llm_cfg.get("base_url", "http://localhost:7890/v1"),
        temperature=0.0,
        max_tokens=1024,
    )


def _recognize_ship_from_bytes(image_bytes: bytes, filename: str) -> dict:
    """从图片字节调用 VLM 识别船只，返回 {hull_number, description}"""
    b64 = base64.b64encode(image_bytes).decode("utf-8")

    # 根据文件扩展名判断 MIME 类型
    ext = Path(filename).suffix.lower()
    mime_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".bmp": "image/bmp",
        ".webp": "image/webp", ".gif": "image/gif",
    }
    mime = mime_map.get(ext, "image/jpeg")

    llm = _get_vlm_client()
    msg = HumanMessage(content=[
        {"type": "text", "text": RECOGNITION_PROMPT},
        {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
    ])

    resp = llm.invoke([msg])
    content = resp.content.strip()

    # 兼容 ```json ... ``` 包裹
    if content.startswith("```"):
        content = content.split("\n", 1)[-1]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

    try:
        result = json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group())
            except json.JSONDecodeError:
                result = {"hull_number": "", "description": content}
        else:
            result = {"hull_number": "", "description": content}

    return {
        "hull_number": str(result.get("hull_number", "")).strip(),
        "description": str(result.get("description", "")).strip(),
    }


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


@app.post("/api/ships/recognize", response_model=ApiResponse)
async def recognize_ship_from_image(file: UploadFile = File(...)):
    """上传图片，调用 VLM 自动识别弦号和描述"""
    # 检查文件类型
    allowed_types = {"image/jpeg", "image/png", "image/bmp", "image/webp", "image/gif"}
    if file.content_type and file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail=f"不支持的文件类型: {file.content_type}，请上传图片文件")

    # 检查文件大小（限制 20MB）
    contents = await file.read()
    if len(contents) > 20 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="文件过大，请上传 20MB 以内的图片")

    try:
        result = _recognize_ship_from_bytes(contents, file.filename or "upload.jpg")
    except Exception as e:
        logger.error("VLM 识别失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"识别失败: {str(e)}")

    # 检查弦号是否已存在
    hull_number = result["hull_number"]
    existing_desc = None
    if hull_number:
        db = get_db()
        existing_desc = db.lookup(hull_number)

    return ApiResponse(
        success=True,
        message="识别成功",
        data={
            "hull_number": result["hull_number"],
            "description": result["description"],
            "already_exists": existing_desc is not None,
            "existing_description": existing_desc,
        },
    )


@app.post("/api/ships/recognize-and-add", response_model=ApiResponse)
async def recognize_and_add_ship(file: UploadFile = File(...)):
    """上传图片，识别后自动添加到数据库（如弦号已存在则覆盖）"""
    allowed_types = {"image/jpeg", "image/png", "image/bmp", "image/webp", "image/gif"}
    if file.content_type and file.content_type not in allowed_types:
        raise HTTPException(status_code=400, detail=f"不支持的文件类型: {file.content_type}")

    contents = await file.read()
    if len(contents) > 20 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="文件过大，请上传 20MB 以内的图片")

    try:
        result = _recognize_ship_from_bytes(contents, file.filename or "upload.jpg")
    except Exception as e:
        logger.error("VLM 识别失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"识别失败: {str(e)}")

    hull_number = result["hull_number"]
    description = result["description"]

    if not hull_number:
        raise HTTPException(status_code=400, detail="未能识别出弦号，请手动输入")

    db = get_db()
    existing_desc = db.lookup(hull_number)

    if existing_desc is not None:
        # 弦号已存在，更新描述
        db.update_ship(hull_number, description)
        return ApiResponse(
            success=True,
            message=f"弦号 {hull_number} 已存在，已更新描述",
            data={
                "hull_number": hull_number,
                "description": description,
                "action": "updated",
            },
        )
    else:
        # 新弦号，直接添加
        db.add_ship(hull_number, description)
        return ApiResponse(
            success=True,
            message=f"成功添加弦号: {hull_number}",
            data={
                "hull_number": hull_number,
                "description": description,
                "action": "added",
            },
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
