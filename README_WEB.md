# 船只舷号管理系统 — 新增功能说明

## 改动概览

在保留原有 CSV 数据源的基础上，新增了：

1. **可插拔数据源架构** — CSV / SQLite 双后端，通过配置切换
2. **SQLite 数据库后端** — 支持完整的 CRUD 操作
3. **FastAPI Web 服务** — REST API + 前端页面，用户可自行管理船只数据

## 项目结构（新增/修改部分）

```
ship-hull-agent/
├── database/
│   ├── __init__.py      ← 修改：支持可插拔后端（原 ShipDatabase 接口不变）
│   ├── base.py          ← 新增：数据源抽象基类
│   ├── csv_source.py    ← 新增：CSV 后端（从原 __init__.py 抽取）
│   └── sql_source.py    ← 新增：SQLite 后端
├── web/
│   ├── __init__.py      ← 新增
│   ├── __main__.py      ← 新增：python -m web 启动
│   ├── app.py           ← 新增：FastAPI 应用（REST API + 页面路由）
│   └── static/
│       └── index.html   ← 新增：前端管理页面
├── data/
│   ├── ships.csv        ← 原始 CSV 数据
│   └── ships.db         ← SQLite 数据库（切换后端后自动创建）
├── config.yaml          ← 修改：新增 database 和 web 配置段
├── config.py            ← 修改：新增 database/web 默认值
├── migrate_csv_to_sqlite.py  ← 新增：CSV → SQLite 迁移工具
└── pyproject.toml       ← 修改：新增 fastapi/uvicorn 依赖
```

## 使用方式

### 方式一：保持 CSV 后端（完全向后兼容）

无需任何改动，`config.yaml` 中 `database.backend` 默认为 `"csv"`。
原有 Agent、Pipeline、CLI 功能全部不受影响。

### 方式二：切换到 SQLite 后端

1. **迁移现有数据**（可选）：
   ```bash
   python migrate_csv_to_sqlite.py --csv ./data/ships.csv --db ./data/ships.db
   ```

2. **修改配置**：
   ```yaml
   # config.yaml
   database:
     backend: "sqlite"
     sqlite_path: "./data/ships.db"
   ```

3. 之后所有读写自动走 SQLite，Agent/Pipeline/CLI 无需改动。

### 启动 Web 管理服务

```bash
# 方式一：直接运行
python -m web.app

# 方式二：模块方式
python -m web

# 自定义端口（修改 config.yaml 中 web.port）
```

浏览器访问 `http://localhost:8000` 即可管理船只数据。

## Web API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/ships` | 获取所有船只列表 |
| GET | `/api/ships/{hull_number}` | 查询单条船只 |
| POST | `/api/ships` | 新增船只 |
| PUT | `/api/ships/{hull_number}` | 更新船只描述 |
| DELETE | `/api/ships/{hull_number}` | 删除船只 |
| POST | `/api/ships/bulk` | 批量导入 |
| GET | `/api/search?q=关键词` | 按描述搜索 |
| GET | `/api/stats` | 数据库统计 |

## 对原有代码的影响

- **ShipDatabase 接口完全不变**：`lookup()`, `semantic_search()`, `semantic_search_filtered()`, `hull_numbers`, `descriptions`, `items`, `__len__` 全部保留
- **tools/__init__.py 无需修改**：`build_tools(db)` 接收的还是 `ShipDatabase` 实例
- **agent/__init__.py 无需修改**
- **pipeline/ 无需修改**
- **build_db.py 无需修改**（仍写 CSV，如果用 SQLite 后端，建议后续扩展）
