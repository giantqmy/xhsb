# Web + SQLite 功能测试指南

## 前置条件

- Python 3.10+
- 无需任何模型服务（VLM / Embedding 均不需要）

---

## 一、环境准备

### 1. 克隆项目

```bash
git clone https://github.com/giantqmy/xhsb.git
cd xhsb
```

### 2. 安装依赖

```bash
pip install fastapi uvicorn pyyaml
```

> 如果你在项目根目录，也可以用 `pip install -e .` 安装全部依赖。

---

## 二、修改配置

编辑 `config.yaml`，将数据库后端切换为 SQLite：

```yaml
# 找到这一段：
database:
  backend: "csv"           # ← 把 "csv" 改成 "sqlite"
  sqlite_path: "./data/ships.db"
```

改完后：

```yaml
database:
  backend: "sqlite"
  sqlite_path: "./data/ships.db"
```

其余配置不用动。

---

## 三、启动 Web 服务

```bash
python -m web
```

看到以下输出说明启动成功：

```
INFO:     Web 服务启动，初始化数据库…
INFO:     SQLite 数据库为空，导入默认船只数据 (9 条)
INFO:     SQLite 数据库就绪: ./data/ships.db
INFO:     启动 Web 服务: http://0.0.0.0:8000
INFO:     Uvicorn running on http://0.0.0.0:8000
```

---

## 四、浏览器测试

打开浏览器访问：**http://localhost:8000**

你应该能看到：
- 顶部统计栏：显示「9」条船只、「SQLITE」后端
- 搜索框 + 新增 / 批量导入 / 刷新按钮
- 9 条默认船只数据的表格

### 在页面上操作

| 操作 | 步骤 |
|------|------|
| 搜索 | 在搜索框输入「白色」，表格自动过滤 |
| 新增 | 点「+ 新增船只」，填入舷号和描述，点确认 |
| 编辑 | 点某行的「编辑」，修改描述后点确认 |
| 删除 | 点某行的「删除」，确认后删除 |
| 批量导入 | 点「批量导入」，输入 JSON 数据后点导入 |

---

## 五、API 接口测试（curl）

打开一个新的终端窗口，逐条执行以下命令：

### 1. 查看所有船只

```bash
curl http://localhost:8000/api/ships
```

预期返回：9 条船的 JSON 列表。

### 2. 查看统计信息

```bash
curl http://localhost:8000/api/stats
```

预期返回：

```json
{"total_ships": 9, "backend": "sqlite"}
```

### 3. 新增一条船

```bash
curl -X POST http://localhost:8000/api/ships \
  -H "Content-Type: application/json" \
  -d '{"hull_number": "TEST01", "description": "测试船只描述"}'
```

预期返回：

```json
{"success": true, "message": "成功添加弦号: TEST01", "data": null}
```

### 4. 查询刚添加的船

```bash
curl http://localhost:8000/api/ships/TEST01
```

预期返回：

```json
{"hull_number": "TEST01", "description": "测试船只描述"}
```

### 5. 修改描述

```bash
curl -X PUT http://localhost:8000/api/ships/TEST01 \
  -H "Content-Type: application/json" \
  -d '{"description": "修改后的描述"}'
```

预期返回：

```json
{"success": true, "message": "成功更新弦号: TEST01", "data": null}
```

### 6. 搜索

```bash
curl "http://localhost:8000/api/search?q=白色"
```

预期返回：描述中包含「白色」的所有船只。

### 7. 删除

```bash
curl -X DELETE http://localhost:8000/api/ships/TEST01
```

预期返回：

```json
{"success": true, "message": "成功删除弦号: TEST01", "data": null}
```

### 8. 批量导入

```bash
curl -X POST http://localhost:8000/api/ships/bulk \
  -H "Content-Type: application/json" \
  -d '{"ships": {"B001": "批量船1", "B002": "批量船2", "B003": "批量船3"}}'
```

预期返回：

```json
{"success": true, "message": "成功添加 3 条（跳过 0 条已存在的）", "data": {"added": 3, "skipped": 0}}
```

### 9. 重复添加（测试冲突）

```bash
curl -X POST http://localhost:8000/api/ships \
  -H "Content-Type: application/json" \
  -d '{"hull_number": "0014", "description": "重复的"}'
```

预期返回：HTTP 409，`"弦号已存在: 0014"`。

### 10. 查询不存在的船

```bash
curl http://localhost:8000/api/ships/NOTEXIST
```

预期返回：HTTP 404，`"未找到弦号: NOTEXIST"`。

---

## 六、验证 SQLite 数据库文件

测试完成后，确认数据库文件已生成：

```bash
ls -la data/ships.db
```

应该能看到 `ships.db` 文件，大小不为 0。

可以用 SQLite 命令行工具查看内容：

```bash
sqlite3 data/ships.db "SELECT * FROM ships LIMIT 5;"
```

---

## 七、测试检查清单

| # | 测试项 | 通过？ |
|---|--------|--------|
| 1 | `python -m web` 正常启动，无报错 | ☐ |
| 2 | 浏览器打开 `http://localhost:8000` 能看到页面 | ☐ |
| 3 | 统计栏显示 9 条船、SQLITE 后端 | ☐ |
| 4 | 搜索框输入关键词能过滤表格 | ☐ |
| 5 | 点「+ 新增船只」能弹窗并添加成功 | ☐ |
| 6 | 点「编辑」能修改描述 | ☐ |
| 7 | 点「删除」能删除记录 | ☐ |
| 8 | 批量导入 JSON 数据成功 | ☐ |
| 9 | curl 测试全部 10 个接口通过 | ☐ |
| 10 | `data/ships.db` 文件存在且有数据 | ☐ |

---

## 八、常见问题

### Q: 启动报错 `ModuleNotFoundError: No module named 'fastapi'`

```bash
pip install fastapi uvicorn
```

### Q: 启动报错 `ModuleNotFoundError: No module named 'yaml'`

```bash
pip install pyyaml
```

### Q: 页面打开是空白或报错

检查终端日志，确认启动时显示 `Uvicorn running on http://0.0.0.0:8000`。

### Q: 想切回 CSV 后端

改 `config.yaml`：

```yaml
database:
  backend: "csv"
```

重启服务即可，原有 CSV 数据不受影响。

---

## 九、停止服务

在运行 `python -m web` 的终端按 `Ctrl+C` 即可停止。
