# Tableau MCP Chat

一个 Tableau Dashboard Extension，让你用自然语言直接查询 Tableau 数据源。

把问题、OpenAPI 说明、MCP tool 描述和当前 dashboard datasource metadata 一起交给 Gemini，让它像 Claude 那样自行规划并查询。

---

## 工作原理

```
Tableau Dashboard
      ↓  (datasource metadata + 用户问题)
  server.js
      ↓
  Gemini API  ←→  Tableau MCP Server (@tableau/mcp-server)
      ↓                ↑
   Agentic Loop  →  tool calls (list-datasources / get-datasource-metadata / query-datasource / ...)
      ↓
  返回结果 + 执行轨迹
```

**Agentic Loop**：Gemini 可以连续多轮调用 MCP 工具，自主决定下一步——无需用户介入。遇到错误时自动换方案重试，最多 12 轮。

---

## 功能

- **自然语言查询**：直接用中文或英文问数据问题，Gemini 自动转成 VizQL 查询
- **数据源自动匹配**：根据 Dashboard 中的数据源名称，自动匹配 MCP 可访问的数据源并预加载元数据
- **Top-N 分组查询**：自动处理"每年每个 Segment 利润前十的产品"此类需要多次查询的复杂需求，无需手动拆分
- **执行轨迹面板**：每次对话都展示 Gemini 调用了哪些工具、参数是什么、结果是否成功
- **结果表格**：自动解析 Gemini 返回的 JSON 数据，渲染为可滚动表格
- **对话历史**：保留上下文，支持追问
- **容错恢复**：自动处理 `MALFORMED_FUNCTION_CALL`（带原始问题重试）、空响应兜底等异常情况

---

## 快速开始

### 1. 克隆并安装依赖

```bash
git clone <this-repo>
cd Gemini1
npm install
```

### 2. 配置环境变量

在根目录创建 `.env` 文件：

```env
# Gemini
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-2.5-flash          # 可选，默认 gemini-2.5-flash

# Tableau Server / Cloud
SERVER=https://your-tableau-server.com
SITE_NAME=your_site_name               # Default site 留空即可
PAT_NAME=your_pat_name
PAT_VALUE=your_pat_value

# 可选
PORT=8080
VDS_OPENAPI_PATH=C:\path\to\openapi.json   # VizQL Data Service OpenAPI 说明文件（可选，增强查询准确性）
```

### 3. 启动服务

```bash
npm start
```

启动成功后会看到：

```
Server ready: http://localhost:8080
成功连接到 Tableau MCP Server!
```

### 4. 在 Tableau 中加载扩展

1. 打开 Tableau Desktop（需要连接到 Tableau Server/Cloud）
2. 打开一个 Dashboard
3. 从左侧面板拖入 **Extension** 对象
4. 选择 **Access Local Extension**
5. 选择本项目根目录下的 `manifest.trex`
6. 确认扩展地址为 `http://localhost:8080/index.html`

---

## 使用方式

加载成功后，扩展界面分为左右两栏：

| 区域 | 说明 |
|------|------|
| 左侧边栏 | 显示当前 Dashboard 的数据源匹配情况、MCP 连接状态和工具数量 |
| 右侧主区域 | 对话区 + 结果表格 + 执行轨迹 |

**直接在输入框提问即可**，例如：

- `2025年东区的利润和2024年相比变化了多少？`
- `查看每年每个 Segment 利润前十的产品`
- `哪个子类别的利润率最低？为什么？`

Gemini 会自动：
1. 识别当前 Dashboard 使用的数据源
2. 加载字段元数据
3. 规划并执行所需的查询（可能多步）
4. 返回分析结果和推理

---

## 项目结构

```
Gemini1/
├── server.js          # Express 服务端 + Gemini Agentic Loop + MCP 桥接
├── public/
│   └── index.html     # Tableau Extension 前端页面
├── manifest.trex      # Tableau 扩展清单
├── package.json
└── .env               # 本地配置（不提交）
```

---

## 本次主要更新

相比之前的最小版本，本次完整重建了 AI 查询能力：

- **重新接入 Gemini API**（gemini-2.5-flash），通过 Agentic Loop 实现多步工具调用
- **接入 Tableau MCP Server**，通过 `@tableau/mcp-server` 连接 Tableau 数据源
- **实现数据源自动匹配**：从 Dashboard 元数据提取数据源名称，自动与 MCP 可用数据源做模糊匹配
- **Top-N 分组查询策略**：System Prompt 中内置策略指导，Gemini 可自主拆解并并行执行多组查询
- **并行调用保护**：限制每轮最多 4 个并行工具调用，避免 `MALFORMED_FUNCTION_CALL`
- **多 DATA 块解析**：支持 Gemini 在一次回答中返回多组 `[DATA]...[/DATA]`，合并后渲染到表格
- **执行轨迹可视化**：前端展示每次工具调用的名称、参数、状态和数据行数
- **异常恢复机制**：`MALFORMED_FUNCTION_CALL` 自动重试（携带原始问题）、空响应兜底 prompt、MCP 连接失败自动重连

---

## 依赖

| 依赖 | 用途 |
|------|------|
| `@google/generative-ai` | Gemini API SDK |
| `@modelcontextprotocol/sdk` | MCP 客户端，连接 Tableau MCP Server |
| `@tableau/mcp-server` | Tableau MCP Server（运行时通过 npx 自动拉取） |
| `express` | 本地 HTTP 服务 |
| `dotenv` | 环境变量管理 |

---

## 注意事项

- 本项目需要本地运行 Node.js 服务，不支持直接部署到静态托管
- Tableau MCP Server 首次启动时会通过 `npx` 自动下载，需要网络访问
- `.env` 文件包含敏感密钥，已在 `.gitignore` 中排除，**请勿提交**
- Gemini 的查询准确性依赖元数据质量和字段命名的清晰程度
