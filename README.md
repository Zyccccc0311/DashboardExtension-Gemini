# Tableau MCP Chat

## Have Chinese version after English ver, thanks!

A Tableau Dashboard Extension that lets you query Tableau datasources using natural language.

It passes your question, the VizQL Data Service OpenAPI spec, MCP tool descriptions, and the current dashboard's datasource metadata all together to Gemini — letting it autonomously plan and execute queries, just like Claude Code does.

---

## How It Works

```
Tableau Dashboard
      ↓  (datasource metadata + user question)
  server.js
      ↓
  Gemini API  ←→  Tableau MCP Server (@tableau/mcp-server)
      ↓                ↑
  Agentic Loop  →  tool calls (list-datasources / get-datasource-metadata / query-datasource / ...)
      ↓
  Answer + execution trace
```

**Agentic Loop**: Gemini calls MCP tools across multiple turns, deciding the next step on its own — no human intervention needed between steps. It retries automatically on errors, up to 12 turns per request.

---

## Features

- **Natural language queries** — Ask questions in plain English or Chinese; Gemini translates them into VizQL queries automatically
- **Automatic datasource matching** — Reads the datasource names from the current Dashboard and fuzzy-matches them against MCP-accessible datasources, pre-loading metadata before the conversation starts
- **Top-N per group queries** — Handles complex requests like "top 10 products by profit for each year and segment" automatically, with no manual query splitting required
- **Execution trace panel** — Every conversation shows which tools Gemini called, with what arguments, and whether each call succeeded
- **Result table** — Parses JSON arrays from Gemini's response and renders them as a scrollable table
- **Conversation history** — Maintains context across turns so you can ask follow-up questions
- **Fault recovery** — Automatically handles `MALFORMED_FUNCTION_CALL` (retries with the original question preserved), empty response fallback, and MCP connection retries

---

## Getting Started

### 1. Clone and install

```bash
git clone <this-repo>
cd Gemini1
npm install
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```env
# Gemini
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-2.5-flash          # Optional, defaults to gemini-2.5-flash

# Tableau Server / Cloud
SERVER=https://your-tableau-server.com
SITE_NAME=your_site_name               # Leave empty for the Default site
PAT_NAME=your_pat_name
PAT_VALUE=your_pat_value

# Optional
PORT=8080
VDS_OPENAPI_PATH=/path/to/openapi.json  # VizQL Data Service OpenAPI spec (improves query accuracy)
```

### 3. Start the server

```bash
npm start
```

You should see:

```
Server ready: http://localhost:8080
Successfully connected to Tableau MCP Server!
```

### 4. Load the extension in Tableau

1. Open Tableau Desktop (must be connected to a Tableau Server or Tableau Cloud)
2. Open a Dashboard
3. Drag an **Extension** object from the left panel onto the dashboard
4. Click **Access Local Extension**
5. Select `manifest.trex` from this project's root directory
6. Confirm the extension URL is `http://localhost:8080/index.html`

---

## Using the Extension

Once loaded, the extension UI has two panels:

| Panel | Description |
|-------|-------------|
| Left sidebar | Shows the current dashboard's datasource match results, MCP connection status, and available tool count |
| Right main area | Conversation, result table, and execution trace |

**Just type your question in the input box**, for example:

- `How did profit in the East region change from 2024 to 2025?`
- `Show me the top 10 products by profit for each year and segment`
- `Which sub-category has the lowest profit margin, and why?`

Gemini will automatically:
1. Identify the datasource used by the current dashboard
2. Load field metadata
3. Plan and execute the necessary queries (potentially across multiple steps)
4. Return analysis results with reasoning

---

## Project Structure

```
Gemini1/
├── server.js          # Express server + Gemini Agentic Loop + MCP bridge
├── public/
│   └── index.html     # Tableau Extension frontend
├── manifest.trex      # Tableau extension manifest
├── package.json
└── .env               # Local config (not committed)
```

---

## What's New in This Version

This version fully rebuilds the AI query capability from scratch:

- **Gemini API integration** (gemini-2.5-flash) with a multi-turn Agentic Loop for autonomous tool use
- **Tableau MCP Server** connected via `@tableau/mcp-server` for live datasource access
- **Datasource auto-matching**: extracts datasource names from Dashboard metadata and fuzzy-matches them against MCP-available datasources
- **Top-N per group strategy**: built into the System Prompt — Gemini autonomously decomposes and parallelizes multi-group queries
- **Parallel call guard**: limits to 4 parallel tool calls per turn to prevent `MALFORMED_FUNCTION_CALL` errors
- **Multi-block data parsing**: correctly merges multiple `[DATA]...[/DATA]` blocks from a single Gemini response into one unified table
- **Execution trace UI**: the frontend displays each tool call's name, arguments, status, and row count
- **Error recovery**: auto-retry on `MALFORMED_FUNCTION_CALL` (with original question preserved), fallback summary prompt on empty response, automatic MCP reconnection on startup failure

### Latest Updates

- **`execute-code` virtual tool** — Gemini can now write JavaScript to process query results server-side (Node.js `vm` sandbox). Handles large datasets and cross-query operations (e.g. set intersections, consecutive-year analysis) that exceed LLM context limits
- **`queryStore` persistence** — Results from both `query-datasource` and `execute-code` are stored server-side by key (`dataset_1`, `dataset_2`...) and passed to the sandbox, enabling multi-step chained computations
- **Tool response truncation** — Only 3 sample rows + metadata are sent back to Gemini per query result (full data stays in `queryStore`). Prevents 429/503 token quota errors on large datasets
- **Smart final data selection** — Final output prefers `execute-code` processed results over raw query data, ensuring the correct filtered/aggregated rows are shown in the table
- **`[DATA]` block hidden from UI** — The raw JSON transmission block is stripped from the displayed chat response; only the narrative text and rendered table are shown

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `@google/generative-ai` | Gemini API SDK |
| `@modelcontextprotocol/sdk` | MCP client for connecting to Tableau MCP Server |
| `@tableau/mcp-server` | Tableau MCP Server (fetched automatically via npx at runtime) |
| `express` | Local HTTP server |
| `dotenv` | Environment variable management |

---

## Notes

- This project requires a locally running Node.js process — it cannot be deployed to static hosting
- Tableau MCP Server is downloaded automatically via `npx` on first startup; internet access is required
- The `.env` file contains sensitive credentials — it is excluded via `.gitignore` and **must not be committed**
- Query accuracy depends on the quality of your datasource metadata and how clearly fields are named

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

### 最新更新

- **`execute-code` 虚拟工具**：Gemini 可以编写 JavaScript 在服务端（Node.js `vm` 沙箱）直接处理查询结果，解决大数据量下 LLM 上下文不足的问题，支持集合求交、连续年份分析等跨查询复杂运算
- **`queryStore` 数据持久化**：`query-datasource` 和 `execute-code` 的结果均以 `dataset_1`、`dataset_2`... 为键存储在服务端，沙箱执行时可引用任意历史数据集，支持多步链式计算
- **工具响应截断**：每次查询结果只向 Gemini 回传 3 行样本 + 元数据（完整数据留在 `queryStore`），有效避免大数据集触发 429/503 限流
- **智能最终数据选择**：输出优先使用 `execute-code` 的处理结果而非原始查询数据，确保表格展示的是正确的筛选/聚合结果
- **`[DATA]` 块从 UI 中隐藏**：原始 JSON 传输块在渲染前从聊天文本中剔除，用户界面只展示自然语言说明和渲染后的数据表格

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
