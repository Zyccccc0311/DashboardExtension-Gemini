# Tableau MCP Chat

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
