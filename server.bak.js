import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import path from 'path';
import { fileURLToPath } from 'url';
import { platform } from 'os';

// 1. 加载配置
dotenv.config();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const isWindows = platform() === 'win32';

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// 2. 初始化 Gemini 
// 💡 修改点1：切换为稳定版模型，避免 503 拥堵报错
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({
    model: "gemini-3.1-flash-lite-preview", 
    systemInstruction: `你是一个严谨的 Tableau 数据分析引擎。严格遵守以下规则：

【工作流程 - 必须按顺序执行】
1. 先调用 list-datasources 获取数据源列表及其真正的 datasourceLuid（UUID 格式，如 "2bac64a3-e216-4d8f-891c-905f9ce33ac3"）
2. 用得到的 datasourceLuid 调用 get-datasource-metadata 获取字段名称和类型
3. 根据元数据返回的精确字段名构造 query-datasource 查询

⚠️ 重要：用户提供的数据源信息中的 luid 字段是 Tableau Extensions API 的内部 ID（如 "sqlproxy.xxx"），不是 MCP 需要的 datasourceLuid。你必须通过 list-datasources 工具获取正确的 UUID 格式 LUID。可以用用户提供的数据源名称（name）来匹配找到对应的 datasourceLuid。

【仪表盘联动控制 - 新增核心能力】
除了查询数据，你还可以控制前端 Tableau 仪表盘的筛选器。
如果用户的意图包含“过滤”、“筛选”、“查看某某的数据（如：切换到2025年）”等改变视图的动作，你必须在回答的最末尾，输出一段用 <FILTER_COMMAND> 标签包裹的 JSON 指令。

示例语法：
用户问：“帮我过滤出 2025 年的数据，并告诉我总利润。”
你的回答：“好的，已经为您过滤到 2025 年，总利润为 xxx。[DATA]...[/DATA]
<FILTER_COMMAND>
{
  "action": "filter",
  "fieldName": "Order Date",
  "values": ["2025"]
}
</FILTER_COMMAND>”

【Filter 语法规范 - 来自实际 MCP Schema】
- 年份/日期过滤：使用 QUANTITATIVE_DATE 类型 + RANGE
  示例: {"field":{"fieldCaption":"Order Date"},"filterType":"QUANTITATIVE_DATE","quantitativeFilterType":"RANGE","minDate":"2026-01-01","maxDate":"2026-12-31"}
- 日期也可以用 DATE 类型做相对日期过滤：
  示例: {"field":{"fieldCaption":"Order Date"},"filterType":"DATE","periodType":"YEARS","dateRangeType":"CURRENT"}
- 维度过滤（如 Segment、Category）：使用 SET 类型
  示例: {"field":{"fieldCaption":"Segment"},"filterType":"SET","values":["Consumer","Corporate"],"exclude":false}
- 数值范围过滤：使用 QUANTITATIVE_NUMERICAL 类型
  示例: {"field":{"fieldCaption":"Sales","function":"SUM"},"filterType":"QUANTITATIVE_NUMERICAL","quantitativeFilterType":"RANGE","min":0,"max":10000}
- 字符串模糊匹配：使用 startsWith / endsWith / contains 字段
  示例: {"field":{"fieldCaption":"Product Name"},"filterType":"SET","contains":"Chair"}
- Top N 过滤：使用 TOP 类型
  示例: {"field":{"fieldCaption":"State"},"filterType":"TOP","howMany":5,"fieldToMeasure":{"fieldCaption":"Sales","function":"SUM"}}

合法的 filterType 值：SET、TOP、QUANTITATIVE_NUMERICAL、QUANTITATIVE_DATE、DATE
不存在 MATCH 类型。

【重要限制】
- TOP filter 是全局的，不支持"按分组各取 Top N"。
  如果用户要求"每个 X 的 Top N"（如"每个 Segment 利润排名前5的产品"），正确策略是：
  (1) 先查出有哪些分组值（如查 Segment 字段拿到 Consumer、Corporate、Home Office）
  (2) 对每个分组分别发起 query-datasource 调用，同时使用 SET filter 指定分组 + TOP filter 取 Top N
- 不支持 ad-hoc calculation 字段。每个 field 必须有 fieldCaption。如需计算衍生指标（如利润率），请同时查询所需度量字段（如 SUM(Profit) 和 SUM(Sales)），然后在回答中自行计算。
- 禁止发送空参数 {}。每次调用 query-datasource 必须包含 datasourceLuid 和 query 两个必填参数。

【度量字段】
- 数值字段必须指定 function，如 "function": "SUM"、"AVG"、"COUNT"、"MIN"、"MAX"

【回答要求】
- 严禁凭空伪造数据，必须基于工具返回的真实数据
- 如果查询失败，检查错误信息并修正参数后重试
- 对于衍生指标（如利润率 = Profit/Sales），必须展示计算过程`
});

let mcpClient;

// 3. 连接 Tableau MCP
async function startMcpServer() {
    const command = isWindows ? 'npx.cmd' : 'npx';
    const args = isWindows 
        ? ["-y", "@tableau/mcp-server@latest"] 
        : ["-y", "@tableau/mcp-server@latest"];

    let transport = new StdioClientTransport({
        command: command,
        args: args,
        env: {
            ...process.env,
            SERVER: process.env.SERVER,
            SITE_NAME: process.env.SITE_NAME,
            PAT_NAME: process.env.PAT_NAME,
            PAT_VALUE: process.env.PAT_VALUE
        }
    });

    const MAX_RETRIES = 3;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            mcpClient = new Client({ name: "tableau-mcp-bridge", version: "1.0.0" }, { capabilities: {} });
            await mcpClient.connect(transport);
            console.log("✅ 成功连接到 Tableau MCP Server!");
            break;
        } catch (err) {
            console.error(`❌ MCP 连接失败 (第 ${attempt}/${MAX_RETRIES} 次):`, err.message);
            mcpClient = null;
            if (attempt < MAX_RETRIES) {
                console.log(`⏳ ${5 * attempt} 秒后重试...`);
                await new Promise(r => setTimeout(r, 5000 * attempt));
                transport = new StdioClientTransport({
                    command, args,
                    env: { ...process.env, SERVER: process.env.SERVER, SITE_NAME: process.env.SITE_NAME, PAT_NAME: process.env.PAT_NAME, PAT_VALUE: process.env.PAT_VALUE }
                });
            }
        }
    }
}

// 4. 转换工具格式
function convertToGeminiTools(mcpTools) {
    const cleanSchema = (schema) => {
        if (!schema || typeof schema !== 'object') return schema;
        if (Array.isArray(schema.type)) schema.type = schema.type.find(t => t !== 'null') || schema.type[0];
        const newSchema = Array.isArray(schema) ? [] : {};
        const whitelist = ['type', 'properties', 'required', 'items', 'description', 'enum', 'anyOf', 'oneOf', 'const', 'default', 'format', 'minimum', 'maximum'];
        for (const key in schema) {
            if (!whitelist.includes(key)) continue;
            newSchema[key] = (typeof schema[key] === 'object') ? cleanSchema(schema[key]) : schema[key];
        }
        return newSchema;
    };

    return [{
        functionDeclarations: mcpTools.map(tool => ({
            name: tool.name,
            description: tool.description,
            parameters: cleanSchema(tool.inputSchema)
        }))
    }];
}

// 5. 核心路由处理
app.post('/api/analyze-datasource', async (req, res) => {
    try {
        const { prompt } = req.body;

        if (!mcpClient) {
            return res.status(503).json({ error: "MCP 服务尚未就绪，请稍后重试。" });
        }

        const { tools: mcpTools } = await mcpClient.listTools();
        const geminiTools = convertToGeminiTools(mcpTools);

        const chat = model.startChat({ tools: geminiTools });
        let result = await chat.sendMessage(prompt);
        let response = result.response;

        let auditHtml = `
        <div style="background: #fdf6e3; padding: 14px; border-left: 4px solid #d97706; margin-bottom: 15px; border-radius: 4px; font-size: 13px;">
            <div style="font-weight: bold; color: #92400e; margin-bottom: 10px;">📋 MCP 调用记录</div>`;
        let toolCalled = false;
        let totalSkipped = 0;

        let iteration = 0;
        while (response.functionCalls() && iteration < 10) {
            iteration++;
            const toolResults = [];

            const allCalls = response.functionCalls();
            const validCalls = allCalls.filter(c => c.args && Object.keys(c.args).length > 0);
            const emptyCalls = allCalls.filter(c => !c.args || Object.keys(c.args).length === 0);

            for (const call of emptyCalls) {
                console.log(`⏭️ 跳过空参数调用: ${call.name}`);
                auditHtml += `<div style="margin-bottom: 10px; border-bottom: 1px dashed #e5e7eb; padding-bottom: 10px;">`;
                auditHtml += `<div style="color: #9ca3af; font-weight: 600;">⏭️ ${call.name} <span style="font-weight:400;">(空参数，已跳过)</span></div>`;
                auditHtml += `</div>`;
                toolResults.push({
                    functionResponse: {
                        name: call.name,
                        response: { error: `调用失败：参数不能为空。此工具必须提供完整参数，请参考之前成功调用时使用的参数格式重新构造。` }
                    }
                });
            }

            totalSkipped += emptyCalls.length;
            for (const call of validCalls) {
                toolCalled = true;
                const args = call.args || {};

                console.log(`🤖 AI 正在调用工具: ${call.name}`, JSON.stringify(args, null, 2));

                const toolName = call.name;
                const luid = args.datasourceLuid || args.data_source_id || args.luid || null;
                const query = args.query || {};
                const filters = query.filters || args.filters || null;
                const fields = query.fields || args.fields || null;

                auditHtml += `<div style="margin-bottom: 10px; border-bottom: 1px dashed #e5e7eb; padding-bottom: 10px;">`;
                auditHtml += `<div style="color: #2563eb; font-weight: 600; margin-bottom: 4px;">🛠 ${toolName}</div>`;

                if (luid) {
                    auditHtml += `<div style="margin-left: 8px;"><span style="color: #64748b;">📍 LUID:</span> <code style="background:#eef2ff;padding:2px 5px;border-radius:3px;font-size:12px;">${luid}</code></div>`;
                }

                if (fields && Array.isArray(fields)) {
                    const fieldSummary = fields.map(f => {
                        let label = f.fieldCaption || f;
                        if (f.function) label = `${f.function}(${label})`;
                        if (f.fieldAlias) label += ` as "${f.fieldAlias}"`;
                        return label;
                    }).join(', ');
                    auditHtml += `<div style="margin-left: 8px;"><span style="color: #64748b;">📑 字段:</span> ${fieldSummary}</div>`;
                }

                if (filters && Array.isArray(filters) && filters.length > 0) {
                    const filterSummary = filters.map(f => {
                        const fname = f.field?.fieldCaption || '?';
                        const ftype = f.filterType || '?';
                        let detail = `${fname} [${ftype}]`;
                        if (f.values) detail += ` → ${JSON.stringify(f.values)}`;
                        if (f.minDate || f.maxDate) detail += ` → ${f.minDate || ''} ~ ${f.maxDate || ''}`;
                        if (f.min !== undefined || f.max !== undefined) detail += ` → ${f.min ?? ''} ~ ${f.max ?? ''}`;
                        if (f.periodType) detail += ` → ${f.dateRangeType} ${f.periodType}`;
                        return detail;
                    }).join('<br/>');
                    auditHtml += `<div style="margin-left: 8px;"><span style="color: #64748b;">🎯 过滤条件:</span><br/><code style="background:#fef3c7;color:#92400e;padding:3px 6px;border-radius:3px;font-size:12px;display:inline-block;margin-top:2px;">${filterSummary}</code></div>`;
                } else if (toolName.includes('query')) {
                    auditHtml += `<div style="margin-left: 8px;"><span style="color: #e53e3e; font-weight: bold;">⚠️ 无过滤条件 (全量拉取)</span></div>`;
                }

                const mcpResult = await mcpClient.callTool({
                    name: call.name,
                    arguments: call.args
                });

                const rawText = (mcpResult.content || [])
                    .filter(c => c.type === "text")
                    .map(c => c.text)
                    .join("\n");

                let parsedResponse;
                try {
                    const parsed = JSON.parse(rawText);
                    if (Array.isArray(parsed)) {
                        parsedResponse = { data: parsed };
                    } else if (parsed !== null && typeof parsed === 'object') {
                        parsedResponse = parsed;
                    } else {
                        parsedResponse = { result: parsed };
                    }
                } catch {
                    parsedResponse = { result: rawText };
                }

                let rowCount = "N/A";
                try {
                    if (parsedResponse.data && Array.isArray(parsedResponse.data)) rowCount = parsedResponse.data.length;
                    else if (parsedResponse.result && typeof parsedResponse.result === "string") rowCount = "文本";
                    else rowCount = "对象";
                } catch (e) { }

                const isError = mcpResult.isError;
                if (isError) {
                    console.error(`⚠️ MCP 工具报错:`, rawText);
                    auditHtml += `<div style="margin-left: 8px; color: #dc2626;">❌ 报错: ${rawText.substring(0, 150)}...</div>`;
                } else if (toolName.includes('query')) {
                    const color = rowCount === "N/A" ? "#dc2626" : "#047857";
                    auditHtml += `<div style="margin-left: 8px;"><span style="color: #64748b;">📊 返回:</span> <strong style="color: ${color};">${rowCount} 行数据</strong></div>`;
                } else if (toolName.includes('list')) {
                    auditHtml += `<div style="margin-left: 8px;"><span style="color: #64748b;">📊 找到:</span> <strong style="color: #047857;">${rowCount} 个数据源</strong></div>`;
                } else if (toolName.includes('metadata')) {
                    auditHtml += `<div style="margin-left: 8px;"><span style="color: #64748b;">📊 状态:</span> <strong style="color: #047857;">✓ 已获取字段元数据</strong></div>`;
                } else {
                    auditHtml += `<div style="margin-left: 8px;"><span style="color: #64748b;">📊 返回:</span> ${rowCount}</div>`;
                }
                auditHtml += `</div>`;

                console.log(`📦 工具 ${call.name} 返回数据行数: ${rowCount}`);
                toolResults.push({ functionResponse: { name: call.name, response: parsedResponse } });
            }
            result = await chat.sendMessage(toolResults);
            response = result.response;
        }

        if (toolCalled) {
            const skippedNote = totalSkipped > 0 ? `，跳过 ${totalSkipped} 次空调用` : '';
            auditHtml += `<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #e5e7eb; color: #78716c; font-size: 12px;">共 ${iteration} 轮工具调用${skippedNote}</div>`;
            auditHtml += `</div>\n\n`;
        } else {
            auditHtml = "";
        }

        const finalAiText = response.text();
        const finalCombinedResponse = auditHtml + finalAiText;

        res.json({ status: "success", response: finalCombinedResponse });
    } catch (error) {
        console.error("❌ API 报错:", error);
        res.status(500).json({ error: error.message });
    }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, '0.0.0.0', async () => {
    console.log(`🚀 服务就绪: http://localhost:${PORT}`);
    await startMcpServer();
});