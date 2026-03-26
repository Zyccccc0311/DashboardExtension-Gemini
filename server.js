import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { platform } from 'os';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const isWindows = platform() === 'win32';
const PORT = Number(process.env.PORT || 8080);
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-2.5-flash';
const OPENAPI_PATH = process.env.VDS_OPENAPI_PATH || 'C:\\Users\\zyc20\\Downloads\\openapi.json';

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

let mcpClient = null;
let mcpConnectionError = null;
let openApiReference = '';

function loadOpenApiReference() {
    try {
        if (!fs.existsSync(OPENAPI_PATH)) {
            return `OpenAPI file not found at ${OPENAPI_PATH}. Use runtime MCP tool schemas as the primary source of truth.`;
        }

        const document = JSON.parse(fs.readFileSync(OPENAPI_PATH, 'utf8'));
        const operations = Object.entries(document.paths || {}).map(([route, config]) => {
            const method = Object.keys(config || {})[0];
            const operation = config?.[method] || {};
            return `- ${method?.toUpperCase()} ${route}: ${operation.summary || operation.operationId || 'No summary'}${operation.description ? ` | ${operation.description}` : ''}`;
        });

        const functionEnum = document?.components?.schemas?.Function?.enum || [];
        const filterEnum = document?.components?.schemas?.Filter?.properties?.filterType?.enum || [];

        return [
            'VizQL Data Service reference from OpenAPI:',
            ...operations,
            '',
            `Datasource shape: ${JSON.stringify(document?.components?.schemas?.Datasource || {})}`,
            `Query shape: ${JSON.stringify(document?.components?.schemas?.Query || {})}`,
            `Supported Function enum: ${functionEnum.join(', ')}`,
            `Supported Filter type enum: ${filterEnum.join(', ')}`
        ].join('\n');
    } catch (error) {
        return `Failed to load OpenAPI reference: ${error.message}. Use runtime MCP tool schemas as the primary source of truth.`;
    }
}

openApiReference = loadOpenApiReference();

function getModel(systemInstruction) {
    return genAI.getGenerativeModel({
        model: GEMINI_MODEL,
        systemInstruction
    });
}

function cleanSchema(schema) {
    if (!schema || typeof schema !== 'object') return schema;
    if (Array.isArray(schema.type)) {
        schema = { ...schema, type: schema.type.find((item) => item !== 'null') || schema.type[0] };
    }

    const whitelist = ['type', 'properties', 'required', 'items', 'description', 'enum', 'anyOf', 'oneOf', 'const', 'default', 'format', 'minimum', 'maximum'];
    const next = Array.isArray(schema) ? [] : {};

    for (const key of Object.keys(schema)) {
        if (!whitelist.includes(key)) continue;
        next[key] = typeof schema[key] === 'object' && schema[key] !== null
            ? cleanSchema(schema[key])
            : schema[key];
    }

    return next;
}

function convertToGeminiTools(mcpTools) {
    return [{
        functionDeclarations: mcpTools.map((tool) => ({
            name: tool.name,
            description: tool.description,
            parameters: cleanSchema(tool.inputSchema)
        }))
    }];
}

function normalizeText(value) {
    return String(value ?? '')
        .toLowerCase()
        .replace(/[_-]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function scoreDatasourceMatch(left, right) {
    const a = normalizeText(left);
    const b = normalizeText(right);
    if (!a || !b) return 0;
    if (a === b) return 1;
    if (a.includes(b) || b.includes(a)) return 0.92;

    const leftTokens = new Set(a.split(' ').filter(Boolean));
    const rightTokens = new Set(b.split(' ').filter(Boolean));
    let overlap = 0;

    for (const token of leftTokens) {
        if (rightTokens.has(token)) overlap += 1;
    }

    return overlap / Math.max(leftTokens.size || 1, rightTokens.size || 1);
}

function resolveDashboardDatasourceContext(dashboardMeta = []) {
    const grouped = new Map();

    for (const item of dashboardMeta) {
        const datasourceName = item?.datasourceName || item?.name;
        if (!datasourceName) continue;

        if (!grouped.has(datasourceName)) {
            grouped.set(datasourceName, {
                datasourceName,
                worksheetNames: new Set(),
                localIds: new Set(),
                fields: new Set()
            });
        }

        const entry = grouped.get(datasourceName);
        if (item.worksheetName) entry.worksheetNames.add(item.worksheetName);
        if (item.localId || item.luid) entry.localIds.add(item.localId || item.luid);
        for (const field of item.fields || []) {
            entry.fields.add(field);
        }
    }

    return [...grouped.values()].map((entry) => ({
        datasourceName: entry.datasourceName,
        worksheetNames: [...entry.worksheetNames],
        localIds: [...entry.localIds],
        fields: [...entry.fields]
    }));
}

function pickBestDatasourceMatches(dashboardDatasources, mcpDatasources) {
    return dashboardDatasources.map((dashboardDatasource) => {
        const candidates = mcpDatasources
            .map((mcpDatasource) => ({
                dashboardName: dashboardDatasource.datasourceName,
                worksheetNames: dashboardDatasource.worksheetNames,
                matchedName: mcpDatasource.name || mcpDatasource.contentUrl || mcpDatasource.datasourceName || '',
                luid: mcpDatasource.luid || mcpDatasource.id || mcpDatasource.datasourceLuid || '',
                score: scoreDatasourceMatch(
                    dashboardDatasource.datasourceName,
                    mcpDatasource.name || mcpDatasource.contentUrl || mcpDatasource.datasourceName || ''
                )
            }))
            .sort((left, right) => right.score - left.score);

        return {
            dashboardDatasource,
            best: candidates[0] || null,
            alternatives: candidates.slice(1, 4)
        };
    });
}

function extractTextContent(mcpResult) {
    return (mcpResult?.content || [])
        .filter((item) => item.type === 'text')
        .map((item) => item.text)
        .join('\n');
}

function parseToolResponse(mcpResult) {
    const rawText = extractTextContent(mcpResult);

    try {
        const parsed = JSON.parse(rawText);
        if (Array.isArray(parsed)) return { rawText, parsedResponse: { data: parsed } };
        if (parsed && typeof parsed === 'object') return { rawText, parsedResponse: parsed };
        return { rawText, parsedResponse: { result: parsed } };
    } catch {
        return { rawText, parsedResponse: { result: rawText } };
    }
}

function extractMetadataRecords(metadataPayload) {
    if (!metadataPayload) return [];
    if (Array.isArray(metadataPayload)) return metadataPayload;
    if (Array.isArray(metadataPayload.fields)) return metadataPayload.fields;
    if (Array.isArray(metadataPayload.data)) return metadataPayload.data;
    if (Array.isArray(metadataPayload?.data?.data)) return metadataPayload.data.data;
    if (Array.isArray(metadataPayload?.result?.fields)) return metadataPayload.result.fields;
    if (Array.isArray(metadataPayload?.result?.data)) return metadataPayload.result.data;
    if (Array.isArray(metadataPayload?.metadata?.fields)) return metadataPayload.metadata.fields;
    if (Array.isArray(metadataPayload?.metadata?.data)) return metadataPayload.metadata.data;
    return [];
}

function getFieldCaption(record) {
    return record?.fieldCaption || record?.caption || record?.fieldName || record?.name || null;
}

function summarizeMetadata(metadataPayload, maxFields = 120) {
    const records = extractMetadataRecords(metadataPayload).slice(0, maxFields);
    if (!records.length) return 'No metadata records loaded yet.';

    return records.map((record) => JSON.stringify({
        fieldCaption: getFieldCaption(record),
        dataType: record?.dataType || record?.datatype || record?.type || null,
        role: record?.role || record?.fieldRole || null,
        description: record?.description || null
    })).join('\n');
}

function summarizeTools(mcpTools) {
    return mcpTools.map((tool) => {
        const parameters = cleanSchema(tool.inputSchema || {});
        return `- ${tool.name}: ${tool.description || 'No description'} | inputSchema=${JSON.stringify(parameters)}`;
    }).join('\n');
}

function summarizeMatches(matches) {
    if (!matches.length) return 'No dashboard datasource context was provided.';
    return matches.map((item) => {
        const best = item.best
            ? `${item.best.matchedName} (${item.best.luid}) score=${item.best.score.toFixed(3)}`
            : 'No match';
        const alternatives = item.alternatives.length
            ? item.alternatives.map((candidate) => `${candidate.matchedName} (${candidate.luid}) score=${candidate.score.toFixed(3)}`).join(' | ')
            : 'None';
        return `- Dashboard datasource "${item.dashboardDatasource.datasourceName}" -> best match: ${best}; alternatives: ${alternatives}`;
    }).join('\n');
}

function summarizeHistory(history = []) {
    if (!Array.isArray(history) || history.length === 0) return 'No prior conversation.';
    return history.slice(-8).map((item) => `${item.role === 'assistant' ? 'Assistant' : 'User'}: ${String(item.content || '').trim()}`).join('\n');
}

function extractTaggedData(text) {
    const rawText = String(text || '');
    let plainText = rawText.replace(/\[DATA\][\s\S]*?\[\/DATA\]/gi, '').trim();
    let data = [];

    // 按 [DATA]...[/DATA] 块分割，同时捕获每块前面最近的标题行作为分组标签
    const blockCount = (rawText.match(/\[DATA\]/gi) || []).length;
    const blockRegex = /([^\n]*)\n?\[DATA\]([\s\S]*?)\[\/DATA\]/gi;
    let match;

    while ((match = blockRegex.exec(rawText)) !== null) {
        const label = match[1].replace(/[*#`]/g, '').trim(); // 清理 markdown 符号
        const rawJson = match[2].replace(/```json/gi, '').replace(/```/g, '').trim();
        try {
            const parsed = JSON.parse(rawJson);
            if (Array.isArray(parsed)) {
                // 只在多个 [DATA] 块时才注入 _group，避免单块时产生噪音
                const enriched = (label && blockCount > 1)
                    ? parsed.map(row => ({ _group: label, ...row }))
                    : parsed;
                data = data.concat(enriched);
            }
        } catch {
            // skip invalid block
        }
    }

    return { plainText, data };
}

function stripHtmlTags(text) {
    return String(text || '').replace(/<[^>]+>/g, '').trim();
}

function formatNestedValue(value) {
    if (value === null || value === undefined) return '';
    if (Array.isArray(value)) return value.map((item) => formatNestedValue(item)).join(' | ');
    if (typeof value === 'object') {
        return Object.entries(value).map(([key, item]) => `${key}: ${formatNestedValue(item)}`).join(', ');
    }
    return value;
}

function normalizeFinalData(rows) {
    if (!Array.isArray(rows)) return [];
    return rows.map((row) => {
        if (!row || typeof row !== 'object' || Array.isArray(row)) {
            return { Value: formatNestedValue(row) };
        }

        const normalized = {};
        for (const [key, value] of Object.entries(row)) {
            normalized[key] = formatNestedValue(value);
        }
        return normalized;
    });
}

function buildAuditEntry(call, parsedResponse, isError = false) {
    const payload = Array.isArray(parsedResponse?.data)
        ? `${parsedResponse.data.length} rows`
        : Object.keys(parsedResponse || {}).join(', ') || 'No structured payload';

    return {
        step: call.name,
        details: `${isError ? 'Error' : 'OK'} | args=${JSON.stringify(call.arguments || call.args || {})} | payload=${payload}`
    };
}

async function startMcpServer() {
    const command = isWindows ? 'npx.cmd' : 'npx';
    const args = ['-y', '@tableau/mcp-server@latest'];
    let transport = new StdioClientTransport({
        command,
        args,
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
            mcpClient = new Client({ name: 'tableau-mcp-bridge', version: '1.0.0' }, { capabilities: {} });
            await mcpClient.connect(transport);
            mcpConnectionError = null;
            console.log('成功连接到 Tableau MCP Server!');
            break;
        } catch (err) {
            console.error(`MCP 连接失败 (${attempt}/${MAX_RETRIES}):`, err.message);
            mcpClient = null;
            mcpConnectionError = err;
            if (attempt < MAX_RETRIES) {
                console.log(`${5 * attempt} 秒后重试...`);
                await new Promise(r => setTimeout(r, 5000 * attempt));
                transport = new StdioClientTransport({
                    command,
                    args,
                    env: {
                        ...process.env,
                        SERVER: process.env.SERVER,
                        SITE_NAME: process.env.SITE_NAME,
                        PAT_NAME: process.env.PAT_NAME,
                        PAT_VALUE: process.env.PAT_VALUE
                    }
                });
            }
        }
    }

    if (!mcpClient) {
        throw mcpConnectionError || new Error('MCP 连接失败');
    }
}

async function ensureMcpReady() {
    if (mcpClient) return;

    try {
        await startMcpServer();
    } catch (error) {
        mcpConnectionError = error;
        mcpClient = null;
        throw error;
    }
}

async function listMcpTools() {
    const result = await mcpClient.listTools();
    return result?.tools || [];
}

async function listDatasources() {
    const mcpResult = await mcpClient.callTool({ name: 'list-datasources', arguments: {} });
    const rawText = extractTextContent(mcpResult);

    try {
        const parsed = JSON.parse(rawText);
        if (Array.isArray(parsed)) return parsed;
        if (Array.isArray(parsed?.data)) return parsed.data;
        if (Array.isArray(parsed?.datasources)) return parsed.datasources;
        return [];
    } catch {
        return [];
    }
}

async function readDatasourceMetadata(datasourceLuid) {
    const mcpResult = await mcpClient.callTool({
        name: 'get-datasource-metadata',
        arguments: { datasourceLuid }
    });
    return parseToolResponse(mcpResult).parsedResponse;
}

async function executeMcpCall(call) {
    const toolName = call.name;
    const toolArgs = call.arguments ?? call.args ?? {};
    const mcpResult = await mcpClient.callTool({ name: toolName, arguments: toolArgs });
    const { rawText, parsedResponse } = parseToolResponse(mcpResult);
    return { mcpResult, rawText, parsedResponse };
}

function buildSystemInstruction() {
    return [
        'You are a Tableau data assistant working through MCP tools.',
        'Your job is to answer the user accurately by using datasource metadata and MCP tool descriptions.',
        'Always prefer runtime MCP tool schemas and descriptions over any static document if they differ.',
        'All available datasource metadata is pre-loaded in the prompt under "Available datasources". Do NOT call list-datasources or get-datasource-metadata — use the datasourceLuid and field information already provided.',
        'Never invent fields, datasource ids, or results.',
        'Use only fields that exist in metadata.',
        'For measures, include a function when the schema requires one.',
        'If the question asks for explanations or reasons, gather enough evidence first and make it clear when a reason is an inference from the data.',
        'When answering with tabular results, end with [DATA]...[/DATA] containing a valid JSON array.',
        '',
        'STRATEGY FOR MULTI-QUERY MERGED RESULTS:',
        'When your analysis involves multiple separate queries that together form a complete picture (e.g., year-over-year comparison, multi-dimension breakdown):',
        '  - In your final [DATA] block, output the COMPLETE merged/computed table — not just the last raw query result.',
        '  - Example: if you queried 2024 profit and 2025 profit separately, your [DATA] should contain rows with columns for both years and the delta, like: [{"Category": "Technology", "2024利润": 10156, "2025利润": 7288, "变化": -2868}]',
        '  - The [DATA] block should mirror what you present in your text analysis, so the user can export the complete comparison as CSV.',
        '',
        'STRATEGY FOR TOP-N PER GROUP QUERIES:',
        'When the user asks for "top N per group" (e.g., top 10 products per year per segment):',
        '  - The TOP filter applies globally across the entire result set, NOT per group.',
        '  - Do NOT ask the user to specify a group. Solve it autonomously:',
        '    Step 1: Query each dimension separately to get all distinct group values (e.g., all years, all segments).',
        '    Step 2: For each combination of group values, run one query with a TOP filter.',
        '    Step 3: Combine all results and return them together.',
        '  - Example: "top 10 products per year per segment" with 4 years × 3 segments = 12 separate queries. Run them all.',
        '  - IMPORTANT: Make at most 4 tool calls in parallel per turn to avoid malformed output. If you need 12 calls, do 4+4+4 across multiple turns.',
        '  - IMPORTANT: When running per-group queries, keep each sub-query simple — query only the leaf-level fields (e.g., Product Name and Profit). Do NOT add group dimensions (Year, Segment) to sub-queries or aggregate products into a single field. The grouping context will be preserved through the conversation.',
        '  - Never give up and ask the user — always attempt the iterative approach first.',
        '',
        openApiReference
    ].join('\n');
}

function buildUserPrompt({ message, history, toolSummary, datasourceContexts }) {
    const datasourceSection = datasourceContexts.length > 0
        ? datasourceContexts.map(ds =>
            `### ${ds.name}\ndatasourceLuid: "${ds.luid}" (match score: ${ds.score.toFixed(2)})\n${ds.summary}`
          ).join('\n\n---\n\n')
        : 'No datasources matched from the dashboard context. Call list-datasources to find available datasources.';

    return [
        'Conversation history:',
        summarizeHistory(history),
        '',
        'Runtime MCP tool guide:',
        toolSummary,
        '',
        'Available datasources (matched from current dashboard):',
        datasourceSection,
        '',
        'Task instructions:',
        '- Metadata for all available datasources is already provided above — do NOT call list-datasources or get-datasource-metadata.',
        '- Use the datasourceLuid shown above directly when calling query-datasource.',
        '- Choose the most appropriate datasource based on the user question.',
        '- If you need to explain why a metric changed, fetch enough supporting metrics before concluding.',
        '',
        `User question: ${message}`
    ].join('\n');
}

app.get('/api/status', async (_req, res) => {
    try {
        await ensureMcpReady();
        const tools = await listMcpTools();
        res.json({
            ok: true,
            model: GEMINI_MODEL,
            mcpConnected: true,
            openApiPath: OPENAPI_PATH,
            toolCount: tools.length
        });
    } catch (error) {
        res.status(503).json({
            ok: false,
            mcpConnected: false,
            model: GEMINI_MODEL,
            openApiPath: OPENAPI_PATH,
            error: error.message
        });
    }
});

app.post('/api/chat', async (req, res) => {
    try {
        const { message, history = [], dashboardMeta = [] } = req.body || {};
        if (!String(message || '').trim()) {
            return res.status(400).json({ error: 'message is required' });
        }

        await ensureMcpReady();

        const [mcpTools, mcpDatasources] = await Promise.all([
            listMcpTools(),
            listDatasources()
        ]);

        const dashboardDatasources = resolveDashboardDatasourceContext(dashboardMeta);
        const datasourceMatches = pickBestDatasourceMatches(dashboardDatasources, mcpDatasources);

        // 所有 score >= 0.5 的匹配，按 score 降序排列
        const matchedDatasources = datasourceMatches
            .filter(item => item.best?.luid && item.best.score >= 0.5)
            .sort((a, b) => b.best.score - a.best.score)
            .map(item => item.best);

        // 并行取所有匹配数据源的 metadata
        const datasourceContexts = await Promise.all(
            matchedDatasources.map(async ds => {
                try {
                    const metadata = await readDatasourceMetadata(ds.luid);
                    return { name: ds.matchedName, luid: ds.luid, score: ds.score, summary: summarizeMetadata(metadata) };
                } catch (err) {
                    return { name: ds.matchedName, luid: ds.luid, score: ds.score, summary: `Failed to load metadata: ${err.message}` };
                }
            })
        );

        const model = getModel(buildSystemInstruction());
        const chat = model.startChat({ tools: convertToGeminiTools(mcpTools) });
        const prompt = buildUserPrompt({
            message,
            history,
            toolSummary: summarizeTools(mcpTools),
            datasourceContexts
        });

        console.log('\n' + '='.repeat(60));
        console.log(`[CHAT] 用户问题: ${message}`);
        console.log('='.repeat(60));

        let result = await chat.sendMessage(prompt);
        let response = result.response;
        const audit = [];
        let lastQueryData = [];
        let turnCount = 0;

        let malformedRetries = 0;

        while (turnCount < 12) {
            // 检测 MALFORMED_FUNCTION_CALL
            const finishReason = response.candidates?.[0]?.finishReason;
            if (finishReason === 'MALFORMED_FUNCTION_CALL') {
                malformedRetries += 1;
                console.log(`\n[Turn ${turnCount}] ⚠️  MALFORMED_FUNCTION_CALL (第${malformedRetries}次) — 发送重试提示`);
                if (malformedRetries > 3) {
                    console.log('[Turn ${turnCount}] 重试次数超限，退出循环');
                    break;
                }
                const luidsHint = datasourceContexts.length
                    ? `Available datasources: ${datasourceContexts.map(d => `"${d.name}" luid="${d.luid}"`).join(', ')}.`
                    : '';
                result = await chat.sendMessage(
                    `Your last function call was malformed. Retry with at most 4 tool calls at a time. ${luidsHint} The user asked: "${message}". Continue working — do not ask the user to repeat themselves.`
                );
                response = result.response;
                continue;
            }

            const calls = response.functionCalls() || [];

            // 打印模型当前的思考文本（如果有）
            let thinkingText = '';
            try { thinkingText = response.text(); } catch {}
            if (thinkingText) {
                console.log(`\n[Turn ${turnCount}] Gemini 文本输出:\n${thinkingText}`);
            }

            if (!calls.length) {
                console.log(`\n[Turn ${turnCount}] 没有工具调用，退出循环`);
                break;
            }

            turnCount += 1;
            console.log(`\n[Turn ${turnCount}] Gemini 发起 ${calls.length} 个工具调用:`);

            const toolResults = [];

            for (const call of calls) {
                console.log(`  → 调用: ${call.name}`);
                console.log(`    参数: ${JSON.stringify(call.args ?? call.arguments ?? {}, null, 2)}`);

                const { mcpResult, parsedResponse } = await executeMcpCall(call);
                const isError = Boolean(mcpResult?.isError);

                const resultPreview = JSON.stringify(parsedResponse).slice(0, 300);
                console.log(`    结果: ${isError ? '❌ ERROR' : '✅ OK'} | ${resultPreview}${resultPreview.length >= 300 ? '...(truncated)' : ''}`);

                audit.push(buildAuditEntry(call, parsedResponse, isError));

                if (call.name === 'query-datasource' && Array.isArray(parsedResponse?.data)) {
                    lastQueryData = parsedResponse.data;
                    console.log(`    数据行数: ${parsedResponse.data.length}`);
                }

                toolResults.push({
                    functionResponse: {
                        name: call.name,
                        response: parsedResponse
                    }
                });
            }

            result = await chat.sendMessage(toolResults);
            response = result.response;
        }

        console.log(`\n[DONE] 共执行 ${turnCount} 轮工具调用`);

        let finalText = '';
        try {
            finalText = response.text() || '';
        } catch (err) {
            console.log(`[WARN] response.text() 抛出异常: ${err.message}`);
        }

        if (!finalText.trim()) {
            console.log('[WARN] finalText 为空，检查最后一次 response 的结构:');
            console.log('  candidates:', JSON.stringify(response.candidates?.map(c => ({
                finishReason: c.finishReason,
                parts: c.content?.parts?.map(p => ({ type: Object.keys(p)[0], len: JSON.stringify(p).length }))
            })), null, 2));

            // 兜底：主动要求模型总结
            console.log('[FALLBACK] 发送兜底 prompt 要求模型输出文本...');
            try {
                const fallback = await chat.sendMessage('Based on all the tool results above, provide your complete final answer to the user now. Do not call any more tools.');
                finalText = fallback.response.text() || '';
                console.log(`[FALLBACK] 兜底结果: ${finalText.slice(0, 200)}`);
            } catch (err) {
                console.log(`[FALLBACK] 兜底失败: ${err.message}`);
            }
        }

        console.log(`\n[FINAL TEXT] (前500字):\n${finalText.slice(0, 500)}`);
        console.log('='.repeat(60) + '\n');

        const tagged = extractTaggedData(finalText);
        const finalData = normalizeFinalData(tagged.data.length ? tagged.data : lastQueryData);
        const plainMessage = stripHtmlTags(tagged.plainText || finalText || 'No response text returned.');

        res.json({
            message: plainMessage,
            data: finalData,
            audit,
            datasourceContexts
        });
    } catch (error) {
        console.error('Chat API error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.listen(PORT, '0.0.0.0', async () => {
    console.log(`Server ready: http://localhost:${PORT}`);

    try {
        await ensureMcpReady();
    } catch (error) {
        console.error('MCP connection failed at startup:', error.message);
    }
});
