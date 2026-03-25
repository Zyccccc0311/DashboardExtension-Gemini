import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import path from 'path';
import { fileURLToPath } from 'url';
import { platform } from 'os';
import { normalizeQueryDatasourceCall } from './vdsQuery.js';
import { buildVdsQueryFromDsl, collectDslDiagnostics, createEmptyDsl } from './queryDsl.js';
import { extractQuerySlots } from './querySlots.js';
import { getResolvedFieldCaption, resolveFieldRoles } from './queryRoles.js';

// 1. 基础配置
dotenv.config();
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const isWindows = platform() === 'win32';

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));
// 2. Gemini 初始化
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
let model = genAI.getGenerativeModel({
    model: "gemini-2.5-flash",
    systemInstruction: `你是一个 Tableau 数据分析助手，通过调用工具查询数据并回答用户问题。
【工作流程】按顺序执行：
1. list-datasources -> 获取真实 datasourceLuid（UUID 格式）
2. get-datasource-metadata -> 用 datasourceLuid 获取精确字段名和类型
3. query-datasource -> 用元数据中的字段名构造查询

【Filter 语法】
- 年份过滤：{"field":{"fieldCaption":"Order Date"},"filterType":"QUANTITATIVE_DATE","quantitativeFilterType":"RANGE","minDate":"2025-01-01","maxDate":"2025-12-31"}
- 维度过滤：{"field":{"fieldCaption":"Segment"},"filterType":"SET","values":["Consumer"],"exclude":false}
- 数值过滤：{"field":{"fieldCaption":"Sales","function":"SUM"},"filterType":"QUANTITATIVE_NUMERICAL","quantitativeFilterType":"RANGE","min":0,"max":10000}
- Top N：{"field":{"fieldCaption":"State"},"filterType":"TOP","howMany":5,"fieldToMeasure":{"fieldCaption":"Sales","function":"SUM"}}
- 合法 filterType：SET、TOP、QUANTITATIVE_NUMERICAL、QUANTITATIVE_DATE、DATE

【分组 Top N】
"每个 X 的 Top N" 必须循环查询：先查出所有分组值，再对每个分组单独用 SET + TOP filter 各查一次。

【其他规则】
- 度量字段必须指定 function（SUM/AVG/COUNT/MIN/MAX）
- 不支持 ad-hoc 计算字段，衍生指标（如利润率）请查出原始字段后自行计算
- 严禁伪造数据，查询失败时修正参数后重试
- 回答末尾将数据用 [DATA][/DATA] 标签包裹为扁平 JSON 数组`
});

// 3. MCP 连接
model = genAI.getGenerativeModel({
    model: "gemini-2.5-flash",
    systemInstruction: `You are a Tableau data assistant.
Always follow this order:
1) list-datasources
2) select datasourceLuid
3) get-datasource-metadata
4) query-datasource

Rules:
- Use only fields from metadata.
- For measures, include function: SUM/AVG/COUNT/MIN/MAX.
- For "Top N per group", first fetch group values, then query each group.
- Do not fabricate data.
- End final answer with [DATA]...[/DATA] as valid JSON array.`
});

let mcpClient;

async function startMcpServer() {
    const command = isWindows ? 'npx.cmd' : 'npx';
    const args = ["-y", "@tableau/mcp-server@latest"];
    let transport = new StdioClientTransport({
        command, args,
        env: { ...process.env, SERVER: process.env.SERVER, SITE_NAME: process.env.SITE_NAME, PAT_NAME: process.env.PAT_NAME, PAT_VALUE: process.env.PAT_VALUE }
    });

    const MAX_RETRIES = 3;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            mcpClient = new Client({ name: "tableau-mcp-bridge", version: "1.0.0" }, { capabilities: {} });
            await mcpClient.connect(transport);
            console.log("成功连接到 Tableau MCP Server!");
            break;
        } catch (err) {
            console.error(`MCP 连接失败 (${attempt}/${MAX_RETRIES}):`, err.message);
            mcpClient = null;
            if (attempt < MAX_RETRIES) {
                console.log(`${5 * attempt} 秒后重试...`);
                await new Promise(r => setTimeout(r, 5000 * attempt));
                transport = new StdioClientTransport({
                    command, args,
                    env: { ...process.env, SERVER: process.env.SERVER, SITE_NAME: process.env.SITE_NAME, PAT_NAME: process.env.PAT_NAME, PAT_VALUE: process.env.PAT_VALUE }
                });
            }
        }
    }
}

// 4. 工具格式转换
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

function extractMetadataRecords(metadataPayload) {
    if (!metadataPayload) return [];
    if (Array.isArray(metadataPayload)) return metadataPayload;
    if (Array.isArray(metadataPayload.fields)) return metadataPayload.fields;
    if (Array.isArray(metadataPayload.data)) return metadataPayload.data;
    if (Array.isArray(metadataPayload?.result?.data)) return metadataPayload.result.data;
    if (Array.isArray(metadataPayload?.result?.fields)) return metadataPayload.result.fields;
    if (Array.isArray(metadataPayload?.metadata?.data)) return metadataPayload.metadata.data;
    if (Array.isArray(metadataPayload?.metadata?.fields)) return metadataPayload.metadata.fields;
    if (Array.isArray(metadataPayload?.output?.data)) return metadataPayload.output.data;
    if (Array.isArray(metadataPayload?.output?.fields)) return metadataPayload.output.fields;
    return [];
}

function getFieldCaption(record) {
    return record?.fieldCaption || record?.caption || record?.fieldName || record?.name || null;
}

function findFieldByCandidates(records, candidates = []) {
    const prepared = extractMetadataRecords(records).map((record) => ({
        caption: getFieldCaption(record),
        record
    })).filter((item) => item.caption);

    for (const candidate of candidates) {
        const exact = prepared.find((item) => normalizeText(item.caption) === normalizeText(candidate));
        if (exact) return exact.record;
    }

    for (const candidate of candidates) {
        const fuzzy = prepared.find((item) => normalizeText(item.caption).includes(normalizeText(candidate)));
        if (fuzzy) return fuzzy.record;
    }

    return null;
}

function parseDeterministicPrompt(prompt) {
    const text = String(prompt || '').trim();
    const normalized = normalizeText(text);

    const topMatch = text.match(/(\d{4})\s*年\s*(.+?)\s*(利润|销售额|销售|销量)\s*前\s*五\s*的\s*(.+)/i);
    if (topMatch) {
        return {
            intent: 'top_n_member_metric_entity',
            year: topMatch[1],
            member: topMatch[2].trim(),
            metricLabel: topMatch[3].trim(),
            entityLabel: topMatch[4].trim()
        };
    }

    const groupMatch = text.match(/(\d{4})\s*年\s*每个\s*(.+?)\s*的\s*(利润|销售额|销售|销量)/i);
    if (groupMatch) {
        return {
            intent: 'group_metric_by_year',
            year: groupMatch[1],
            groupLabel: groupMatch[2].trim(),
            metricLabel: groupMatch[3].trim()
        };
    }

    const memberMatch = text.match(/(\d{4})\s*年\s*(.+?)\s*的\s*(利润|销售额|销售|销量)/i);
    if (memberMatch && !normalized.includes('每个')) {
        return {
            intent: 'member_metric_by_year',
            year: memberMatch[1],
            member: memberMatch[2].trim(),
            metricLabel: memberMatch[3].trim()
        };
    }

    return null;
}

function resolveMetricField(metadataRecords, metricLabel) {
    if (/利润/.test(metricLabel)) {
        return findFieldByCandidates(metadataRecords, ['Profit', '利润']);
    }
    if (/销售额|销售/.test(metricLabel)) {
        return findFieldByCandidates(metadataRecords, ['Sales', '销售额', '销售']);
    }
    if (/销量/.test(metricLabel)) {
        return findFieldByCandidates(metadataRecords, ['Quantity', '销量', '数量']);
    }
    return null;
}

function resolveGroupingField(metadataRecords, label) {
    return resolveFieldBySemanticLabel(metadataRecords, label, [label]);
}

function resolveEntityField(metadataRecords, label) {
    if (/产品|商品/.test(label)) {
        return findFieldByCandidates(metadataRecords, ['Product Name', 'Product', '商品', '产品']);
    }
    return findFieldByCandidates(metadataRecords, [label]);
}

function resolveMemberFilterField(metadataRecords, memberText) {
    if (/consumer|corporate|home office/i.test(memberText)) {
        const segmentField = findFieldByCandidates(metadataRecords, ['Segment', '客户细分']);
        if (segmentField) {
            return { field: segmentField, value: memberText };
        }
    }
    return null;
}

function parseDeterministicPromptV2(prompt) {
    const text = String(prompt || '').trim();
    const normalized = normalizeText(text);

    const groupedTopMatch = text.match(/(?:每年|所有年份|全部年份)\s*每个\s*(.+?)\s*(\u5229\u6da6|\u9500\u552e\u989d|\u9500\u552e|\u9500\u91cf|\u6298\u6263)\s*(\u524d|\u540e)\s*([0-9\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341\u4e24]+)\s*\u540d?\s*\u7684\s*(.+)/i);
    if (groupedTopMatch) {
        const topN = chineseNumeralToInt(groupedTopMatch[4]) || 10;
        return {
            intent: 'grouped_top_n_by_year',
            groupLabel: groupedTopMatch[1].trim(),
            metricLabel: groupedTopMatch[2].trim(),
            topDirection: groupedTopMatch[3] === '\u540e' ? 'BOTTOM' : 'TOP',
            topN,
            entityLabel: groupedTopMatch[5].trim()
        };
    }

    const topMatch = text.match(/(\d{4})\s*\u5e74\s*(.+?)\s*(\u5229\u6da6|\u9500\u552e\u989d|\u9500\u552e|\u9500\u91cf|\u6298\u6263)\s*(\u524d|\u540e)\s*([0-9\u4e00\u4e8c\u4e09\u56db\u4e94\u516d\u4e03\u516b\u4e5d\u5341\u4e24]+)\s*\u540d?\s*\u7684\s*(.+)/i);
    if (topMatch) {
        const topN = chineseNumeralToInt(topMatch[5]) || 5;
        return {
            intent: 'top_n_member_metric_entity',
            year: topMatch[1],
            member: topMatch[2].trim(),
            metricLabel: topMatch[3].trim(),
            topDirection: topMatch[4] === '\u540e' ? 'BOTTOM' : 'TOP',
            topN,
            entityLabel: topMatch[6].trim()
        };
    }

    const groupMatch = text.match(/(\d{4})\s*\u5e74\s*\u6bcf\u4e2a\s*(.+?)\s*\u7684\s*(\u5229\u6da6|\u9500\u552e\u989d|\u9500\u552e|\u9500\u91cf|\u6298\u6263)/i);
    if (groupMatch) {
        return {
            intent: 'group_metric_by_year',
            year: groupMatch[1],
            groupLabel: groupMatch[2].trim(),
            metricLabel: groupMatch[3].trim()
        };
    }

    const memberMatch = text.match(/(\d{4})\s*\u5e74\s*(.+?)\s*\u7684\s*(\u5229\u6da6|\u9500\u552e\u989d|\u9500\u552e|\u9500\u91cf|\u6298\u6263)/i);
    if (memberMatch && !normalized.includes('\u6bcf\u4e2a')) {
        return {
            intent: 'member_metric_by_year',
            year: memberMatch[1],
            member: memberMatch[2].trim(),
            metricLabel: memberMatch[3].trim()
        };
    }

    return null;
}

function resolveMetricFieldV2(metadataRecords, metricLabel) {
    if (/\u5229\u6da6/.test(metricLabel)) {
        return findFieldByCandidates(metadataRecords, ['Profit', '\u5229\u6da6']);
    }
    if (/\u9500\u552e\u989d|\u9500\u552e/.test(metricLabel)) {
        return findFieldByCandidates(metadataRecords, ['Sales', '\u9500\u552e\u989d', '\u9500\u552e']);
    }
    if (/\u9500\u91cf/.test(metricLabel)) {
        return findFieldByCandidates(metadataRecords, ['Quantity', '\u9500\u91cf', '\u6570\u91cf']);
    }
    if (/\u6298\u6263/.test(metricLabel)) {
        return findFieldByCandidates(metadataRecords, ['Discount', '\u6298\u6263']);
    }
    return null;
}

function resolveEntityFieldV2(metadataRecords, label) {
    return resolveFieldBySemanticLabel(metadataRecords, label, [label]);
}

function resolveMemberFilterFieldV2(metadataRecords, memberText) {
    if (/consumer|corporate|home office/i.test(memberText)) {
        const segmentField = findFieldByCandidates(metadataRecords, ['Segment', '\u5ba2\u6237\u7ec6\u5206']);
        if (segmentField) {
            return { field: segmentField, value: normalizeMemberValue(memberText) };
        }
    }
    if (/east|west|central|south/i.test(memberText)) {
        const regionField = findFieldByCandidates(metadataRecords, ['Region', '\u5730\u533a', '\u533a\u57df']);
        if (regionField) {
            return { field: regionField, value: normalizeMemberValue(memberText) };
        }
    }
    if (/technology|office supplies|furniture/i.test(memberText)) {
        const categoryField = findFieldByCandidates(metadataRecords, ['Category', '\u7c7b\u522b', '\u54c1\u7c7b']);
        if (categoryField) {
            return { field: categoryField, value: normalizeMemberValue(memberText) };
        }
    }
    return null;
}

function chineseNumeralToInt(value) {
    const text = String(value || '').trim();
    if (/^\d+$/.test(text)) return Number(text);

    const map = {
        '零': 0,
        '一': 1,
        '二': 2,
        '两': 2,
        '三': 3,
        '四': 4,
        '五': 5,
        '六': 6,
        '七': 7,
        '八': 8,
        '九': 9,
        '十': 10
    };

    if (text === '十') return 10;
    if (text.length === 2 && text.startsWith('十') && map[text[1]] !== undefined) {
        return 10 + map[text[1]];
    }
    if (text.length === 2 && text.endsWith('十') && map[text[0]] !== undefined) {
        return map[text[0]] * 10;
    }
    if (text.length === 3 && text[1] === '十' && map[text[0]] !== undefined && map[text[2]] !== undefined) {
        return map[text[0]] * 10 + map[text[2]];
    }

    return null;
}

function normalizeMemberValue(value) {
    const text = String(value || '').trim();
    const normalized = normalizeText(text);

    const memberMap = new Map([
        ['consumer', 'Consumer'],
        ['corporate', 'Corporate'],
        ['home office', 'Home Office'],
        ['homeoffice', 'Home Office'],
        ['east', 'East'],
        ['west', 'West'],
        ['central', 'Central'],
        ['south', 'South'],
        ['technology', 'Technology'],
        ['office supplies', 'Office Supplies'],
        ['officesupplies', 'Office Supplies'],
        ['furniture', 'Furniture']
    ]);

    return memberMap.get(normalized) || text;
}

function resolveFieldBySemanticLabel(metadataRecords, label, fallbackCandidates = []) {
    const text = String(label || '').trim();
    const normalized = normalizeText(text);

    const semanticCandidates = [
        { patterns: [/segment/i, /客户细分/, /细分/], candidates: ['Segment', '客户细分'] },
        { patterns: [/category/i, /类别/, /品类/], candidates: ['Category', '类别', '品类'] },
        { patterns: [/sub\s*-?\s*category/i, /子类别/, /子类/], candidates: ['Sub-Category', 'Sub Category', '子类别'] },
        { patterns: [/region/i, /地区/, /区域/], candidates: ['Region', '地区', '区域'] },
        { patterns: [/product name/i, /product/i, /商品/, /产品/], candidates: ['Product Name', 'Product', '商品', '产品'] },
        { patterns: [/customer name/i, /customer/i, /客户/], candidates: ['Customer Name', 'Customer', '客户'] },
        { patterns: [/state|province/i, /州/, /省/], candidates: ['State/Province', 'State', 'Province', '州', '省'] },
        { patterns: [/city/i, /城市/], candidates: ['City', '城市'] }
    ];

    for (const entry of semanticCandidates) {
        if (entry.patterns.some((pattern) => pattern.test(text) || pattern.test(normalized))) {
            const field = findFieldByCandidates(metadataRecords, entry.candidates);
            if (field) return field;
        }
    }

    return findFieldByCandidates(metadataRecords, [text, ...fallbackCandidates]);
}

async function getDatasourceMetadata(datasourceLuid) {
    const { parsedResponse } = await executeMcpCall({
        name: 'get-datasource-metadata',
        args: { datasourceLuid }
    });
    return parsedResponse || {};
}

async function runSingleDslQuery(datasourceLuid, dsl) {
    const query = buildVdsQueryFromDsl(dsl);
    const call = {
        name: 'query-datasource',
        args: {
            datasourceLuid,
            query
        }
    };
    const { mcpResult, parsedResponse } = await executeMcpCall(call);
    return { call, mcpResult, parsedResponse };
}

async function runDeterministicDslQuery(prompt, datasourceLuid) {
    const slots = extractQuerySlots(prompt);
    const parsed = slots ? {
        intent: slots.intent,
        year: slots.timeScope?.year || null,
        groupLabel: slots.groupBy || null,
        metricLabel: slots.metric || null,
        member: slots.memberFilters?.[0]?.value || null,
        topDirection: slots.ranking?.direction || null,
        topN: slots.ranking?.count || null,
        entityLabel: slots.entity || null,
        _source: 'slots'
    } : parseDeterministicPromptV2(prompt);
    if (!parsed) {
        console.log('deterministic DSL: prompt not matched');
        return null;
    }
    console.log(`deterministic DSL: parsed via ${parsed._source || 'legacy-parser'} intent=${parsed.intent}`);

    const metadataPayload = await getDatasourceMetadata(datasourceLuid);
    const metadataRecords = extractMetadataRecords(metadataPayload);
    console.log(`deterministic DSL: metadata records=${metadataRecords.length}`);
    if (metadataRecords.length === 0) {
        console.log('deterministic DSL: metadata records empty');
        return null;
    }

    const resolvedRoles = resolveFieldRoles(metadataRecords, {
        metric: parsed.metricLabel,
        groupBy: parsed.groupLabel,
        entity: parsed.entityLabel,
        memberFilters: parsed.member ? [{ value: parsed.member }] : []
    });
    const orderDateField = resolvedRoles.timeField;
    const metricField = resolvedRoles.metricField;
    if (!orderDateField || !metricField) {
        console.log(`deterministic DSL: failed field resolution orderDate=${!!orderDateField} metric=${!!metricField}`);
        return null;
    }

    const dsl = createEmptyDsl();
    if (parsed.year) {
        dsl.filters.push({ kind: 'year', fieldCaption: getFieldCaption(orderDateField), year: parsed.year });
    }

    if (parsed.intent === 'group_metric_by_year') {
        const groupField = resolvedRoles.groupField;
        if (!groupField) {
            console.log('deterministic DSL: group field not found');
            return null;
        }
        dsl.dimensions.push({ fieldCaption: getResolvedFieldCaption(groupField) });
        dsl.measures.push({
            fieldCaption: getResolvedFieldCaption(metricField),
            fieldAlias: `Total ${getResolvedFieldCaption(metricField)}`,
            function: 'SUM'
        });
    }

    if (parsed.intent === 'member_metric_by_year') {
        const memberFilter = resolvedRoles.memberFilter;
        if (!memberFilter) {
            console.log('deterministic DSL: member filter not found');
            return null;
        }
        dsl.measures.push({
            fieldCaption: getResolvedFieldCaption(metricField),
            fieldAlias: `Total ${getResolvedFieldCaption(metricField)}`,
            function: 'SUM'
        });
        dsl.filters.push({
            kind: 'set',
            fieldCaption: getResolvedFieldCaption(memberFilter.field),
            values: [memberFilter.value]
        });
    }

    if (parsed.intent === 'top_n_member_metric_entity') {
        const memberFilter = resolvedRoles.memberFilter;
        const entityField = resolvedRoles.entityField;
        if (!memberFilter || !entityField) {
            console.log(`deterministic DSL: topN resolution failed member=${!!memberFilter} entity=${!!entityField}`);
            return null;
        }
        dsl.dimensions.push({ fieldCaption: getResolvedFieldCaption(entityField) });
        dsl.measures.push({
            fieldCaption: getResolvedFieldCaption(metricField),
            fieldAlias: `Total ${getResolvedFieldCaption(metricField)}`,
            function: 'SUM',
            sortDirection: 'DESC',
            sortPriority: 1
        });
        dsl.filters.push({
            kind: 'set',
            fieldCaption: getResolvedFieldCaption(memberFilter.field),
            values: [memberFilter.value]
        });
        dsl.filters.push({
            kind: 'topN',
            fieldCaption: getResolvedFieldCaption(entityField),
            measureFieldCaption: getResolvedFieldCaption(metricField),
            measureFunction: 'SUM',
            howMany: parsed.topN || 5,
            direction: parsed.topDirection || 'TOP'
        });
    }

    if (parsed.intent === 'grouped_top_n_by_year') {
        const groupField = resolvedRoles.groupField;
        const entityField = resolvedRoles.entityField;
        if (!groupField || !entityField) {
            console.log(`deterministic DSL: grouped topN resolution failed group=${!!groupField} entity=${!!entityField}`);
            return null;
        }
        dsl.dimensions.push({
            fieldCaption: getResolvedFieldCaption(orderDateField),
            fieldAlias: 'Order Year',
            function: 'YEAR',
            sortDirection: 'ASC',
            sortPriority: 1
        });
        dsl.dimensions.push({
            fieldCaption: getResolvedFieldCaption(groupField),
            fieldAlias: getResolvedFieldCaption(groupField),
            sortDirection: 'ASC',
            sortPriority: 2
        });
        dsl.dimensions.push({
            fieldCaption: getResolvedFieldCaption(entityField),
            fieldAlias: getResolvedFieldCaption(entityField)
        });
        dsl.measures.push({
            fieldCaption: getResolvedFieldCaption(metricField),
            fieldAlias: `Total ${getResolvedFieldCaption(metricField)}`,
            function: 'SUM',
            sortDirection: parsed.topDirection === 'BOTTOM' ? 'ASC' : 'DESC',
            sortPriority: 3
        });
    }

    if (parsed.intent === 'compare_yoy_member_metric') {
        const memberFilter = resolvedRoles.memberFilter;
        if (!memberFilter) {
            console.log('deterministic DSL: member filter not found for yoy');
            return null;
        }

        const currentDsl = createEmptyDsl();
        currentDsl.measures.push({
            fieldCaption: getResolvedFieldCaption(metricField),
            fieldAlias: `Total ${getResolvedFieldCaption(metricField)}`,
            function: 'SUM'
        });
        currentDsl.filters.push({
            kind: 'year',
            fieldCaption: getResolvedFieldCaption(orderDateField),
            year: parsed.year
        });
        currentDsl.filters.push({
            kind: 'set',
            fieldCaption: getResolvedFieldCaption(memberFilter.field),
            values: [memberFilter.value]
        });

        const previousDsl = createEmptyDsl();
        previousDsl.measures.push({
            fieldCaption: getResolvedFieldCaption(metricField),
            fieldAlias: `Total ${getResolvedFieldCaption(metricField)}`,
            function: 'SUM'
        });
        previousDsl.filters.push({
            kind: 'year',
            fieldCaption: getResolvedFieldCaption(orderDateField),
            year: String(Number(parsed.year) - 1)
        });
        previousDsl.filters.push({
            kind: 'set',
            fieldCaption: getResolvedFieldCaption(memberFilter.field),
            values: [memberFilter.value]
        });

        const currentResult = await runSingleDslQuery(datasourceLuid, currentDsl);
        const previousResult = await runSingleDslQuery(datasourceLuid, previousDsl);
        const currentRow = currentResult.parsedResponse?.data?.[0] || {};
        const previousRow = previousResult.parsedResponse?.data?.[0] || {};
        const metricKey = Object.keys(currentRow).find((key) => /total|profit|sales|quantity|discount/i.test(key))
            || Object.keys(previousRow).find((key) => /total|profit|sales|quantity|discount/i.test(key));

        if (!metricKey) {
            console.log('deterministic DSL: yoy metric key not found');
            return null;
        }

        const currentValue = Number(currentRow[metricKey]) || 0;
        const previousValue = Number(previousRow[metricKey]) || 0;
        const diffValue = currentValue - previousValue;
        const yoyRate = previousValue === 0 ? null : diffValue / previousValue;

        return {
            type: 'yoy_analysis',
            parsed,
            queryResults: [currentResult, previousResult],
            analysisRows: [{
                Scope: memberFilter.value,
                Metric: getResolvedFieldCaption(metricField),
                'Current Year': parsed.year,
                'Current Value': currentValue,
                'Previous Year': String(Number(parsed.year) - 1),
                'Previous Value': previousValue,
                Difference: diffValue,
                'YoY Rate': yoyRate === null ? 'N/A' : yoyRate
            }],
            plainMessage: `${memberFilter.value} 在 ${parsed.year} 年的${parsed.metricLabel}与去年对比已完成。`
        };
    }

    const dslDiagnostics = collectDslDiagnostics(dsl);
    if (dslDiagnostics.length > 0) {
        console.log(`DSL diagnostics: ${dslDiagnostics.join(' | ')}`);
        return null;
    }

    const query = buildVdsQueryFromDsl(dsl);
    return {
        type: 'single_query',
        query,
        parsed,
        metadataPayload
    };
}

function applyGroupedTopN(rows, parsed) {
    if (!Array.isArray(rows) || rows.length === 0 || !parsed) return rows;
    if (parsed.intent !== 'grouped_top_n_by_year') return rows;

    const topN = parsed.topN || 10;
    const direction = parsed.topDirection || 'TOP';
    const metricKeys = ['Total Profit', 'Total Sales', 'Total Quantity', 'Total Discount', 'Profit', 'Sales', 'Quantity', 'Discount'];
    const metricKey = metricKeys.find((key) => rows[0] && Object.prototype.hasOwnProperty.call(rows[0], key))
        || Object.keys(rows[0] || {}).find((key) => /total|profit|sales|quantity|discount/i.test(key));
    const yearKey = Object.keys(rows[0] || {}).find((key) => /order year|year/i.test(key)) || 'Order Year';
    const groupKey = Object.keys(rows[0] || {}).find((key) => key !== yearKey && key !== metricKey && key !== 'Product Name');
    const entityKey = Object.keys(rows[0] || {}).find((key) => key !== yearKey && key !== groupKey && key !== metricKey) || 'Product Name';

    if (!metricKey || !yearKey || !groupKey || !entityKey) return rows;

    const grouped = new Map();
    for (const row of rows) {
        const key = `${row[yearKey]}__${row[groupKey]}`;
        if (!grouped.has(key)) grouped.set(key, []);
        grouped.get(key).push(row);
    }

    const output = [];
    for (const [key, groupRows] of grouped.entries()) {
        const sorted = [...groupRows].sort((a, b) => {
            const va = Number(a[metricKey]) || 0;
            const vb = Number(b[metricKey]) || 0;
            return direction === 'BOTTOM' ? va - vb : vb - va;
        });
        const [yearValue, groupValue] = key.split('__');
        sorted.slice(0, topN).forEach((row, index) => {
            output.push({
                [yearKey]: yearValue,
                [groupKey]: groupValue,
                Rank: index + 1,
                [entityKey]: row[entityKey],
                [metricKey]: row[metricKey]
            });
        });
    }

    output.sort((a, b) => {
        if (String(a[yearKey]) !== String(b[yearKey])) return String(a[yearKey]).localeCompare(String(b[yearKey]));
        if (String(a[groupKey]) !== String(b[groupKey])) return String(a[groupKey]).localeCompare(String(b[groupKey]));
        return Number(a.Rank) - Number(b.Rank);
    });
    return output;
}

// 5. 审计日志模块
function buildAuditEntry(call, mcpResult, parsedResponse) {
    const toolName = call.name;
    const args = call.args || {};
    const query = args.query || {};
    const luid = args.datasourceLuid || args.data_source_id || args.luid || null;
    const filters = query.filters || args.filters || null;
    const fields = query.fields || args.fields || null;

    const isError = mcpResult.isError;
    const rawText = (mcpResult.content || []).filter(c => c.type === "text").map(c => c.text).join("\n");

    let rowCount = "N/A";
    if (parsedResponse.data && Array.isArray(parsedResponse.data)) rowCount = parsedResponse.data.length;
    else if (parsedResponse.result && typeof parsedResponse.result === "string") rowCount = "文本";
    else rowCount = "对象";

    let html = `<div style="margin-bottom:10px;border-bottom:1px dashed #e5e7eb;padding-bottom:10px;">`;
    html += `<div style="color:#2563eb;font-weight:600;margin-bottom:4px;">工具 ${toolName}</div>`;

    if (luid) {
        html += `<div style="margin-left:8px;"><span style="color:#64748b;">LUID:</span> <code style="background:#eef2ff;padding:2px 5px;border-radius:3px;font-size:12px;">${luid}</code></div>`;
    }
    if (fields && Array.isArray(fields)) {
        const fieldSummary = fields.map(f => {
            let label = f.fieldCaption || f;
            if (f.function) label = `${f.function}(${label})`;
            if (f.fieldAlias) label += ` as "${f.fieldAlias}"`;
            return label;
        }).join(', ');
        html += `<div style="margin-left:8px;"><span style="color:#64748b;">字段:</span> ${fieldSummary}</div>`;
    }
    if (filters && Array.isArray(filters) && filters.length > 0) {
        const filterSummary = filters.map(f => {
            const fname = f.field?.fieldCaption || '?';
            const ftype = f.filterType || '?';
            let detail = `${fname} [${ftype}]`;
            if (f.values) detail += ` -> ${JSON.stringify(f.values)}`;
            if (f.minDate || f.maxDate) detail += ` -> ${f.minDate || ''} ~ ${f.maxDate || ''}`;
            if (f.min !== undefined || f.max !== undefined) detail += ` -> ${f.min ?? ''} ~ ${f.max ?? ''}`;
            if (f.periodType) detail += ` -> ${f.dateRangeType} ${f.periodType}`;
            return detail;
        }).join('<br/>');
        html += `<div style="margin-left:8px;"><span style="color:#64748b;">过滤条件:</span><br/><code style="background:#fef3c7;color:#92400e;padding:3px 6px;border-radius:3px;font-size:12px;display:inline-block;margin-top:2px;">${filterSummary}</code></div>`;
    } else if (toolName.includes('query')) {
        html += `<div style="margin-left:8px;"><span style="color:#e53e3e;font-weight:bold;">无过滤条件（全量拉取）</span></div>`;
    }

    const uid = `mcp-${Date.now()}-${Math.random().toString(36).slice(2,7)}`;

    if (isError) {
        html += `<div style="margin-left:8px;color:#dc2626;">报错: ${rawText.substring(0, 200)}</div>`;
    } else if (toolName.includes('query')) {
        const color = rowCount === "N/A" ? "#dc2626" : "#047857";
        html += `<div style="margin-left:8px;"><span style="color:#64748b;">返回:</span> <strong style="color:${color};">${rowCount} 行数据</strong>`;
        const data = parsedResponse.data;
        if (Array.isArray(data) && data.length > 0) {
            const headers = Object.keys(data[0]);
            html += ` <span onclick="(function(el){el.style.display=el.style.display==='none'?'block':'none'})(document.getElementById('${uid}'))"
                style="cursor:pointer;color:#4299e1;font-size:11px;margin-left:6px;">[展开/收起]</span>`;
            html += `<div id="${uid}" style="display:none;margin-top:6px;overflow-x:auto;max-height:200px;overflow-y:auto;border:1px solid #e2e8f0;border-radius:4px;">`;
            html += `<table style="width:100%;border-collapse:collapse;font-size:11px;">`;
            html += `<thead><tr style="background:#f8fafc;position:sticky;top:0;">`;
            headers.forEach(h => {
                html += `<th style="padding:5px 8px;border-bottom:2px solid #e2e8f0;color:#4a5568;font-weight:600;text-align:left;white-space:nowrap;">${h}</th>`;
            });
            html += `</tr></thead><tbody>`;
            data.forEach((row, i) => {
                const bg = i % 2 === 0 ? '#ffffff' : '#f8fafc';
                html += `<tr style="background:${bg};">`;
                headers.forEach(h => {
                    const val = row[h] ?? '';
                    html += `<td style="padding:4px 8px;border-bottom:1px solid #edf2f7;white-space:nowrap;max-width:180px;overflow:hidden;text-overflow:ellipsis;" title="${String(val).replace(/"/g,"'")}">${val}</td>`;
                });
                html += `</tr>`;
            });
            html += `</tbody></table></div>`;
        }
        html += `</div>`;
    } else if (toolName.includes('list')) {
        html += `<div style="margin-left:8px;"><span style="color:#64748b;">找到:</span> <strong style="color:#047857;">${rowCount} 个数据源</strong>`;
        const sources = parsedResponse.data || parsedResponse.datasources || [];
        const sourceList = Array.isArray(sources) ? sources : (Array.isArray(parsedResponse) ? parsedResponse : []);
        if (sourceList.length > 0) {
            html += ` <span onclick="(function(el){el.style.display=el.style.display==='none'?'block':'none'})(document.getElementById('${uid}'))"
                style="cursor:pointer;color:#4299e1;font-size:11px;margin-left:6px;">[展开查看数据源列表]</span>`;
            html += `<div id="${uid}" style="display:none;margin-top:6px;overflow-x:auto;max-height:200px;overflow-y:auto;border:1px solid #e2e8f0;border-radius:4px;">`;
            html += `<table style="width:100%;border-collapse:collapse;font-size:11px;">`;
            html += `<thead><tr style="background:#f8fafc;"><th style="padding:5px 8px;border-bottom:2px solid #e2e8f0;color:#4a5568;font-weight:600;text-align:left;">数据源名称</th><th style="padding:5px 8px;border-bottom:2px solid #e2e8f0;color:#4a5568;font-weight:600;text-align:left;">LUID</th></tr></thead><tbody>`;
            sourceList.forEach((s, i) => {
                const name = s.name || s.contentUrl || s.datasourceName || JSON.stringify(s).slice(0, 40);
                const luidValue = s.luid || s.id || s.datasourceLuid || '-';
                const bg = i % 2 === 0 ? '#ffffff' : '#f8fafc';
                html += `<tr style="background:${bg};"><td style="padding:4px 8px;border-bottom:1px solid #edf2f7;white-space:nowrap;">${name}</td><td style="padding:4px 8px;border-bottom:1px solid #edf2f7;font-family:monospace;font-size:10px;color:#805ad5;">${luidValue}</td></tr>`;
            });
            html += `</tbody></table></div>`;
        }
        html += `</div>`;
    } else if (toolName.includes('metadata')) {
        html += `<div style="margin-left:8px;"><span style="color:#64748b;">状态:</span> <strong style="color:#047857;">已获取字段元数据</strong></div>`;
    } else {
        html += `<div style="margin-left:8px;"><span style="color:#64748b;">返回:</span> ${rowCount}</div>`;
    }

    html += `</div>`;
    return { html, rowCount };
}

// 6. 单次 MCP 工具调用
function interceptTopFilter(args) {
    if (args.name !== 'query-datasource') return { intercepted: false };

    const filters = args.args?.query?.filters || [];
    const topFilter = filters.find(f => f.filterType === 'TOP');
    const otherFilters = filters.filter(f => f.filterType !== 'TOP');

    if (!topFilter || otherFilters.length === 0) return { intercepted: false };

    const cleanedArgs = JSON.parse(JSON.stringify(args));
    cleanedArgs.args.query.filters = otherFilters;

    const topN = topFilter.howMany || 5;
    const topByField = topFilter.fieldToMeasure?.fieldCaption;
    const topByFunc = topFilter.fieldToMeasure?.function || 'SUM';

    console.log(`TOP filter 拦截: 摘除 TOP filter，查全量后代码取前 ${topN} 名（按 ${topByFunc}(${topByField})）`);
    return { intercepted: true, cleanedArgs, topN, topByField, topByFunc };
}

function applyTopN(data, topN, topByField, topByFunc) {
    if (!data || !Array.isArray(data) || !topByField) return data;

    const keys = Object.keys(data[0] || {});
    const sortKey = keys.find(k =>
        k === topByField ||
        k.toLowerCase().includes(topByField.toLowerCase()) ||
        k.toLowerCase().includes(topByFunc.toLowerCase())
    ) || topByField;

    const sorted = [...data].sort((a, b) => {
        const va = parseFloat(String(a[sortKey]).replace(/,/g, '')) || 0;
        const vb = parseFloat(String(b[sortKey]).replace(/,/g, '')) || 0;
        return vb - va;
    });

    console.log(`代码取前 ${topN} 名（共 ${data.length} 行 -> ${Math.min(topN, data.length)} 行）`);
    return sorted.slice(0, topN);
}

async function executeMcpCall(call) {
    const normalizedCall = normalizeQueryDatasourceCall(call);
    const normalizedArgs = normalizedCall.args || normalizedCall.arguments;
    const vdsDiagnostics = Array.isArray(normalizedArgs?._vdsDiagnostics) ? normalizedArgs._vdsDiagnostics : [];
    if (normalizedArgs && normalizedArgs._vdsDiagnostics) {
        delete normalizedArgs._vdsDiagnostics;
    }
    for (const line of vdsDiagnostics) {
        console.log(`VDS normalize: ${line}`);
    }
    const intercept = interceptTopFilter(normalizedCall);
    const actualArgs = intercept.intercepted ? intercept.cleanedArgs : normalizedCall;
    const actualToolArgs = actualArgs.arguments ?? actualArgs.args;

    const mcpResult = await mcpClient.callTool({ name: actualArgs.name, arguments: actualToolArgs });

    const rawText = (mcpResult.content || [])
        .filter(c => c.type === "text")
        .map(c => c.text)
        .join("\n");

    let parsedResponse;
    try {
        const parsed = JSON.parse(rawText);
        let data = Array.isArray(parsed) ? parsed : (parsed?.data ?? parsed);

        if (intercept.intercepted && Array.isArray(data)) {
            data = applyTopN(data, intercept.topN, intercept.topByField, intercept.topByFunc);
        }

        if (Array.isArray(data)) parsedResponse = { data };
        else if (data !== null && typeof data === 'object') parsedResponse = data;
        else parsedResponse = { result: data };
    } catch {
        parsedResponse = { result: rawText };
    }

    return { mcpResult, parsedResponse };
}

async function listDatasources() {
    const mcpResult = await mcpClient.callTool({ name: 'list-datasources', arguments: {} });
    const rawText = (mcpResult.content || [])
        .filter(c => c.type === "text")
        .map(c => c.text)
        .join("\n");

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

function extractTaggedData(text) {
    const rawText = String(text || '');
    const dataMatch = rawText.match(/\[DATA\]([\s\S]*?)\[\/DATA\]/i);
    let plainText = rawText.replace(/\[DATA\][\s\S]*?\[\/DATA\]/i, '').trim();
    let data = [];

    if (dataMatch) {
        const rawJson = dataMatch[1].replace(/```json/gi, '').replace(/```/g, '').trim();
        try {
            data = JSON.parse(rawJson);
        } catch {
            data = [];
        }
    }

    return { plainText, data };
}

function stripHtmlTags(text) {
    return String(text || '').replace(/<[^>]+>/g, '').trim();
}

function formatNestedValue(value) {
    if (value === null || value === undefined) return '';
    if (Array.isArray(value)) {
        const objectArray = value.every(item => item && typeof item === 'object' && !Array.isArray(item));
        if (objectArray) {
            const compact = value.map((item) => {
                const productName = item['Product Name'] || item['Product'] || item['productName'] || item['name'];
                const profit = item['Total Profit'] ?? item['Profit'] ?? item['profit'];
                if (productName !== undefined && profit !== undefined) {
                    return `${productName} (${profit})`;
                }
                return Object.entries(item)
                    .map(([key, val]) => `${key}: ${formatNestedValue(val)}`)
                    .join(', ');
            });
            return compact.join(' | ');
        }
        return value.map(item => formatNestedValue(item)).join(' | ');
    }
    if (typeof value === 'object') {
        return Object.entries(value)
            .map(([key, val]) => `${key}: ${formatNestedValue(val)}`)
            .join(', ');
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

function enrichRowsWithFilterContext(rows, filters = []) {
    if (!Array.isArray(rows) || rows.length === 0) return rows;

    const context = {};
    for (const filter of filters) {
        const fieldCaption = filter?.field?.fieldCaption;
        if (!fieldCaption) continue;

        if (fieldCaption === 'Segment' && Array.isArray(filter.values) && filter.values.length === 1) {
            context.Segment = filter.values[0];
        }

        if (fieldCaption === 'Order Date' && filter.minDate && filter.maxDate) {
            const minYear = String(filter.minDate).slice(0, 4);
            const maxYear = String(filter.maxDate).slice(0, 4);
            if (minYear === maxYear) {
                context['Order Year'] = minYear;
            }
        }
    }

    if (Object.keys(context).length === 0) return rows;
    return rows.map((row) => ({ ...context, ...row }));
}

function stableRowSignature(row) {
    if (!row || typeof row !== 'object' || Array.isArray(row)) return JSON.stringify(row);
    const normalized = {};
    for (const key of Object.keys(row).sort()) {
        normalized[key] = row[key];
    }
    return JSON.stringify(normalized);
}

function dedupeRows(rows) {
    if (!Array.isArray(rows) || rows.length === 0) return [];
    const seen = new Set();
    const result = [];
    for (const row of rows) {
        const sig = stableRowSignature(row);
        if (seen.has(sig)) continue;
        seen.add(sig);
        result.push(row);
    }
    return result;
}

// 7. 核心路由
app.post('/api/analyze-datasource', async (req, res) => {
    try {
        const { prompt, dashboardMeta = [] } = req.body;
        console.log(`收到请求, prompt长度: ${prompt?.length}, dashboardMeta: ${dashboardMeta?.length} 个数据源`);

        if (!mcpClient) {
            return res.status(503).json({ error: "MCP 服务尚未就绪，请稍后重试。" });
        }

        const { tools: mcpTools } = await mcpClient.listTools();
        const geminiTools = convertToGeminiTools(mcpTools);
        console.log(`MCP 工具数量: ${mcpTools.length}, 转换后: ${geminiTools[0]?.functionDeclarations?.length}`);
        const chat = model.startChat({ tools: geminiTools });

        let finalPrompt = prompt;
        if (dashboardMeta.length > 0) {
            const dashboardDatasources = resolveDashboardDatasourceContext(dashboardMeta);
            const mcpDatasources = await listDatasources();
            const datasourceMatches = pickBestDatasourceMatches(dashboardDatasources, mcpDatasources);
            const primaryDatasourceLuid = datasourceMatches.find((item) => item.best?.luid)?.best?.luid;

            if (primaryDatasourceLuid) {
                const deterministicPlan = await runDeterministicDslQuery(prompt, primaryDatasourceLuid);
                if (deterministicPlan?.type === 'yoy_analysis') {
                    console.log('命中 deterministic YOY 路径');
                    const auditHtml = deterministicPlan.queryResults
                        .map(({ call, mcpResult, parsedResponse }) => buildAuditEntry(call, mcpResult, parsedResponse).html)
                        .join('');
                    const finalData = normalizeFinalData(deterministicPlan.analysisRows || []);
                    return res.json({
                        status: 'success',
                        response: auditHtml,
                        plainMessage: deterministicPlan.plainMessage || '已通过 deterministic YOY 分析返回结果。',
                        data: finalData,
                        auditHtml
                    });
                }
                if (deterministicPlan?.query) {
                    console.log('命中 deterministic DSL 路径');
                    const deterministicCall = {
                        name: 'query-datasource',
                        args: {
                            datasourceLuid: primaryDatasourceLuid,
                            query: deterministicPlan.query
                        }
                    };
                    const { mcpResult, parsedResponse } = await executeMcpCall(deterministicCall);
                    const { html } = buildAuditEntry(deterministicCall, mcpResult, parsedResponse);
                    const deterministicRows = applyGroupedTopN(parsedResponse.data || [], deterministicPlan.parsed);
                    const finalData = normalizeFinalData(deterministicRows);
                    return res.json({
                        status: 'success',
                        response: html,
                        plainMessage: '已通过 deterministic query 返回结果。',
                        data: finalData,
                        auditHtml: html
                    });
                }
            }

            const datasourceContextText = datasourceMatches.map((item) => {
                const requested = item.dashboardDatasource.datasourceName;
                const worksheets = item.dashboardDatasource.worksheetNames.join(', ') || 'N/A';
                const best = item.best
                    ? `${item.best.matchedName} (${item.best.luid}) score=${item.best.score.toFixed(3)}`
                    : '未匹配到候选';
                const alternatives = item.alternatives.length
                    ? item.alternatives
                        .filter(candidate => candidate.score >= 0.45)
                        .map(candidate => `${candidate.matchedName} (${candidate.luid}) score=${candidate.score.toFixed(3)}`)
                        .join(' | ')
                    : '无';
                return `- Dashboard datasource: ${requested}
  Worksheets: ${worksheets}
  Best MCP match: ${best}
  Alternatives: ${alternatives}`;
            }).join('\n');

            finalPrompt = `【当前 Dashboard 正在使用的数据源及建议匹配】
${datasourceContextText}

要求：
1. 必须先调用 list-datasources
2. 优先选择上面 Best MCP match 中对应的数据源
3. 只有当 Best MCP match 明显错误时，才考虑 alternatives
4. 选定真实 datasourceLuid 后，再调用 get-datasource-metadata 和 query-datasource

${prompt}`;
        }

        console.log('发送主查询...');
        let result = await chat.sendMessage(finalPrompt);
        let response = result.response;
        const candidates = response.candidates || [];
        const finishReason = candidates[0]?.finishReason || 'UNKNOWN';
        console.log(`主查询响应: functionCalls=${response.functionCalls()?.length ?? 0}, text长度=${response.text()?.length ?? 0}, finishReason=${finishReason}`);

        let auditHtml = `<div style="background:#fdf6e3;padding:14px;border-left:4px solid #d97706;margin-bottom:15px;border-radius:4px;font-size:13px;"><div style="font-weight:bold;color:#92400e;margin-bottom:10px;">MCP 调用记录</div>`;
        let toolCalled = false;
        let totalSkipped = 0;
        let iteration = 0;
        let lastQueryData = null;
        let collectedQueryData = [];

        while (iteration < 15) {
            iteration++;
            const calls = response.functionCalls() || [];
            console.log(`循环第 ${iteration} 轮 calls=${calls.length}, text长度=${response.text()?.length ?? 0}`);

            if (calls.length > 0) {
                const toolResults = [];
                const NO_PARAM_TOOLS = ["list-datasources"];
                const validCalls = calls.filter(c => NO_PARAM_TOOLS.includes(c.name) || (c.args && Object.keys(c.args).length > 0));
                const emptyCalls = calls.filter(c => !NO_PARAM_TOOLS.includes(c.name) && (!c.args || Object.keys(c.args).length === 0));

                for (const call of emptyCalls) {
                    console.log(`跳过空参数调用 ${call.name}`);
                    auditHtml += `<div style="margin-bottom:10px;border-bottom:1px dashed #e5e7eb;padding-bottom:10px;"><div style="color:#9ca3af;font-weight:600;">跳过 ${call.name} <span style="font-weight:400;">(空参数，已跳过)</span></div></div>`;
                    toolResults.push({
                        functionResponse: {
                            name: call.name,
                            response: { error: "参数不能为空，请提供完整参数后重新调用。" }
                        }
                    });
                }
                totalSkipped += emptyCalls.length;

                for (const call of validCalls) {
                    toolCalled = true;
                    console.log(`调用工具: ${call.name}`, JSON.stringify(call.args, null, 2));
                    const { mcpResult, parsedResponse } = await executeMcpCall(call);
                    const { html, rowCount } = buildAuditEntry(call, mcpResult, parsedResponse);
                    auditHtml += html;
                    console.log(`${call.name} 返回: ${rowCount}`);
                    toolResults.push({ functionResponse: { name: call.name, response: parsedResponse } });

                    if (call.name === 'query-datasource' && parsedResponse.data) {
                        lastQueryData = parsedResponse.data;
                        collectedQueryData.push(...enrichRowsWithFilterContext(parsedResponse.data, call.args?.query?.filters || []));
                    }
                }

                result = await chat.sendMessage(toolResults);
                response = result.response;
            } else {
                const text = response.text() || '';

                if (text.includes('[DATA]') || (text.trim().length > 0 && calls.length === 0) || !toolCalled) {
                    console.log(`任务完成，退出循环（第 ${iteration} 轮）`);
                    break;
                }

                let resumeMsg = '请继续执行剩余的查询步骤，完成后将完整结果用 [DATA][/DATA] 标签输出。';
                if (lastQueryData && Array.isArray(lastQueryData)) {
                    resumeMsg = `上一步查询返回了以下数据：${JSON.stringify(lastQueryData)}

请基于这些数据继续执行后续查询步骤，完成后将完整结果用 [DATA][/DATA] 标签输出。`;
                }
                console.log(`Gemini 过渡状态，推动继续...（第 ${iteration} 轮）`);
                result = await chat.sendMessage(resumeMsg);
                response = result.response;
            }
        }

        if (toolCalled) {
            const skippedNote = totalSkipped > 0 ? `，跳过 ${totalSkipped} 次空调用` : '';
            auditHtml += `<div style="margin-top:8px;padding-top:8px;border-top:1px solid #e5e7eb;color:#78716c;font-size:12px;">共 ${iteration} 轮工具调用${skippedNote}</div></div>\n\n`;
        } else {
            auditHtml = "";
        }

        let finalText = response.text() || '';
        if (!finalText.trim() && collectedQueryData.length > 0) {
            finalText = `已完成查询。由于模型没有输出最终总结，下面直接提供查询结果。`;
        }

        const tagged = extractTaggedData(finalText);
        if (!tagged.data.length && /\[DATA\]/i.test(finalText)) {
            console.log('tagged DATA parse failed; falling back to collectedQueryData');
        }
        const finalData = normalizeFinalData(tagged.data.length > 0 ? tagged.data : collectedQueryData);
        const plainMessage = stripHtmlTags(tagged.plainText || finalText || '已完成查询，请查看结果表。');

        console.log(`最终返回 text长度=${finalText?.length ?? 0}, finalData=${finalData.length}`);
        res.json({
            status: "success",
            response: auditHtml + finalText,
            plainMessage,
            data: finalData,
            auditHtml
        });

    } catch (error) {
        console.error("API 报错:", error);
        res.status(500).json({ error: error.message });
    }
});

// 8. 启动
const PORT = process.env.PORT || 8080;
app.listen(PORT, '0.0.0.0', async () => {
    console.log(`服务就绪: http://localhost:${PORT}`);
    await startMcpServer();
});
