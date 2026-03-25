function normalizeText(value) {
    return String(value ?? '')
        .toLowerCase()
        .replace(/[_-]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function getFieldCaption(record) {
    return record?.fieldCaption || record?.caption || record?.fieldName || record?.name || null;
}

function prepareRecords(metadataRecords = []) {
    return (Array.isArray(metadataRecords) ? metadataRecords : [])
        .map((record) => ({ record, caption: getFieldCaption(record) }))
        .filter((item) => item.caption);
}

function findFieldByCandidates(metadataRecords, candidates = []) {
    const prepared = prepareRecords(metadataRecords);

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

function resolveMetricField(metadataRecords, metricLabel) {
    const text = String(metricLabel || '');
    if (/利润/.test(text)) return findFieldByCandidates(metadataRecords, ['Profit', '利润']);
    if (/销售额|销售/.test(text)) return findFieldByCandidates(metadataRecords, ['Sales', '销售额', '销售']);
    if (/销量|数量/.test(text)) return findFieldByCandidates(metadataRecords, ['Quantity', '销量', '数量']);
    if (/折扣/.test(text)) return findFieldByCandidates(metadataRecords, ['Discount', '折扣']);
    return null;
}

function normalizeMemberValue(value) {
    const text = String(value || '').trim();
    const normalized = normalizeText(text);

    const memberMap = new Map([
        ['consumer', 'Consumer'],
        ['消费者', 'Consumer'],
        ['corporate', 'Corporate'],
        ['公司', 'Corporate'],
        ['home office', 'Home Office'],
        ['homeoffice', 'Home Office'],
        ['家庭办公室', 'Home Office'],
        ['east', 'East'],
        ['东部', 'East'],
        ['东部地区', 'East'],
        ['west', 'West'],
        ['西部', 'West'],
        ['西部地区', 'West'],
        ['central', 'Central'],
        ['中部', 'Central'],
        ['中部地区', 'Central'],
        ['south', 'South'],
        ['南部', 'South'],
        ['南部地区', 'South'],
        ['technology', 'Technology'],
        ['科技', 'Technology'],
        ['office supplies', 'Office Supplies'],
        ['officesupplies', 'Office Supplies'],
        ['办公用品', 'Office Supplies'],
        ['furniture', 'Furniture'],
        ['家具', 'Furniture']
    ]);

    return memberMap.get(normalized) || text;
}

export function resolveFieldBySemanticLabel(metadataRecords, label, fallbackCandidates = []) {
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

function resolveMemberFilter(metadataRecords, rawValue) {
    if (!rawValue) return null;
    const value = normalizeMemberValue(rawValue);

    if (/consumer|corporate|home office|消费者|公司|家庭办公室/i.test(rawValue)) {
        const field = findFieldByCandidates(metadataRecords, ['Segment', '客户细分']);
        if (field) return { field, value };
    }

    if (/east|west|central|south|东部|西部|中部|南部|地区/i.test(rawValue)) {
        const field = findFieldByCandidates(metadataRecords, ['Region', '地区', '区域']);
        if (field) return { field, value };
    }

    if (/technology|office supplies|furniture|科技|办公用品|家具/i.test(rawValue)) {
        const field = findFieldByCandidates(metadataRecords, ['Category', '类别', '品类']);
        if (field) return { field, value };
    }

    return null;
}

export function resolveFieldRoles(metadataRecords, slots = {}) {
    const timeField = findFieldByCandidates(metadataRecords, ['Order Date', '日期', 'Date']);
    const metricField = resolveMetricField(metadataRecords, slots.metric);
    const groupField = slots.groupBy ? resolveFieldBySemanticLabel(metadataRecords, slots.groupBy, [slots.groupBy]) : null;
    const entityField = slots.entity ? resolveFieldBySemanticLabel(metadataRecords, slots.entity, [slots.entity]) : null;
    const memberFilter = resolveMemberFilter(metadataRecords, slots.memberFilters?.[0]?.value);

    return {
        timeField,
        metricField,
        groupField,
        entityField,
        memberFilter
    };
}

export function getResolvedFieldCaption(record) {
    return getFieldCaption(record);
}
