function normalizeText(value) {
    return String(value ?? '')
        .toLowerCase()
        .replace(/[_-]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
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
    if (text.length === 2 && text.startsWith('十') && map[text[1]] !== undefined) return 10 + map[text[1]];
    if (text.length === 2 && text.endsWith('十') && map[text[0]] !== undefined) return map[text[0]] * 10;
    if (text.length === 3 && text[1] === '十' && map[text[0]] !== undefined && map[text[2]] !== undefined) {
        return map[text[0]] * 10 + map[text[2]];
    }

    return null;
}

function detectMetric(text) {
    if (/利润/.test(text)) return '利润';
    if (/销售额|销售/.test(text)) return '销售额';
    if (/销量|数量/.test(text)) return '销量';
    if (/折扣/.test(text)) return '折扣';
    return null;
}

function detectTimeScope(text) {
    const yearMatch = text.match(/(20\d{2})\s*年/);
    if (yearMatch) return { mode: 'year', year: yearMatch[1] };
    if (/(每年|所有年份|全部年份)/.test(text)) return { mode: 'all_years' };
    return null;
}

export function extractQuerySlots(prompt) {
    const text = String(prompt || '').trim();
    const normalized = normalizeText(text);
    const timeScope = detectTimeScope(text);
    const metric = detectMetric(text);

    if (!metric) return null;

    const yoyMemberPattern = /(.+?)\s*(20\d{2})\s*年\s*的\s*(利润|销售额|销售|销量|数量|折扣).*(比去年|同比)/i;
    const yoyMemberMatch = text.match(yoyMemberPattern);
    if (yoyMemberMatch) {
        return {
            intent: 'compare_yoy_member_metric',
            timeScope: { mode: 'year', year: yoyMemberMatch[2] },
            memberFilters: [{ value: yoyMemberMatch[1].trim() }],
            metric,
            comparison: { baseline: 'previous_year' },
            rawPrompt: text
        };
    }

    const groupedTopPattern = /(?:每年|所有年份|全部年份)\s*每个\s*(.+?)\s*(利润|销售额|销售|销量|数量|折扣)\s*(前|后)\s*([0-9一二三四五六七八九十两]+)\s*名?\s*的\s*(.+)/i;
    const groupedTopMatch = text.match(groupedTopPattern);
    if (groupedTopMatch) {
        return {
            intent: 'grouped_top_n_by_year',
            timeScope: { mode: 'all_years' },
            groupBy: groupedTopMatch[1].trim(),
            metric,
            ranking: {
                direction: groupedTopMatch[3] === '后' ? 'BOTTOM' : 'TOP',
                count: chineseNumeralToInt(groupedTopMatch[4]) || 10
            },
            entity: groupedTopMatch[5].trim(),
            rawPrompt: text
        };
    }

    const memberTopPattern = /(20\d{2})\s*年\s*(.+?)\s*(利润|销售额|销售|销量|数量|折扣)\s*(前|后)\s*([0-9一二三四五六七八九十两]+)\s*名?\s*的\s*(.+)/i;
    const memberTopMatch = text.match(memberTopPattern);
    if (memberTopMatch) {
        return {
            intent: 'top_n_member_metric_entity',
            timeScope: { mode: 'year', year: memberTopMatch[1] },
            memberFilters: [{ value: memberTopMatch[2].trim() }],
            metric,
            ranking: {
                direction: memberTopMatch[4] === '后' ? 'BOTTOM' : 'TOP',
                count: chineseNumeralToInt(memberTopMatch[5]) || 5
            },
            entity: memberTopMatch[6].trim(),
            rawPrompt: text
        };
    }

    const groupPattern = /(20\d{2})\s*年\s*每个\s*(.+?)\s*的\s*(利润|销售额|销售|销量|数量|折扣)/i;
    const groupMatch = text.match(groupPattern);
    if (groupMatch) {
        return {
            intent: 'group_metric_by_year',
            timeScope: { mode: 'year', year: groupMatch[1] },
            groupBy: groupMatch[2].trim(),
            metric,
            rawPrompt: text
        };
    }

    const memberPattern = /(20\d{2})\s*年\s*(.+?)\s*的\s*(利润|销售额|销售|销量|数量|折扣)/i;
    const memberMatch = text.match(memberPattern);
    if (memberMatch && !normalized.includes('每个')) {
        return {
            intent: 'member_metric_by_year',
            timeScope: { mode: 'year', year: memberMatch[1] },
            memberFilters: [{ value: memberMatch[2].trim() }],
            metric,
            rawPrompt: text
        };
    }

    if (timeScope && metric) {
        return {
            intent: 'aggregate',
            timeScope,
            metric,
            rawPrompt: text
        };
    }

    return null;
}
