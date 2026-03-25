function pick(obj, keys) {
    const out = {};
    for (const key of keys) {
        if (obj[key] !== undefined) out[key] = obj[key];
    }
    return out;
}

const QUERY_KEYS = ['fields', 'filters', 'parameters'];
const FIELD_KEYS = ['fieldCaption', 'fieldAlias', 'maxDecimalPlaces', 'sortDirection', 'sortPriority', 'function', 'calculation', 'binSize', 'tableCalculation', 'nestedTableCalculations'];
const PARAMETER_KEYS = ['parameterCaption', 'parameterName', 'parameterType', 'dataType', 'value'];
const SORT_DIRECTIONS = new Set(['ASC', 'DESC']);
const FILTER_TYPES = new Set(['QUANTITATIVE_DATE', 'QUANTITATIVE_NUMERICAL', 'SET', 'MATCH', 'CONDITION', 'DATE', 'TOP']);
const PERIOD_TYPES = new Set(['MINUTES', 'HOURS', 'DAYS', 'WEEKS', 'MONTHS', 'QUARTERS', 'YEARS']);
const DATE_RANGE_TYPES = new Set(['CURRENT', 'LAST', 'LASTN', 'NEXT', 'NEXTN', 'TODATE']);
const PARAMETER_TYPES = new Set(['ANY_VALUE', 'LIST', 'QUANTITATIVE_RANGE', 'QUANTITATIVE_DATE']);
const DATA_TYPES = new Set(['INTEGER', 'REAL', 'STRING', 'DATETIME', 'BOOLEAN', 'DATE', 'SPATIAL', 'UNKNOWN']);
const TABLEAU_FUNCTIONS = new Set([
    'SUM', 'AVG', 'MEDIAN', 'COUNT', 'COUNTD', 'MIN', 'MAX', 'STDEV', 'VAR', 'COLLECT',
    'YEAR', 'QUARTER', 'MONTH', 'WEEK', 'DAY',
    'TRUNC_YEAR', 'TRUNC_QUARTER', 'TRUNC_MONTH', 'TRUNC_WEEK', 'TRUNC_DAY',
    'AGG', 'NONE', 'UNSPECIFIED'
]);

function normalizeEnum(value, allowed) {
    if (value === undefined || value === null) return value;
    const normalized = String(value).toUpperCase();
    return allowed.has(normalized) ? normalized : value;
}

function trimDateString(value) {
    const raw = String(value ?? '').trim();
    if (!raw) return raw;
    const match = raw.match(/^(\d{4}-\d{2}-\d{2})/);
    return match ? match[1] : raw;
}

function normalizeFieldRef(field) {
    if (!field || typeof field !== 'object') return field;
    const normalized = pick(field, ['fieldCaption', 'fieldAlias', 'function', 'calculation']);
    if (normalized.function) normalized.function = normalizeEnum(normalized.function, TABLEAU_FUNCTIONS);
    return normalized;
}

export function buildDimensionField(fieldCaption, extras = {}) {
    return { fieldCaption, ...extras };
}

export function buildMeasureField(fieldCaption, fn = 'SUM', extras = {}) {
    return { fieldCaption, function: fn, ...extras };
}

export function buildCalculatedField(fieldCaption, calculation, extras = {}) {
    return { fieldCaption, calculation, ...extras };
}

export function buildSetFilter(fieldCaption, values, extras = {}) {
    return {
        field: { fieldCaption },
        filterType: 'SET',
        values,
        exclude: false,
        ...extras
    };
}

export function buildQuantitativeDateRangeFilter(fieldCaption, yearOrMinDate, maxDate, extras = {}) {
    if (maxDate === undefined && /^\d{4}$/.test(String(yearOrMinDate ?? ''))) {
        const year = String(yearOrMinDate);
        return {
            field: { fieldCaption },
            filterType: 'QUANTITATIVE_DATE',
            quantitativeFilterType: 'RANGE',
            minDate: `${year}-01-01`,
            maxDate: `${year}-12-31`,
            ...extras
        };
    }

    return {
        field: { fieldCaption },
        filterType: 'QUANTITATIVE_DATE',
        quantitativeFilterType: 'RANGE',
        minDate: trimDateString(yearOrMinDate),
        maxDate: trimDateString(maxDate),
        ...extras
    };
}

export function buildTopNFilter(fieldCaption, measureFieldCaption, howMany = 5, measureFn = 'SUM', extras = {}) {
    return {
        field: { fieldCaption },
        filterType: 'TOP',
        direction: 'TOP',
        howMany,
        fieldToMeasure: { fieldCaption: measureFieldCaption, function: measureFn },
        ...extras
    };
}

export function normalizeVdsField(field) {
    if (!field || typeof field !== 'object') return field;

    const base = pick(field, FIELD_KEYS);
    if (base.function) base.function = normalizeEnum(base.function, TABLEAU_FUNCTIONS);
    if (base.sortDirection) base.sortDirection = normalizeEnum(base.sortDirection, SORT_DIRECTIONS);

    if (base.tableCalculation) {
        return pick(base, [
            'fieldCaption',
            'fieldAlias',
            'maxDecimalPlaces',
            'function',
            'calculation',
            'tableCalculation',
            'nestedTableCalculations',
            'sortDirection',
            'sortPriority'
        ]);
    }

    if (base.calculation) {
        delete base.function;
        return pick(base, [
            'fieldCaption',
            'fieldAlias',
            'maxDecimalPlaces',
            'calculation',
            'sortDirection',
            'sortPriority'
        ]);
    }

    if (base.binSize !== undefined) {
        delete base.function;
        return pick(base, [
            'fieldCaption',
            'fieldAlias',
            'maxDecimalPlaces',
            'binSize',
            'sortDirection',
            'sortPriority'
        ]);
    }

    if (base.function) {
        return pick(base, [
            'fieldCaption',
            'fieldAlias',
            'maxDecimalPlaces',
            'function',
            'sortDirection',
            'sortPriority'
        ]);
    }

    return pick(base, [
        'fieldCaption',
        'fieldAlias',
        'maxDecimalPlaces',
        'sortDirection',
        'sortPriority'
    ]);
}

export function normalizeVdsFilter(filter) {
    if (!filter || typeof filter !== 'object') return filter;

    const next = JSON.parse(JSON.stringify(filter));
    if (next.filterType) next.filterType = normalizeEnum(next.filterType, FILTER_TYPES);
    if (next.quantitativeFilterType) next.quantitativeFilterType = String(next.quantitativeFilterType).toUpperCase();
    if (next.direction) next.direction = String(next.direction).toUpperCase();
    if (next.periodType) next.periodType = normalizeEnum(next.periodType, PERIOD_TYPES);
    if (next.dateRangeType) next.dateRangeType = normalizeEnum(next.dateRangeType, DATE_RANGE_TYPES);
    if (next.minDate) next.minDate = trimDateString(next.minDate);
    if (next.maxDate) next.maxDate = trimDateString(next.maxDate);
    if (next.anchorDate) next.anchorDate = trimDateString(next.anchorDate);

    const fieldCaption = next.field?.fieldCaption;
    const fieldFunction = next.field?.function;
    const values = Array.isArray(next.values) ? next.values.map((v) => String(v)) : [];
    const explicitYear = next.year !== undefined && next.year !== null ? String(next.year) : '';
    const minLike = next.minDate ?? next.min;
    const maxLike = next.maxDate ?? next.max;
    const specificDate = next.specificDate ? trimDateString(next.specificDate) : '';
    const looksLikeYearOnly =
        values.length === 1 &&
        (/^\d{4}$/.test(values[0]) || /^\d{4}-\d{2}-\d{2}(T.*)?$/.test(values[0]));
    const looksLikeExplicitYear = /^\d{4}$/.test(explicitYear);
    const looksLikeDateRange =
        typeof minLike === 'string' &&
        typeof maxLike === 'string' &&
        /^\d{4}-\d{2}-\d{2}/.test(minLike) &&
        /^\d{4}-\d{2}-\d{2}/.test(maxLike);
    const looksLikeSpecificYearDate =
        /^\d{4}-\d{2}-\d{2}$/.test(specificDate) &&
        next.filterType === 'DATE' &&
        String(next.dateRangeType || '').toUpperCase() === 'SPECIFIC' &&
        String(next.range || '').toUpperCase() === 'YEAR';

    const isSpecificYearDateFilter =
        fieldCaption === 'Order Date' &&
        next.filterType === 'DATE' &&
        (
            (looksLikeYearOnly && fieldFunction === 'TRUNC_YEAR') ||
            (looksLikeExplicitYear && String(next.dateRangeType || '').toUpperCase().includes('SPECIFIC_YEAR')) ||
            (looksLikeYearOnly && String(next.dateRangeType || '').toUpperCase().includes('SPECIFIC_YEAR')) ||
            looksLikeSpecificYearDate
        );

    if (isSpecificYearDateFilter) {
        const year = looksLikeExplicitYear
            ? explicitYear
            : (looksLikeSpecificYearDate ? specificDate.slice(0, 4) : values[0].slice(0, 4));
        return buildQuantitativeDateRangeFilter('Order Date', year, undefined, pick(next, ['context']));
    }

    const isAbsoluteDateRangeDisguisedAsDateFilter =
        fieldCaption === 'Order Date' &&
        next.filterType === 'DATE' &&
        looksLikeDateRange;

    if (isAbsoluteDateRangeDisguisedAsDateFilter) {
        return buildQuantitativeDateRangeFilter(
            'Order Date',
            trimDateString(minLike),
            trimDateString(maxLike),
            pick(next, ['context'])
        );
    }

    if (next.filterType === 'QUANTITATIVE_DATE') {
        return {
            ...pick(next, ['context']),
            field: normalizeFieldRef(next.field),
            filterType: 'QUANTITATIVE_DATE',
            quantitativeFilterType: next.quantitativeFilterType || 'RANGE',
            minDate: next.minDate,
            maxDate: next.maxDate
        };
    }

    if (next.filterType === 'DATE') {
        return {
            ...pick(next, ['context', 'includeNulls', 'rangeN']),
            field: { fieldCaption: fieldCaption },
            filterType: 'DATE',
            periodType: next.periodType,
            dateRangeType: next.dateRangeType,
            anchorDate: next.anchorDate
        };
    }

    if (next.filterType === 'SET') {
        return {
            ...pick(next, ['context', 'exclude']),
            field: { fieldCaption: fieldCaption },
            filterType: 'SET',
            values: next.values || []
        };
    }

    if (next.filterType === 'TOP') {
        return {
            ...pick(next, ['context']),
            field: { fieldCaption: fieldCaption },
            filterType: 'TOP',
            direction: next.direction || 'TOP',
            howMany: next.howMany,
            fieldToMeasure: normalizeFieldRef(next.fieldToMeasure)
        };
    }

    if (next.filterType === 'QUANTITATIVE_NUMERICAL') {
        return {
            ...pick(next, ['context']),
            field: normalizeFieldRef(next.field),
            filterType: 'QUANTITATIVE_NUMERICAL',
            quantitativeFilterType: next.quantitativeFilterType || 'RANGE',
            min: next.min,
            max: next.max
        };
    }

    return next;
}

export function normalizeVdsParameter(parameter) {
    if (!parameter || typeof parameter !== 'object') return parameter;
    const next = pick(parameter, PARAMETER_KEYS);
    if (next.parameterType) next.parameterType = normalizeEnum(next.parameterType, PARAMETER_TYPES);
    if (next.dataType) next.dataType = normalizeEnum(next.dataType, DATA_TYPES);
    if (next.parameterType === 'QUANTITATIVE_DATE' || next.dataType === 'DATE' || next.dataType === 'DATETIME') {
        next.value = trimDateString(next.value);
    }
    return next;
}

export function normalizeVdsQuery(query) {
    if (!query || typeof query !== 'object') return query;

    const normalized = pick(JSON.parse(JSON.stringify(query)), QUERY_KEYS);
    if (Array.isArray(normalized.fields)) {
        normalized.fields = normalized.fields.map(normalizeVdsField);
    }
    if (Array.isArray(normalized.filters)) {
        normalized.filters = normalized.filters.map(normalizeVdsFilter);
    }
    if (Array.isArray(normalized.parameters)) {
        normalized.parameters = normalized.parameters.map(normalizeVdsParameter);
    }
    return normalized;
}

export function collectVdsQueryDiagnostics(originalQuery, normalizedQuery) {
    const diagnostics = [];
    if (!originalQuery || typeof originalQuery !== 'object') return diagnostics;

    const originalKeys = Object.keys(originalQuery);
    const strippedKeys = originalKeys.filter((key) => !QUERY_KEYS.includes(key));
    if (strippedKeys.length > 0) {
        diagnostics.push(`Stripped unsupported query keys: ${strippedKeys.join(', ')}`);
    }

    if (!Array.isArray(normalizedQuery?.fields) || normalizedQuery.fields.length === 0) {
        diagnostics.push('Query has no fields after normalization.');
    }

    for (const [index, field] of (normalizedQuery?.fields || []).entries()) {
        if (!field?.fieldCaption) {
            diagnostics.push(`Field[${index}] is missing fieldCaption.`);
        }
        if (field?.function && !TABLEAU_FUNCTIONS.has(String(field.function))) {
            diagnostics.push(`Field[${index}] uses unsupported function: ${field.function}`);
        }
        if (field?.sortDirection && !SORT_DIRECTIONS.has(String(field.sortDirection))) {
            diagnostics.push(`Field[${index}] uses unsupported sortDirection: ${field.sortDirection}`);
        }
    }

    const filterFieldMap = new Map();
    for (const [index, filter] of (normalizedQuery?.filters || []).entries()) {
        if (!FILTER_TYPES.has(String(filter?.filterType))) {
            diagnostics.push(`Filter[${index}] uses unsupported filterType: ${filter?.filterType}`);
        }

        const filterFieldCaption = filter?.field?.fieldCaption || filter?.field?.calculation || `__filter_${index}`;
        filterFieldMap.set(filterFieldCaption, (filterFieldMap.get(filterFieldCaption) || 0) + 1);

        if (filter?.filterType === 'DATE') {
            if (!filter.periodType || !filter.dateRangeType) {
                diagnostics.push(`Filter[${index}] DATE filter is missing periodType or dateRangeType.`);
            }
            if (filter.min !== undefined || filter.max !== undefined || filter.minDate !== undefined || filter.maxDate !== undefined) {
                diagnostics.push(`Filter[${index}] DATE filter looks like an absolute date range and should be QUANTITATIVE_DATE.`);
            }
            if (filter.specificDate !== undefined || filter.range !== undefined) {
                diagnostics.push(`Filter[${index}] DATE filter uses specificDate/range syntax and may need conversion to QUANTITATIVE_DATE.`);
            }
        }

        if (filter?.filterType === 'QUANTITATIVE_DATE') {
            if (!filter.quantitativeFilterType) {
                diagnostics.push(`Filter[${index}] QUANTITATIVE_DATE filter is missing quantitativeFilterType.`);
            }
            if (filter.minDate && !/^\d{4}-\d{2}-\d{2}$/.test(String(filter.minDate))) {
                diagnostics.push(`Filter[${index}] minDate is not YYYY-MM-DD: ${filter.minDate}`);
            }
            if (filter.maxDate && !/^\d{4}-\d{2}-\d{2}$/.test(String(filter.maxDate))) {
                diagnostics.push(`Filter[${index}] maxDate is not YYYY-MM-DD: ${filter.maxDate}`);
            }
        }

        if (filter?.filterType === 'TOP' && !filter?.fieldToMeasure?.function) {
            diagnostics.push(`Filter[${index}] TOP filter is missing fieldToMeasure.function.`);
        }

        if (filter?.filterType === 'QUANTITATIVE_NUMERICAL' && !filter?.field?.function && !filter?.field?.calculation) {
            diagnostics.push(`Filter[${index}] QUANTITATIVE_NUMERICAL filter should use a measure field or calculation.`);
        }
    }

    for (const [fieldCaption, count] of filterFieldMap.entries()) {
        if (count > 1) {
            diagnostics.push(`Multiple filters target the same field: ${fieldCaption}`);
        }
    }

    for (const [index, parameter] of (normalizedQuery?.parameters || []).entries()) {
        if (!parameter?.parameterCaption) {
            diagnostics.push(`Parameter[${index}] is missing parameterCaption.`);
        }
        if (parameter?.parameterType && !PARAMETER_TYPES.has(String(parameter.parameterType))) {
            diagnostics.push(`Parameter[${index}] uses unsupported parameterType: ${parameter.parameterType}`);
        }
        if (parameter?.dataType && !DATA_TYPES.has(String(parameter.dataType))) {
            diagnostics.push(`Parameter[${index}] uses unsupported dataType: ${parameter.dataType}`);
        }
    }

    return diagnostics;
}

export function normalizeQueryDatasourceCall(callLike) {
    const cloned = JSON.parse(JSON.stringify(callLike));
    const targetArgs = cloned.args || cloned.arguments;
    if (!targetArgs?.query) return cloned;
    const originalQuery = JSON.parse(JSON.stringify(targetArgs.query));
    targetArgs.query = normalizeVdsQuery(targetArgs.query);
    const diagnostics = collectVdsQueryDiagnostics(originalQuery, targetArgs.query);
    if (diagnostics.length > 0) {
        targetArgs._vdsDiagnostics = diagnostics;
    }
    return cloned;
}
