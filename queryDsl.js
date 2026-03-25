import {
    buildCalculatedField,
    buildDimensionField,
    buildMeasureField,
    buildQuantitativeDateRangeFilter,
    buildSetFilter,
    buildTopNFilter,
    normalizeVdsParameter,
    normalizeVdsQuery
} from './vdsQuery.js';

function clone(value) {
    return JSON.parse(JSON.stringify(value));
}

export function createEmptyDsl() {
    return {
        dimensions: [],
        measures: [],
        calculations: [],
        filters: [],
        parameters: []
    };
}

export function collectDslDiagnostics(dsl) {
    const diagnostics = [];
    if (!dsl || typeof dsl !== 'object') {
        diagnostics.push('DSL is missing or invalid.');
        return diagnostics;
    }

    if (!Array.isArray(dsl.dimensions) && !Array.isArray(dsl.measures) && !Array.isArray(dsl.calculations)) {
        diagnostics.push('DSL does not expose dimensions/measures/calculations arrays.');
    }

    if ((dsl.dimensions || []).length === 0 && (dsl.measures || []).length === 0 && (dsl.calculations || []).length === 0) {
        diagnostics.push('DSL has no output fields.');
    }

    for (const [index, measure] of (dsl.measures || []).entries()) {
        if (!measure?.fieldCaption) diagnostics.push(`Measure[${index}] is missing fieldCaption.`);
        if (!measure?.function) diagnostics.push(`Measure[${index}] is missing function.`);
    }

    for (const [index, calc] of (dsl.calculations || []).entries()) {
        if (!calc?.fieldCaption) diagnostics.push(`Calculation[${index}] is missing fieldCaption.`);
        if (!calc?.calculation) diagnostics.push(`Calculation[${index}] is missing calculation.`);
    }

    return diagnostics;
}

function buildDslField(entry, kind) {
    if (kind === 'dimension') {
        return buildDimensionField(entry.fieldCaption, {
            fieldAlias: entry.fieldAlias,
            function: entry.function,
            calculation: entry.calculation,
            sortDirection: entry.sortDirection,
            sortPriority: entry.sortPriority
        });
    }

    if (kind === 'measure') {
        return buildMeasureField(entry.fieldCaption, entry.function, {
            fieldAlias: entry.fieldAlias,
            sortDirection: entry.sortDirection,
            sortPriority: entry.sortPriority
        });
    }

    if (kind === 'calculation') {
        return buildCalculatedField(entry.fieldCaption, entry.calculation, {
            fieldAlias: entry.fieldAlias,
            sortDirection: entry.sortDirection,
            sortPriority: entry.sortPriority
        });
    }

    return entry;
}

function buildDslFilter(filter) {
    if (!filter || typeof filter !== 'object') return filter;

    if (filter.kind === 'year') {
        return buildQuantitativeDateRangeFilter(filter.fieldCaption || 'Order Date', String(filter.year));
    }

    if (filter.kind === 'dateRange') {
        return buildQuantitativeDateRangeFilter(filter.fieldCaption || 'Order Date', filter.minDate, filter.maxDate);
    }

    if (filter.kind === 'set') {
        return buildSetFilter(filter.fieldCaption, filter.values || [], { exclude: !!filter.exclude });
    }

    if (filter.kind === 'topN') {
        return buildTopNFilter(
            filter.fieldCaption,
            filter.measureFieldCaption,
            filter.howMany ?? 5,
            filter.measureFunction || 'SUM',
            { direction: filter.direction || 'TOP' }
        );
    }

    return filter;
}

export function buildVdsQueryFromDsl(dsl) {
    const safeDsl = clone(dsl || createEmptyDsl());
    const fields = [
        ...(safeDsl.dimensions || []).map((entry) => buildDslField(entry, 'dimension')),
        ...(safeDsl.measures || []).map((entry) => buildDslField(entry, 'measure')),
        ...(safeDsl.calculations || []).map((entry) => buildDslField(entry, 'calculation'))
    ];

    const query = {
        fields,
        filters: (safeDsl.filters || []).map(buildDslFilter),
        parameters: (safeDsl.parameters || []).map(normalizeVdsParameter)
    };

    return normalizeVdsQuery(query);
}
