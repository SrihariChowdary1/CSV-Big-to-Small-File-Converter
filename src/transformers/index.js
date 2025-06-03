/**
 * Data Transformation Module
 * Provides various data transformation capabilities for CSV processing
 */

/**
 * Column Filter - Select specific columns
 */
export class ColumnFilter {
    constructor(includeColumns = [], excludeColumns = []) {
        this.includeColumns = includeColumns;
        this.excludeColumns = excludeColumns;
    }

    transform(row, headers) {
        let result = { ...row };

        // If include columns specified, only keep those
        if (this.includeColumns.length > 0) {
            const filtered = {};
            this.includeColumns.forEach(col => {
                if (result[col] !== undefined) {
                    filtered[col] = result[col];
                }
            });
            result = filtered;
        }

        // Remove excluded columns
        if (this.excludeColumns.length > 0) {
            this.excludeColumns.forEach(col => {
                delete result[col];
            });
        }

        return result;
    }

    getOutputHeaders(inputHeaders) {
        let outputHeaders = [...inputHeaders];

        if (this.includeColumns.length > 0) {
            outputHeaders = this.includeColumns.filter(col => inputHeaders.includes(col));
        }

        if (this.excludeColumns.length > 0) {
            outputHeaders = outputHeaders.filter(col => !this.excludeColumns.includes(col));
        }

        return outputHeaders;
    }
}

/**
 * Data Type Converter
 */
export class DataTypeConverter {
    constructor(conversions = {}) {
        this.conversions = conversions;
    }

    transform(row, headers) {
        const result = { ...row };

        Object.entries(this.conversions).forEach(([column, type]) => {
            if (result[column] !== undefined && result[column] !== null && result[column] !== '') {
                try {
                    switch (type.toLowerCase()) {
                        case 'number':
                        case 'float':
                            result[column] = parseFloat(result[column]) || 0;
                            break;
                        case 'integer':
                        case 'int':
                            result[column] = parseInt(result[column]) || 0;
                            break;
                        case 'boolean':
                        case 'bool':
                            result[column] = this.parseBoolean(result[column]);
                            break;
                        case 'date':
                            result[column] = new Date(result[column]).toISOString().split('T')[0];
                            break;
                        case 'datetime':
                            result[column] = new Date(result[column]).toISOString();
                            break;
                        case 'uppercase':
                            result[column] = String(result[column]).toUpperCase();
                            break;
                        case 'lowercase':
                            result[column] = String(result[column]).toLowerCase();
                            break;
                        case 'trim':
                            result[column] = String(result[column]).trim();
                            break;
                        case 'string':
                            result[column] = String(result[column]);
                            break;
                    }
                } catch (error) {
                    console.warn(`Failed to convert ${column} to ${type}: ${error.message}`);
                }
            }
        });

        return result;
    }

    parseBoolean(value) {
        if (typeof value === 'boolean') return value;
        const str = String(value).toLowerCase().trim();
        return ['true', '1', 'yes', 'y', 'on'].includes(str);
    }
}

/**
 * Data Validator
 */
export class DataValidator {
    constructor(rules = {}) {
        this.rules = rules;
    }

    transform(row, headers) {
        // Validate the row against rules
        for (const [column, rule] of Object.entries(this.rules)) {
            const value = row[column];

            // Required field validation
            if (rule.required && this.isEmpty(value)) {
                return null; // Skip this row
            }

            // Type validation
            if (rule.type && !this.isEmpty(value)) {
                if (!this.validateType(value, rule.type)) {
                    return null; // Skip this row
                }
            }

            // Length validation
            if (rule.minLength && !this.isEmpty(value)) {
                if (String(value).length < rule.minLength) {
                    return null; // Skip this row
                }
            }

            if (rule.maxLength && !this.isEmpty(value)) {
                if (String(value).length > rule.maxLength) {
                    return null; // Skip this row
                }
            }

            // Pattern validation
            if (rule.pattern && !this.isEmpty(value)) {
                if (!new RegExp(rule.pattern).test(String(value))) {
                    return null; // Skip this row
                }
            }

            // Range validation for numbers
            if (rule.min !== undefined && !this.isEmpty(value)) {
                const numValue = Number(value);
                if (!isNaN(numValue) && numValue < rule.min) {
                    return null; // Skip this row
                }
            }

            if (rule.max !== undefined && !this.isEmpty(value)) {
                const numValue = Number(value);
                if (!isNaN(numValue) && numValue > rule.max) {
                    return null; // Skip this row
                }
            }

            // Enum validation
            if (rule.enum && !this.isEmpty(value)) {
                if (!rule.enum.includes(value)) {
                    return null; // Skip this row
                }
            }
        }

        return row; // Row passed validation
    }

    isEmpty(value) {
        return value === null || value === undefined || String(value).trim() === '';
    }

    validateType(value, type) {
        switch (type.toLowerCase()) {
            case 'number':
            case 'float':
                return !isNaN(Number(value));
            case 'integer':
            case 'int':
                return Number.isInteger(Number(value));
            case 'boolean':
            case 'bool':
                return ['true', 'false', '1', '0', 'yes', 'no', 'y', 'n', 'on', 'off'].includes(String(value).toLowerCase());
            case 'date':
                return !isNaN(Date.parse(value));
            case 'email':
                return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
            case 'url':
                try {
                    new URL(value);
                    return true;
                } catch {
                    return false;
                }
            default:
                return true; // Unknown type, assume valid
        }
    }
}

/**
 * Custom Function Transformer
 */
export class CustomTransformer {
    constructor(transformFunction) {
        this.transformFunction = transformFunction;
    }

    transform(row, headers) {
        try {
            return this.transformFunction(row, headers);
        } catch (error) {
            console.warn(`Custom transformation failed: ${error.message}`);
            return row; // Return original row on error
        }
    }
}

/**
 * Aggregator for statistical operations
 */
export class Aggregator {
    constructor() {
        this.stats = {};
        this.rowCount = 0;
    }

    process(row, headers) {
        this.rowCount++;

        headers.forEach(header => {
            const value = row[header];
            
            if (!this.stats[header]) {
                this.stats[header] = {
                    count: 0,
                    nullCount: 0,
                    uniqueValues: new Set(),
                    numericValues: [],
                    minLength: Infinity,
                    maxLength: 0
                };
            }

            const stat = this.stats[header];
            stat.count++;

            if (value === null || value === undefined || value === '') {
                stat.nullCount++;
            } else {
                stat.uniqueValues.add(value);
                
                const strValue = String(value);
                stat.minLength = Math.min(stat.minLength, strValue.length);
                stat.maxLength = Math.max(stat.maxLength, strValue.length);

                const numValue = Number(value);
                if (!isNaN(numValue)) {
                    stat.numericValues.push(numValue);
                }
            }
        });

        return row; // Pass through the row unchanged
    }

    getStatistics() {
        const result = {
            totalRows: this.rowCount,
            columns: {}
        };

        Object.entries(this.stats).forEach(([column, stat]) => {
            const columnStats = {
                count: stat.count,
                nullCount: stat.nullCount,
                nullPercentage: ((stat.nullCount / stat.count) * 100).toFixed(2),
                uniqueCount: stat.uniqueValues.size,
                minLength: stat.minLength === Infinity ? 0 : stat.minLength,
                maxLength: stat.maxLength
            };

            // Add numeric statistics if applicable
            if (stat.numericValues.length > 0) {
                const sorted = stat.numericValues.sort((a, b) => a - b);
                const sum = sorted.reduce((a, b) => a + b, 0);
                
                columnStats.numeric = {
                    count: sorted.length,
                    min: sorted[0],
                    max: sorted[sorted.length - 1],
                    mean: (sum / sorted.length).toFixed(2),
                    median: sorted.length % 2 === 0 
                        ? ((sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2).toFixed(2)
                        : sorted[Math.floor(sorted.length / 2)].toFixed(2)
                };
            }

            result.columns[column] = columnStats;
        });

        return result;
    }
}

/**
 * Transformation Pipeline
 */
export class TransformationPipeline {
    constructor() {
        this.transformers = [];
        this.aggregator = null;
    }

    addTransformer(transformer) {
        this.transformers.push(transformer);
        return this;
    }

    enableAggregation() {
        this.aggregator = new Aggregator();
        return this;
    }

    transform(row, headers) {
        let result = row;

        // Apply all transformers in sequence
        for (const transformer of this.transformers) {
            result = transformer.transform(result, headers);
            if (result === null) {
                return null; // Row was filtered out
            }
        }

        // Collect statistics if aggregation is enabled
        if (this.aggregator) {
            this.aggregator.process(result, headers);
        }

        return result;
    }

    getOutputHeaders(inputHeaders) {
        let headers = inputHeaders;
        
        for (const transformer of this.transformers) {
            if (transformer.getOutputHeaders) {
                headers = transformer.getOutputHeaders(headers);
            }
        }
        
        return headers;
    }

    getStatistics() {
        return this.aggregator ? this.aggregator.getStatistics() : null;
    }
}

export default {
    ColumnFilter,
    DataTypeConverter,
    DataValidator,
    CustomTransformer,
    Aggregator,
    TransformationPipeline
};
