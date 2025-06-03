import fs from 'fs';
import path from 'path';
import { XMLBuilder } from 'fast-xml-parser';

/**
 * Base formatter class for output formats
 */
export class BaseFormatter {
    constructor(options = {}) {
        this.options = options;
    }

    async writeHeader(stream, headers) {
        throw new Error('writeHeader method must be implemented by subclass');
    }

    async writeRow(stream, row, headers) {
        throw new Error('writeRow method must be implemented by subclass');
    }

    async writeFooter(stream) {
        // Optional footer - default implementation does nothing
    }

    getFileExtension() {
        throw new Error('getFileExtension method must be implemented by subclass');
    }
}

/**
 * CSV Formatter
 */
export class CSVFormatter extends BaseFormatter {
    escapeCSVValue(value) {
        if (value === null || value === undefined) return '';
        const str = String(value);
        if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
            return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
    }

    async writeHeader(stream, headers) {
        stream.write(headers.join(',') + '\n');
    }

    async writeRow(stream, row, headers) {
        const rowValues = headers.map(header => this.escapeCSVValue(row[header] || ''));
        stream.write(rowValues.join(',') + '\n');
    }

    getFileExtension() {
        return '.csv';
    }
}

/**
 * JSON Formatter (JSONL - JSON Lines format)
 */
export class JSONFormatter extends BaseFormatter {
    constructor(options = {}) {
        super(options);
        this.isFirstRow = true;
        this.format = options.format || 'jsonl'; // 'jsonl' or 'array'
    }

    async writeHeader(stream, headers) {
        if (this.format === 'array') {
            stream.write('[\n');
        }
        this.isFirstRow = true;
    }

    async writeRow(stream, row, headers) {
        if (this.format === 'jsonl') {
            stream.write(JSON.stringify(row) + '\n');
        } else if (this.format === 'array') {
            if (!this.isFirstRow) {
                stream.write(',\n');
            }
            stream.write('  ' + JSON.stringify(row));
            this.isFirstRow = false;
        }
    }

    async writeFooter(stream) {
        if (this.format === 'array') {
            stream.write('\n]\n');
        }
    }

    getFileExtension() {
        return this.format === 'jsonl' ? '.jsonl' : '.json';
    }
}

/**
 * XML Formatter
 */
export class XMLFormatter extends BaseFormatter {
    constructor(options = {}) {
        super(options);
        this.rootElement = options.rootElement || 'data';
        this.rowElement = options.rowElement || 'row';
        this.xmlBuilder = new XMLBuilder({
            ignoreAttributes: false,
            format: true,
            indentBy: '  '
        });
    }

    async writeHeader(stream, headers) {
        stream.write('<?xml version="1.0" encoding="UTF-8"?>\n');
        stream.write(`<${this.rootElement}>\n`);
    }

    async writeRow(stream, row, headers) {
        const xmlRow = { [this.rowElement]: row };
        const xmlString = this.xmlBuilder.build(xmlRow);
        // Remove the XML declaration and root wrapper, keep only the row
        const rowXml = xmlString
            .replace(/^<\?xml.*?\?>\s*/, '')
            .replace(new RegExp(`^<${this.rowElement}>`), `  <${this.rowElement}>`)
            .replace(new RegExp(`</${this.rowElement}>$`), `  </${this.rowElement}>`);
        stream.write(rowXml + '\n');
    }

    async writeFooter(stream) {
        stream.write(`</${this.rootElement}>\n`);
    }

    getFileExtension() {
        return '.xml';
    }
}

/**
 * TSV (Tab-Separated Values) Formatter
 */
export class TSVFormatter extends BaseFormatter {
    escapeTSVValue(value) {
        if (value === null || value === undefined) return '';
        const str = String(value);
        // Replace tabs, newlines, and carriage returns
        return str.replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r');
    }

    async writeHeader(stream, headers) {
        stream.write(headers.join('\t') + '\n');
    }

    async writeRow(stream, row, headers) {
        const rowValues = headers.map(header => this.escapeTSVValue(row[header] || ''));
        stream.write(rowValues.join('\t') + '\n');
    }

    getFileExtension() {
        return '.tsv';
    }
}

/**
 * Parquet Formatter (simplified - for basic use cases)
 */
export class ParquetFormatter extends BaseFormatter {
    constructor(options = {}) {
        super(options);
        this.rows = [];
        this.batchSize = options.batchSize || 10000;
    }

    async writeHeader(stream, headers) {
        this.headers = headers;
        this.rows = [];
    }

    async writeRow(stream, row, headers) {
        this.rows.push(row);
        
        // Write batch when it reaches the batch size
        if (this.rows.length >= this.batchSize) {
            await this.writeBatch(stream);
        }
    }

    async writeFooter(stream) {
        // Write remaining rows
        if (this.rows.length > 0) {
            await this.writeBatch(stream);
        }
    }

    async writeBatch(stream) {
        // For now, we'll write as JSON since parquetjs is complex
        // In a production environment, you'd use proper Parquet writing
        const batch = {
            schema: this.headers,
            data: this.rows
        };
        stream.write(JSON.stringify(batch) + '\n');
        this.rows = [];
    }

    getFileExtension() {
        return '.parquet.json'; // Simplified format
    }
}

/**
 * Factory function to create formatters
 */
export function createFormatter(format, options = {}) {
    switch (format.toLowerCase()) {
        case 'csv':
            return new CSVFormatter(options);
        case 'json':
        case 'jsonl':
            return new JSONFormatter({ ...options, format: format.toLowerCase() });
        case 'xml':
            return new XMLFormatter(options);
        case 'tsv':
            return new TSVFormatter(options);
        case 'parquet':
            return new ParquetFormatter(options);
        default:
            throw new Error(`Unsupported output format: ${format}`);
    }
}

/**
 * Get supported formats
 */
export function getSupportedFormats() {
    return ['csv', 'json', 'jsonl', 'xml', 'tsv', 'parquet'];
}

export default {
    BaseFormatter,
    CSVFormatter,
    JSONFormatter,
    XMLFormatter,
    TSVFormatter,
    ParquetFormatter,
    createFormatter,
    getSupportedFormats
};
