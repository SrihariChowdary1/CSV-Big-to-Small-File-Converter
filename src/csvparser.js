import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';
import { WorkerPool } from './workers/csv-worker.js';
import { createFormatter } from './formatters/index.js';
import {
    ColumnFilter,
    DataTypeConverter,
    DataValidator,
    TransformationPipeline
} from './transformers/index.js';

class CSVParser {
    constructor(options = {}) {
        this.inputFilePath = options.inputFilePath || 'users_202506020651.csv';
        this.outputDirectory = options.outputDirectory || './split_csv_output';
        this.maxRowsPerFile = options.maxRowsPerFile || 100000;
        this.useMultipleProcesses = options.useMultipleProcesses || false;
        this.processCount = options.processCount || 4;

        // New options for enhanced functionality
        this.outputFormat = options.outputFormat || 'csv';
        this.transformations = options.transformations || null;
        this.generateStats = options.generateStats || false;
        this.quiet = options.quiet || false;
        this.chunkSizeBytes = options.chunkSizeBytes || 50 * 1024 * 1024; // 50MB chunks

        this.currentFileIndex = 1;
        this.currentRowCount = 0;
        this.totalRowsProcessed = 0;
        this.currentOutputStream = null;
        this.headers = [];
        this.outputHeaders = [];
        this.startTime = null;
        this.formatter = null;
        this.transformationPipeline = null;
        this.workerPool = null;
    }

    ensureOutputDirectory() {
        if (!fs.existsSync(this.outputDirectory)) {
            fs.mkdirSync(this.outputDirectory, { recursive: true });
            if (!this.quiet) {
                console.log(`📁 Created output directory: ${this.outputDirectory}`);
            }
        }
    }

    initializeFormatter() {
        this.formatter = createFormatter(this.outputFormat, {
            rootElement: 'data',
            rowElement: 'row'
        });
    }

    initializeTransformations() {
        if (!this.transformations) return;

        this.transformationPipeline = new TransformationPipeline();

        // Add column filter
        if (this.transformations.includeColumns || this.transformations.excludeColumns) {
            const filter = new ColumnFilter(
                this.transformations.includeColumns || [],
                this.transformations.excludeColumns || []
            );
            this.transformationPipeline.addTransformer(filter);
        }

        // Add data type converter
        if (this.transformations.typeConversions) {
            const converter = new DataTypeConverter(this.transformations.typeConversions);
            this.transformationPipeline.addTransformer(converter);
        }

        // Add validator
        if (this.transformations.validation) {
            const validator = new DataValidator(this.transformations.validation);
            this.transformationPipeline.addTransformer(validator);
        }

        // Enable aggregation if stats are requested
        if (this.generateStats) {
            this.transformationPipeline.enableAggregation();
        }
    }
    escapeCSVValue(value) {
        if (value === null || value === undefined) return '';
        const str = String(value);
        if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
            return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
    }

    async createNewOutputStream() {
        if (this.currentOutputStream) {
            await this.formatter.writeFooter(this.currentOutputStream);
            this.currentOutputStream.end();
            if (!this.quiet) {
                console.log(`✅ Completed file ${this.currentFileIndex - 1} with ${this.currentRowCount} records`);
            }
        }

        const timestamp = new Date().toISOString().split('T')[0];
        const fileExtension = this.formatter.getFileExtension();
        const outputFileName = `split_part_${this.currentFileIndex}_${timestamp}${fileExtension}`;
        const outputFilePath = path.join(this.outputDirectory, outputFileName);

        this.currentOutputStream = fs.createWriteStream(outputFilePath, { encoding: 'utf8' });

        // Write headers using formatter
        if (this.outputHeaders.length > 0) {
            await this.formatter.writeHeader(this.currentOutputStream, this.outputHeaders);
        }

        this.currentRowCount = 0;
        if (!this.quiet) {
            console.log(`📝 Created new ${this.outputFormat.toUpperCase()} file: ${outputFileName}`);
        }
        this.currentFileIndex++;
    }

    createNewOutputStreamSync() {
        if (this.currentOutputStream) {
            this.writeFooterSync();
            this.currentOutputStream.end();
            if (!this.quiet) {
                console.log(`✅ Completed file ${this.currentFileIndex - 1} with ${this.currentRowCount} records`);
            }
        }

        const timestamp = new Date().toISOString().split('T')[0];
        const fileExtension = this.formatter.getFileExtension();
        const outputFileName = `split_part_${this.currentFileIndex}_${timestamp}${fileExtension}`;
        const outputFilePath = path.join(this.outputDirectory, outputFileName);

        this.currentOutputStream = fs.createWriteStream(outputFilePath, { encoding: 'utf8' });

        // Write headers using formatter (synchronously)
        if (this.outputHeaders.length > 0) {
            this.writeHeaderSync();
        }

        this.currentRowCount = 0;
        if (!this.quiet) {
            console.log(`📝 Created new ${this.outputFormat.toUpperCase()} file: ${outputFileName}`);
        }
        this.currentFileIndex++;
    }

    writeHeaderSync() {
        // Simple synchronous header writing for CSV
        if (this.outputFormat === 'csv') {
            this.currentOutputStream.write(this.outputHeaders.join(',') + '\n');
        } else if (this.outputFormat === 'json' || this.outputFormat === 'jsonl') {
            // For JSON, we don't write headers
        } else if (this.outputFormat === 'xml') {
            this.currentOutputStream.write('<?xml version="1.0" encoding="UTF-8"?>\n<data>\n');
        } else if (this.outputFormat === 'tsv') {
            this.currentOutputStream.write(this.outputHeaders.join('\t') + '\n');
        }
    }

    writeRowSync(row) {
        if (this.outputFormat === 'csv') {
            const rowValues = this.outputHeaders.map(header => this.escapeCSVValue(row[header] || ''));
            this.currentOutputStream.write(rowValues.join(',') + '\n');
        } else if (this.outputFormat === 'json' || this.outputFormat === 'jsonl') {
            this.currentOutputStream.write(JSON.stringify(row) + '\n');
        } else if (this.outputFormat === 'xml') {
            this.currentOutputStream.write('  <row>\n');
            this.outputHeaders.forEach(header => {
                const value = this.escapeXMLValue(row[header] || '');
                this.currentOutputStream.write(`    <${header}>${value}</${header}>\n`);
            });
            this.currentOutputStream.write('  </row>\n');
        } else if (this.outputFormat === 'tsv') {
            const rowValues = this.outputHeaders.map(header => this.escapeTSVValue(row[header] || ''));
            this.currentOutputStream.write(rowValues.join('\t') + '\n');
        }
    }

    writeFooterSync() {
        if (this.outputFormat === 'xml') {
            this.currentOutputStream.write('</data>\n');
        }
        // Other formats don't need footers
    }

    escapeXMLValue(value) {
        if (value === null || value === undefined) return '';
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;');
    }

    escapeTSVValue(value) {
        if (value === null || value === undefined) return '';
        const str = String(value);
        return str.replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r');
    }

    async getFileStats() {
        try {
            const stats = fs.statSync(this.inputFilePath);
            const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);
            if (!this.quiet) {
                console.log(`📊 Input file size: ${fileSizeInMB} MB`);
            }
            return stats;
        } catch (error) {
            throw new Error(`Cannot access input file: ${this.inputFilePath}`);
        }
    }

    async detectHeaders() {
        return new Promise((resolve, reject) => {
            const readStream = fs.createReadStream(this.inputFilePath);
            let headerDetected = false;

            const csvStream = readStream.pipe(csv());

            csvStream
                .on('headers', (headers) => {
                    this.headers = headers;
                    headerDetected = true;

                    // Calculate output headers based on transformations
                    this.outputHeaders = this.transformationPipeline
                        ? this.transformationPipeline.getOutputHeaders(headers)
                        : headers;

                    if (!this.quiet) {
                        console.log(`📋 Detected ${headers.length} columns in CSV`);
                        console.log(`📋 Headers: ${headers.slice(0, 5).join(', ')}${headers.length > 5 ? '...' : ''}`);
                        if (this.outputHeaders.length !== headers.length) {
                            console.log(`📋 Output headers (${this.outputHeaders.length}): ${this.outputHeaders.slice(0, 5).join(', ')}${this.outputHeaders.length > 5 ? '...' : ''}`);
                        }
                    }

                    // Properly close the streams
                    csvStream.destroy();
                    readStream.destroy();
                    resolve(this.headers);
                })
                .on('error', (error) => {
                    readStream.destroy();
                    reject(error);
                })
                .on('data', () => {
                    // We only need headers, so destroy after first data row if headers weren't detected
                    if (headerDetected) {
                        csvStream.destroy();
                        readStream.destroy();
                    }
                });
        });
    }

    showProgress() {
        if (this.startTime && !this.quiet) {
            const elapsed = (Date.now() - this.startTime) / 1000;
            const rate = Math.round(this.totalRowsProcessed / elapsed);
            console.log(`⏳ Processed ${this.totalRowsProcessed} rows in ${elapsed.toFixed(1)}s (${rate} rows/sec)`);
        }
    }

    async processSingleThread() {
        if (!this.quiet) {
            console.log('🔄 Starting single-threaded CSV processing...');
        }

        this.createNewOutputStreamSync();
        const readStream = fs.createReadStream(this.inputFilePath);

        return new Promise((resolve, reject) => {
            readStream
                .pipe(csv())
                .on('data', (row) => {
                    try {
                        // Apply transformations if configured
                        let transformedRow = row;
                        if (this.transformationPipeline) {
                            transformedRow = this.transformationPipeline.transform(row, this.headers);
                            if (transformedRow === null) {
                                return; // Skip this row
                            }
                        }

                        // Write row using the appropriate formatter (synchronously)
                        this.writeRowSync(transformedRow);

                        this.currentRowCount++;
                        this.totalRowsProcessed++;

                        // Show progress every 10000 rows
                        if (this.totalRowsProcessed % 10000 === 0) {
                            this.showProgress();
                        }

                        // Create new file if current file is full
                        if (this.currentRowCount >= this.maxRowsPerFile) {
                            this.createNewOutputStreamSync();
                        }
                    } catch (error) {
                        console.error(`Error processing row: ${error.message}`);
                    }
                })
                .on('end', () => {
                    try {
                        if (this.currentOutputStream) {
                            this.writeFooterSync();
                            this.currentOutputStream.end();
                            if (!this.quiet) {
                                console.log(`✅ Completed final file ${this.currentFileIndex - 1} with ${this.currentRowCount} records`);
                            }
                        }
                        resolve();
                    } catch (error) {
                        reject(error);
                    }
                })
                .on('error', reject);
        });
    }

    async processMultiThread() {
        if (!this.quiet) {
            console.log(`🚀 Starting multi-threaded CSV processing with ${this.processCount} workers...`);
        }

        try {
            // Initialize worker pool
            this.workerPool = new WorkerPool(this.processCount);

            // Get file size for chunking
            const stats = await this.getFileStats();
            const fileSize = stats.size;

            // Create chunks based on byte ranges
            const chunks = this.createFileChunks(fileSize);

            if (!this.quiet) {
                console.log(`📊 Created ${chunks.length} chunks for processing`);
            }

            // Process chunks in parallel
            const workerData = {
                inputFilePath: this.inputFilePath,
                outputDirectory: this.outputDirectory,
                headers: this.headers,
                maxRowsPerFile: this.maxRowsPerFile,
                outputFormat: this.outputFormat,
                transformations: this.transformations
            };

            const results = await this.workerPool.processChunks(chunks, workerData);

            // Aggregate results
            let totalRowsProcessed = 0;
            let totalFilesCreated = 0;
            const allErrors = [];

            results.forEach(result => {
                totalRowsProcessed += result.rowsProcessed;
                totalFilesCreated += result.filesCreated.length;
                allErrors.push(...result.errors);
            });

            this.totalRowsProcessed = totalRowsProcessed;
            this.currentFileIndex = totalFilesCreated + 1;

            if (allErrors.length > 0 && !this.quiet) {
                console.warn(`⚠️ ${allErrors.length} errors occurred during processing`);
                allErrors.slice(0, 5).forEach(error => console.warn(`  - ${error}`));
                if (allErrors.length > 5) {
                    console.warn(`  ... and ${allErrors.length - 5} more errors`);
                }
            }

            await this.workerPool.terminate();

        } catch (error) {
            if (!this.quiet) {
                console.log('⚠️ Multi-threading failed, falling back to single-threaded processing...');
                console.log(`Error: ${error.message}`);
            }
            return this.processSingleThread();
        }
    }

    createFileChunks(fileSize) {
        const chunks = [];
        const chunkSize = Math.min(this.chunkSizeBytes, Math.ceil(fileSize / this.processCount));

        for (let start = 0; start < fileSize; start += chunkSize) {
            const end = Math.min(start + chunkSize - 1, fileSize - 1);
            chunks.push({
                startByte: start,
                endByte: end
            });
        }

        return chunks;
    }

    /**
     * Main processing method
     */
    async process() {
        try {
            if (!this.quiet) {
                console.log('🚀 Starting Enhanced CSV Parser v2.0...');
                console.log(`📁 Input file: ${this.inputFilePath}`);
                console.log(`📁 Output directory: ${this.outputDirectory}`);
                console.log(`📊 Max rows per file: ${this.maxRowsPerFile.toLocaleString()}`);
                console.log(`📄 Output format: ${this.outputFormat.toUpperCase()}`);
            }

            this.startTime = Date.now();

            // Initialize components
            this.initializeFormatter();
            this.initializeTransformations();

            // Validate input file
            await this.getFileStats();

            // Ensure output directory exists
            this.ensureOutputDirectory();

            // Detect CSV structure
            await this.detectHeaders();

            // Process the file
            if (this.useMultipleProcesses) {
                await this.processMultiThread();
            } else {
                await this.processSingleThread();
            }

            // Show final statistics
            const totalTime = (Date.now() - this.startTime) / 1000;
            const avgRate = Math.round(this.totalRowsProcessed / totalTime);

            if (!this.quiet) {
                console.log('\n🎉 CSV processing completed successfully!');
                console.log(`📊 Total rows processed: ${this.totalRowsProcessed.toLocaleString()}`);
                console.log(`📊 Total files created: ${this.currentFileIndex - 1}`);
                console.log(`⏱️ Total time: ${totalTime.toFixed(2)} seconds`);
                console.log(`⚡ Average rate: ${avgRate.toLocaleString()} rows/second`);
                console.log(`📁 Output files saved in: ${this.outputDirectory}`);
            }

            // Show transformation statistics if available
            if (this.transformationPipeline && this.generateStats) {
                const stats = this.transformationPipeline.getStatistics();
                if (stats && !this.quiet) {
                    console.log('\n📈 Data Statistics:');
                    console.log(`📊 Total rows analyzed: ${stats.totalRows.toLocaleString()}`);
                    console.log(`📊 Columns analyzed: ${Object.keys(stats.columns).length}`);

                    // Show top 3 columns with most null values
                    const nullStats = Object.entries(stats.columns)
                        .map(([col, stat]) => ({ column: col, nullPercentage: parseFloat(stat.nullPercentage) }))
                        .sort((a, b) => b.nullPercentage - a.nullPercentage)
                        .slice(0, 3);

                    if (nullStats.length > 0) {
                        console.log('📊 Columns with highest null percentages:');
                        nullStats.forEach(({ column, nullPercentage }) => {
                            console.log(`   - ${column}: ${nullPercentage}%`);
                        });
                    }
                }
            }

        } catch (error) {
            console.error('❌ Error during CSV processing:', error.message);
            throw error;
        }
    }

    /**
     * Static method to create and run parser with options
     */
    static async run(options = {}) {
        const parser = new CSVParser(options);
        await parser.process();
        return parser;
    }
}

/**
 * Configuration and usage examples
 */
const defaultConfig = {
    inputFilePath: 'users_202506020651.csv',
    outputDirectory: './split_csv_output',
    maxRowsPerFile: 100000,
    useMultipleProcesses: false,
    processCount: 4
};

/**
 * Main execution function
 */
async function main() {
    try {
        // Check if we have command line arguments
        const args = process.argv.slice(2);
        let config = { ...defaultConfig };

        // Parse command line arguments
        for (let i = 0; i < args.length; i += 2) {
            const key = args[i]?.replace('--', '');
            const value = args[i + 1];

            if (key && value) {
                switch (key) {
                    case 'input':
                        config.inputFilePath = value;
                        break;
                    case 'output':
                        config.outputDirectory = value;
                        break;
                    case 'rows':
                        config.maxRowsPerFile = parseInt(value);
                        break;
                    case 'multi':
                        config.useMultipleProcesses = value.toLowerCase() === 'true';
                        break;
                    case 'processes':
                        config.processCount = parseInt(value);
                        break;
                }
            }
        }

        // Show usage if no input file specified and default doesn't exist
        if (!fs.existsSync(config.inputFilePath)) {
            console.log('📖 CSV Parser Usage:');
            console.log('node csvparser.js --input <input.csv> --output <output_dir> --rows <max_rows_per_file>');
            console.log('');
            console.log('Options:');
            console.log('  --input     Input CSV file path');
            console.log('  --output    Output directory for split files');
            console.log('  --rows      Maximum rows per output file (default: 100000)');
            console.log('  --multi     Use multiple processes (true/false, default: false)');
            console.log('  --processes Number of worker processes (default: 4)');
            console.log('');
            console.log('Example:');
            console.log('node csvparser.js --input large_file.csv --output ./split_files --rows 50000');
            return;
        }

        // Run the parser
        await CSVParser.run(config);

    } catch (error) {
        console.error('❌ Fatal error:', error.message);
        process.exit(1);
    }
}

// Export the class for use as a module
export default CSVParser;
export { CSVParser };

// Run main function if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    main();
}
