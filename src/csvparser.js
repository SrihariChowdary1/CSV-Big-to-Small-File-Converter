import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';

class CSVParser {
    constructor(options = {}) {
        this.inputFilePath = options.inputFilePath || 'users_202506020651.csv';
        this.outputDirectory = options.outputDirectory || './split_csv_output';
        this.maxRowsPerFile = options.maxRowsPerFile || 100000;
        this.useMultipleProcesses = options.useMultipleProcesses || false;
        this.processCount = options.processCount || 4;

        this.currentFileIndex = 1;
        this.currentRowCount = 0;
        this.totalRowsProcessed = 0;
        this.currentOutputStream = null;
        this.headers = [];
        this.startTime = null;
    }

    ensureOutputDirectory() {
        if (!fs.existsSync(this.outputDirectory)) {
            fs.mkdirSync(this.outputDirectory, { recursive: true });
            console.log(`📁 Created output directory: ${this.outputDirectory}`);
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

    createNewOutputStream() {
        if (this.currentOutputStream) {
            this.currentOutputStream.end();
            console.log(`✅ Completed file ${this.currentFileIndex - 1} with ${this.currentRowCount} records`);
        }

        const timestamp = new Date().toISOString().split('T')[0];
        const outputFileName = `split_part_${this.currentFileIndex}_${timestamp}.csv`;
        const outputFilePath = path.join(this.outputDirectory, outputFileName);

        this.currentOutputStream = fs.createWriteStream(outputFilePath, { encoding: 'utf8' });

        // Write headers
        if (this.headers.length > 0) {
            this.currentOutputStream.write(this.headers.join(',') + '\n');
        }

        this.currentRowCount = 0;
        console.log(`📝 Created new CSV file: ${outputFileName}`);
        this.currentFileIndex++;
    }

    async getFileStats() {
        try {
            const stats = fs.statSync(this.inputFilePath);
            const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);
            console.log(`📊 Input file size: ${fileSizeInMB} MB`);
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
                    console.log(`📋 Detected ${headers.length} columns in CSV`);
                    console.log(`📋 Headers: ${headers.slice(0, 5).join(', ')}${headers.length > 5 ? '...' : ''}`);

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
        if (this.startTime) {
            const elapsed = (Date.now() - this.startTime) / 1000;
            const rate = Math.round(this.totalRowsProcessed / elapsed);
            console.log(`⏳ Processed ${this.totalRowsProcessed} rows in ${elapsed.toFixed(1)}s (${rate} rows/sec)`);
        }
    }

    async processSingleThread() {
        console.log('🔄 Starting single-threaded CSV processing...');

        this.createNewOutputStream();
        const readStream = fs.createReadStream(this.inputFilePath);

        return new Promise((resolve, reject) => {
            readStream
                .pipe(csv())
                .on('data', (row) => {
                    // Write row data
                    const rowValues = this.headers.map(header => this.escapeCSVValue(row[header] || ''));
                    this.currentOutputStream.write(rowValues.join(',') + '\n');

                    this.currentRowCount++;
                    this.totalRowsProcessed++;

                    // Show progress every 10000 rows
                    if (this.totalRowsProcessed % 10000 === 0) {
                        this.showProgress();
                    }

                    // Create new file if current file is full
                    if (this.currentRowCount >= this.maxRowsPerFile) {
                        this.createNewOutputStream();
                    }
                })
                .on('end', () => {
                    if (this.currentOutputStream) {
                        this.currentOutputStream.end();
                        console.log(`✅ Completed final file ${this.currentFileIndex - 1} with ${this.currentRowCount} records`);
                    }
                    resolve();
                })
                .on('error', reject);
        });
    }

    async processMultiThread() {
        console.log(`🚀 Starting multi-threaded CSV processing with ${this.processCount} workers...`);

        // For multi-threading, we'll need to implement chunk-based processing
        // This is a simplified version - in production, you'd want more sophisticated chunking
        console.log('⚠️ Multi-threading implementation requires more complex setup.');
        console.log('🔄 Falling back to single-threaded processing for now...');

        return this.processSingleThread();
    }

    /**
     * Main processing method
     */
    async process() {
        try {
            console.log('🚀 Starting Enhanced CSV Parser...');
            console.log(`📁 Input file: ${this.inputFilePath}`);
            console.log(`📁 Output directory: ${this.outputDirectory}`);
            console.log(`📊 Max rows per file: ${this.maxRowsPerFile.toLocaleString()}`);

            this.startTime = Date.now();

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

            console.log('\n🎉 CSV processing completed successfully!');
            console.log(`📊 Total rows processed: ${this.totalRowsProcessed.toLocaleString()}`);
            console.log(`📊 Total files created: ${this.currentFileIndex - 1}`);
            console.log(`⏱️ Total time: ${totalTime.toFixed(2)} seconds`);
            console.log(`⚡ Average rate: ${avgRate.toLocaleString()} rows/second`);
            console.log(`📁 Output files saved in: ${this.outputDirectory}`);

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
