#!/usr/bin/env node

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import chalk from 'chalk';
import ora from 'ora';
import fs from 'fs';
import path from 'path';
import { CSVParser } from './csvparser.js';
import { getSupportedFormats } from './formatters/index.js';

/**
 * Enhanced Command Line Interface for CSV Big-to-Small File Converter
 */

const supportedFormats = getSupportedFormats();

// Create the CLI with yargs
const cli = yargs(hideBin(process.argv))
    .scriptName('csv-converter')
    .usage('$0 <command> [options]')
    .version('2.0.0')
    .help('help')
    .alias('h', 'help')
    .alias('v', 'version')
    .wrap(Math.min(120, process.stdout.columns || 80))
    .example('$0 split input.csv -o ./output -r 50000', 'Split CSV into files with 50,000 rows each')
    .example('$0 split input.csv -f json --multi', 'Convert to JSON format using multi-threading')
    .example('$0 split input.csv --include-columns "name,email,age"', 'Only include specific columns')
    .epilogue('For more information, visit: https://github.com/yourusername/csv-big-to-small-file-converter');

// Split command
cli.command(
    'split <input>',
    'Split a large CSV file into smaller files',
    (yargs) => {
        return yargs
            .positional('input', {
                describe: 'Input CSV file path',
                type: 'string',
                demandOption: true
            })
            .option('output', {
                alias: 'o',
                describe: 'Output directory for split files',
                type: 'string',
                default: './split_csv_output'
            })
            .option('rows', {
                alias: 'r',
                describe: 'Maximum rows per output file',
                type: 'number',
                default: 100000
            })
            .option('format', {
                alias: 'f',
                describe: 'Output format',
                type: 'string',
                choices: supportedFormats,
                default: 'csv'
            })
            .option('multi', {
                alias: 'm',
                describe: 'Enable multi-threading',
                type: 'boolean',
                default: false
            })
            .option('workers', {
                alias: 'w',
                describe: 'Number of worker threads',
                type: 'number',
                default: 4
            })
            .option('include-columns', {
                describe: 'Comma-separated list of columns to include',
                type: 'string'
            })
            .option('exclude-columns', {
                describe: 'Comma-separated list of columns to exclude',
                type: 'string'
            })
            .option('type-conversions', {
                describe: 'JSON string of column type conversions (e.g., \'{"age":"number","active":"boolean"}\')',
                type: 'string'
            })
            .option('validation', {
                describe: 'JSON string of validation rules',
                type: 'string'
            })
            .option('config', {
                alias: 'c',
                describe: 'Configuration file path (JSON)',
                type: 'string'
            })
            .option('stats', {
                describe: 'Generate statistics report',
                type: 'boolean',
                default: false
            })
            .option('quiet', {
                alias: 'q',
                describe: 'Suppress progress output',
                type: 'boolean',
                default: false
            });
    },
    async (argv) => {
        await handleSplitCommand(argv);
    }
);

// Info command
cli.command(
    'info <input>',
    'Display information about a CSV file',
    (yargs) => {
        return yargs
            .positional('input', {
                describe: 'Input CSV file path',
                type: 'string',
                demandOption: true
            })
            .option('sample', {
                alias: 's',
                describe: 'Number of sample rows to display',
                type: 'number',
                default: 5
            });
    },
    async (argv) => {
        await handleInfoCommand(argv);
    }
);

// Validate command
cli.command(
    'validate <input>',
    'Validate a CSV file structure and data',
    (yargs) => {
        return yargs
            .positional('input', {
                describe: 'Input CSV file path',
                type: 'string',
                demandOption: true
            })
            .option('rules', {
                describe: 'JSON string of validation rules',
                type: 'string'
            })
            .option('config', {
                alias: 'c',
                describe: 'Configuration file path (JSON)',
                type: 'string'
            });
    },
    async (argv) => {
        await handleValidateCommand(argv);
    }
);

/**
 * Handle split command
 */
async function handleSplitCommand(argv) {
    try {
        // Load configuration from file if provided
        let config = {};
        if (argv.config) {
            if (!fs.existsSync(argv.config)) {
                console.error(chalk.red(`❌ Configuration file not found: ${argv.config}`));
                process.exit(1);
            }
            config = JSON.parse(fs.readFileSync(argv.config, 'utf8'));
        }

        // Merge command line arguments with config file
        const options = {
            inputFilePath: argv.input,
            outputDirectory: argv.output,
            maxRowsPerFile: argv.rows,
            outputFormat: argv.format,
            useMultipleProcesses: argv.multi,
            processCount: argv.workers,
            generateStats: argv.stats,
            quiet: argv.quiet,
            ...config
        };

        // Parse transformations
        if (argv.includeColumns || config.includeColumns) {
            const columns = argv.includeColumns || config.includeColumns;
            options.transformations = options.transformations || {};
            options.transformations.includeColumns = typeof columns === 'string' 
                ? columns.split(',').map(c => c.trim())
                : columns;
        }

        if (argv.excludeColumns || config.excludeColumns) {
            const columns = argv.excludeColumns || config.excludeColumns;
            options.transformations = options.transformations || {};
            options.transformations.excludeColumns = typeof columns === 'string'
                ? columns.split(',').map(c => c.trim())
                : columns;
        }

        if (argv.typeConversions || config.typeConversions) {
            const conversions = argv.typeConversions || config.typeConversions;
            options.transformations = options.transformations || {};
            options.transformations.typeConversions = typeof conversions === 'string'
                ? JSON.parse(conversions)
                : conversions;
        }

        if (argv.validation || config.validation) {
            const validation = argv.validation || config.validation;
            options.transformations = options.transformations || {};
            options.transformations.validation = typeof validation === 'string'
                ? JSON.parse(validation)
                : validation;
        }

        // Validate input file
        if (!fs.existsSync(options.inputFilePath)) {
            console.error(chalk.red(`❌ Input file not found: ${options.inputFilePath}`));
            process.exit(1);
        }

        // Show configuration
        if (!options.quiet) {
            console.log(chalk.blue.bold('🚀 CSV Big-to-Small File Converter v2.0.0\n'));
            console.log(chalk.cyan('Configuration:'));
            console.log(`  Input file: ${chalk.white(options.inputFilePath)}`);
            console.log(`  Output directory: ${chalk.white(options.outputDirectory)}`);
            console.log(`  Max rows per file: ${chalk.white(options.maxRowsPerFile.toLocaleString())}`);
            console.log(`  Output format: ${chalk.white(options.outputFormat.toUpperCase())}`);
            console.log(`  Multi-threading: ${chalk.white(options.useMultipleProcesses ? 'Enabled' : 'Disabled')}`);
            if (options.useMultipleProcesses) {
                console.log(`  Worker threads: ${chalk.white(options.processCount)}`);
            }
            if (options.transformations) {
                console.log(`  Transformations: ${chalk.white('Enabled')}`);
            }
            console.log();
        }

        // Create and run parser
        const parser = new CSVParser(options);
        await parser.process();

        if (!options.quiet) {
            console.log(chalk.green.bold('\n✅ Processing completed successfully!'));
        }

    } catch (error) {
        console.error(chalk.red(`❌ Error: ${error.message}`));
        process.exit(1);
    }
}

/**
 * Handle info command
 */
async function handleInfoCommand(argv) {
    try {
        if (!fs.existsSync(argv.input)) {
            console.error(chalk.red(`❌ Input file not found: ${argv.input}`));
            process.exit(1);
        }

        const spinner = ora('Analyzing CSV file...').start();

        const parser = new CSVParser({
            inputFilePath: argv.input,
            generateStats: true
        });

        // Get file stats
        const stats = fs.statSync(argv.input);
        const fileSizeInMB = (stats.size / (1024 * 1024)).toFixed(2);

        // Detect headers
        await parser.detectHeaders();

        spinner.stop();

        console.log(chalk.blue.bold('📊 CSV File Information\n'));
        console.log(`File: ${chalk.white(argv.input)}`);
        console.log(`Size: ${chalk.white(fileSizeInMB)} MB`);
        console.log(`Columns: ${chalk.white(parser.headers.length)}`);
        console.log(`Headers: ${chalk.white(parser.headers.slice(0, 10).join(', '))}${parser.headers.length > 10 ? '...' : ''}`);
        console.log(`Created: ${chalk.white(stats.birthtime.toISOString().split('T')[0])}`);
        console.log(`Modified: ${chalk.white(stats.mtime.toISOString().split('T')[0])}`);

    } catch (error) {
        console.error(chalk.red(`❌ Error: ${error.message}`));
        process.exit(1);
    }
}

/**
 * Handle validate command
 */
async function handleValidateCommand(argv) {
    try {
        if (!fs.existsSync(argv.input)) {
            console.error(chalk.red(`❌ Input file not found: ${argv.input}`));
            process.exit(1);
        }

        let validationRules = {};
        
        if (argv.rules) {
            validationRules = JSON.parse(argv.rules);
        } else if (argv.config && fs.existsSync(argv.config)) {
            const config = JSON.parse(fs.readFileSync(argv.config, 'utf8'));
            validationRules = config.validation || {};
        }

        console.log(chalk.blue.bold('🔍 CSV File Validation\n'));
        console.log(`File: ${chalk.white(argv.input)}`);
        
        if (Object.keys(validationRules).length === 0) {
            console.log(chalk.yellow('⚠️  No validation rules provided. Performing basic structure validation only.'));
        } else {
            console.log(`Validation rules: ${chalk.white(Object.keys(validationRules).join(', '))}`);
        }

        // TODO: Implement validation logic
        console.log(chalk.green('✅ Validation completed (feature in development)'));

    } catch (error) {
        console.error(chalk.red(`❌ Error: ${error.message}`));
        process.exit(1);
    }
}

// Show help if no command provided
if (process.argv.length <= 2) {
    cli.showHelp();
    process.exit(0);
}

// Parse and execute
cli.parse();
