#!/usr/bin/env node

import { CSVParser } from '../src/csvparser.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Comprehensive demo of CSV Big-to-Small File Converter v2.0 features
 */

// Create a sample CSV file for demonstration
function createSampleCSV() {
    const sampleData = [
        'id,first_name,last_name,email,age,country,salary,department,active,registration_date',
        '1,John,Doe,john.doe@example.com,30,US,75000,Engineering,true,2023-01-15',
        '2,Jane,Smith,jane.smith@example.com,25,CA,65000,Marketing,true,2023-02-20',
        '3,Bob,Johnson,bob.johnson@example.com,35,UK,80000,Engineering,false,2023-03-10',
        '4,Alice,Brown,alice.brown@example.com,28,DE,70000,Sales,true,2023-04-05',
        '5,Charlie,Wilson,charlie.wilson@example.com,42,FR,90000,Management,true,2023-05-12',
        '6,Diana,Davis,diana.davis@example.com,31,AU,72000,Engineering,true,2023-06-18',
        '7,Eve,Miller,eve.miller@example.com,27,JP,68000,Marketing,false,2023-07-22',
        '8,Frank,Garcia,frank.garcia@example.com,38,IN,85000,Engineering,true,2023-08-30',
        '9,Grace,Rodriguez,grace.rodriguez@example.com,29,BR,71000,Sales,true,2023-09-14',
        '10,Henry,Martinez,henry.martinez@example.com,33,MX,77000,Engineering,true,2023-10-08',
        '11,Ivy,Anderson,ivy.anderson@example.com,26,US,66000,Marketing,false,2023-11-25',
        '12,Jack,Taylor,jack.taylor@example.com,40,CA,88000,Management,true,2023-12-03'
    ].join('\n');

    const sampleFilePath = path.join(__dirname, 'sample-data.csv');
    fs.writeFileSync(sampleFilePath, sampleData);
    console.log(`📝 Created sample CSV file: ${sampleFilePath}`);
    return sampleFilePath;
}

async function demo1_BasicSplitting() {
    console.log('\n🔹 Demo 1: Basic CSV Splitting');
    console.log('===============================');
    
    const inputFile = createSampleCSV();
    const outputDir = path.join(__dirname, 'demo1-basic');
    
    try {
        const parser = new CSVParser({
            inputFilePath: inputFile,
            outputDirectory: outputDir,
            maxRowsPerFile: 5,
            outputFormat: 'csv'
        });

        await parser.process();
        
        const files = fs.readdirSync(outputDir);
        console.log(`✅ Created ${files.length} CSV files in ${outputDir}`);
        
    } finally {
        // Cleanup
        if (fs.existsSync(outputDir)) {
            fs.rmSync(outputDir, { recursive: true });
        }
        fs.unlinkSync(inputFile);
    }
}

async function demo2_MultipleFormats() {
    console.log('\n🔹 Demo 2: Multiple Output Formats');
    console.log('===================================');
    
    const inputFile = createSampleCSV();
    const formats = ['csv', 'json', 'xml', 'tsv'];
    
    try {
        for (const format of formats) {
            const outputDir = path.join(__dirname, `demo2-${format}`);
            
            const parser = new CSVParser({
                inputFilePath: inputFile,
                outputDirectory: outputDir,
                maxRowsPerFile: 6,
                outputFormat: format,
                quiet: true
            });

            await parser.process();
            
            const files = fs.readdirSync(outputDir);
            console.log(`✅ ${format.toUpperCase()}: ${files.length} files created`);
            
            // Show sample content
            if (files.length > 0) {
                const sampleFile = path.join(outputDir, files[0]);
                const content = fs.readFileSync(sampleFile, 'utf8');
                const preview = content.split('\n').slice(0, 3).join('\n');
                console.log(`   Preview: ${preview.substring(0, 80)}...`);
            }
            
            // Cleanup
            fs.rmSync(outputDir, { recursive: true });
        }
        
    } finally {
        fs.unlinkSync(inputFile);
    }
}

async function demo3_DataTransformations() {
    console.log('\n🔹 Demo 3: Data Transformations');
    console.log('===============================');
    
    const inputFile = createSampleCSV();
    const outputDir = path.join(__dirname, 'demo3-transformations');
    
    try {
        const parser = new CSVParser({
            inputFilePath: inputFile,
            outputDirectory: outputDir,
            maxRowsPerFile: 8,
            outputFormat: 'json',
            transformations: {
                includeColumns: ['first_name', 'last_name', 'email', 'age', 'country'],
                typeConversions: {
                    age: 'number',
                    first_name: 'uppercase',
                    country: 'lowercase'
                },
                validation: {
                    email: { required: true, pattern: '.*@.*' },
                    age: { type: 'number', min: 18, max: 65 }
                }
            },
            generateStats: true
        });

        await parser.process();
        
        const files = fs.readdirSync(outputDir);
        console.log(`✅ Created ${files.length} transformed JSON files`);
        
        // Show transformed data sample
        if (files.length > 0) {
            const sampleFile = path.join(outputDir, files[0]);
            const content = fs.readFileSync(sampleFile, 'utf8');
            const firstRecord = JSON.parse(content.split('\n')[0]);
            console.log('📊 Sample transformed record:');
            console.log(JSON.stringify(firstRecord, null, 2));
        }
        
    } finally {
        // Cleanup
        if (fs.existsSync(outputDir)) {
            fs.rmSync(outputDir, { recursive: true });
        }
        fs.unlinkSync(inputFile);
    }
}

async function demo4_MultiThreading() {
    console.log('\n🔹 Demo 4: Multi-threading Performance');
    console.log('======================================');
    
    // Create a larger sample file
    const largerData = ['id,name,email,value'];
    for (let i = 1; i <= 1000; i++) {
        largerData.push(`${i},User${i},user${i}@example.com,${Math.random() * 1000}`);
    }
    
    const inputFile = path.join(__dirname, 'large-sample.csv');
    fs.writeFileSync(inputFile, largerData.join('\n'));
    
    try {
        // Single-threaded test
        console.log('🔄 Testing single-threaded processing...');
        const outputDir1 = path.join(__dirname, 'demo4-single');
        const start1 = Date.now();
        
        const parser1 = new CSVParser({
            inputFilePath: inputFile,
            outputDirectory: outputDir1,
            maxRowsPerFile: 200,
            outputFormat: 'csv',
            useMultipleProcesses: false,
            quiet: true
        });

        await parser1.process();
        const time1 = Date.now() - start1;
        console.log(`✅ Single-threaded: ${time1}ms`);
        
        // Multi-threaded test
        console.log('🔄 Testing multi-threaded processing...');
        const outputDir2 = path.join(__dirname, 'demo4-multi');
        const start2 = Date.now();
        
        const parser2 = new CSVParser({
            inputFilePath: inputFile,
            outputDirectory: outputDir2,
            maxRowsPerFile: 200,
            outputFormat: 'csv',
            useMultipleProcesses: true,
            processCount: 2,
            quiet: true
        });

        await parser2.process();
        const time2 = Date.now() - start2;
        console.log(`✅ Multi-threaded: ${time2}ms`);
        
        if (time2 < time1) {
            const improvement = ((time1 - time2) / time1 * 100).toFixed(1);
            console.log(`📈 Performance improvement: ${improvement}% faster`);
        } else {
            console.log(`📊 Multi-threading overhead: ${time2 - time1}ms (expected for small files)`);
            console.log(`💡 Multi-threading shows benefits with larger files (>10MB)`);
        }
        
        // Cleanup
        if (fs.existsSync(outputDir1)) fs.rmSync(outputDir1, { recursive: true });
        if (fs.existsSync(outputDir2)) fs.rmSync(outputDir2, { recursive: true });
        
    } finally {
        fs.unlinkSync(inputFile);
    }
}

async function demo5_ConfigurationFile() {
    console.log('\n🔹 Demo 5: Configuration File Usage');
    console.log('===================================');
    
    const inputFile = createSampleCSV();
    const outputDir = path.join(__dirname, 'demo5-config');
    const configFile = path.join(__dirname, 'config.json');
    
    try {
        // Load and modify config for demo
        const config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
        config.inputFilePath = inputFile;
        config.outputDirectory = outputDir;
        config.maxRowsPerFile = 4;
        config.useMultipleProcesses = false; // Disable for demo

        const parser = new CSVParser(config);
        await parser.process();
        
        const files = fs.readdirSync(outputDir);
        console.log(`✅ Created ${files.length} files using configuration file`);
        console.log(`📄 Output format: ${config.outputFormat.toUpperCase()}`);
        console.log(`🔧 Applied transformations: column filtering, type conversion, validation`);
        
    } finally {
        // Cleanup
        if (fs.existsSync(outputDir)) {
            fs.rmSync(outputDir, { recursive: true });
        }
        fs.unlinkSync(inputFile);
    }
}

// Main demo runner
async function runAllDemos() {
    console.log('🚀 CSV Big-to-Small File Converter v2.0 - Feature Demonstrations');
    console.log('================================================================');
    
    try {
        await demo1_BasicSplitting();
        await demo2_MultipleFormats();
        await demo3_DataTransformations();
        await demo4_MultiThreading();
        await demo5_ConfigurationFile();
        
        console.log('\n🎉 All demonstrations completed successfully!');
        console.log('\n📚 Key Features Demonstrated:');
        console.log('   ✅ Basic CSV file splitting');
        console.log('   ✅ Multiple output formats (CSV, JSON, XML, TSV)');
        console.log('   ✅ Data transformations (filtering, type conversion, validation)');
        console.log('   ✅ Multi-threading performance improvements');
        console.log('   ✅ Configuration file support');
        console.log('\n🔗 Try the CLI: node src/cli.js split sample.csv --help');
        
    } catch (error) {
        console.error('❌ Demo failed:', error.message);
        process.exit(1);
    }
}

// Run demos if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    runAllDemos();
}

export default runAllDemos;
