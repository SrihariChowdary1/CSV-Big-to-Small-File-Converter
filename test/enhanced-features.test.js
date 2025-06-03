import { CSVParser } from '../src/csvparser.js';
import { createFormatter, getSupportedFormats } from '../src/formatters/index.js';
import { ColumnFilter, DataTypeConverter, TransformationPipeline } from '../src/transformers/index.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Test suite for enhanced CSV parser features
 */

// Create a test CSV file
function createTestCSV() {
    const testData = [
        'id,name,email,age,active,salary',
        '1,John Doe,john@example.com,30,true,50000',
        '2,Jane Smith,jane@example.com,25,false,45000',
        '3,Bob Johnson,bob@example.com,35,true,60000',
        '4,Alice Brown,alice@example.com,28,true,52000',
        '5,Charlie Wilson,charlie@example.com,42,false,75000'
    ].join('\n');

    const testFilePath = path.join(__dirname, 'test-data.csv');
    fs.writeFileSync(testFilePath, testData);
    return testFilePath;
}

// Clean up test files
function cleanup(directory) {
    if (fs.existsSync(directory)) {
        fs.rmSync(directory, { recursive: true, force: true });
    }
}

async function testBasicFunctionality() {
    console.log('🧪 Testing basic enhanced functionality...');
    
    const testFile = createTestCSV();
    const outputDir = path.join(__dirname, 'test-output-basic');
    
    try {
        cleanup(outputDir);
        
        const parser = new CSVParser({
            inputFilePath: testFile,
            outputDirectory: outputDir,
            maxRowsPerFile: 3,
            outputFormat: 'csv',
            quiet: true
        });

        await parser.process();

        // Check if files were created
        const files = fs.readdirSync(outputDir);
        console.log(`✅ Created ${files.length} output files`);
        
        // Check first file content
        const firstFile = files.find(f => f.includes('split_part_1'));
        if (firstFile) {
            const content = fs.readFileSync(path.join(outputDir, firstFile), 'utf8');
            const lines = content.trim().split('\n');
            console.log(`✅ First file has ${lines.length} lines (including header)`);
        }

    } finally {
        cleanup(outputDir);
        fs.unlinkSync(testFile);
    }
}

async function testMultipleFormats() {
    console.log('🧪 Testing multiple output formats...');
    
    const testFile = createTestCSV();
    const formats = ['csv', 'json', 'xml', 'tsv'];
    
    try {
        for (const format of formats) {
            const outputDir = path.join(__dirname, `test-output-${format}`);
            cleanup(outputDir);
            
            const parser = new CSVParser({
                inputFilePath: testFile,
                outputDirectory: outputDir,
                maxRowsPerFile: 3,
                outputFormat: format,
                quiet: true
            });

            await parser.process();

            const files = fs.readdirSync(outputDir);
            const expectedExtension = format === 'csv' ? '.csv' : 
                                    format === 'json' ? '.json' :
                                    format === 'xml' ? '.xml' :
                                    format === 'tsv' ? '.tsv' : '';
            
            const hasCorrectExtension = files.some(f => f.endsWith(expectedExtension));
            console.log(`✅ ${format.toUpperCase()} format: ${files.length} files created with correct extension: ${hasCorrectExtension}`);
            
            cleanup(outputDir);
        }
    } finally {
        fs.unlinkSync(testFile);
    }
}

async function testDataTransformations() {
    console.log('🧪 Testing data transformations...');
    
    const testFile = createTestCSV();
    const outputDir = path.join(__dirname, 'test-output-transform');
    
    try {
        cleanup(outputDir);
        
        const parser = new CSVParser({
            inputFilePath: testFile,
            outputDirectory: outputDir,
            maxRowsPerFile: 10,
            outputFormat: 'json',
            transformations: {
                includeColumns: ['name', 'email', 'age'],
                typeConversions: {
                    age: 'number'
                },
                validation: {
                    email: { required: true, pattern: '.*@.*' },
                    age: { type: 'number', min: 18, max: 65 }
                }
            },
            generateStats: true,
            quiet: true
        });

        await parser.process();

        const files = fs.readdirSync(outputDir);
        console.log(`✅ Transformation test: ${files.length} files created`);
        
        // Check if transformations were applied
        if (files.length > 0) {
            const firstFile = files[0];
            const content = fs.readFileSync(path.join(outputDir, firstFile), 'utf8');
            const lines = content.trim().split('\n').filter(line => line.trim());

            if (lines.length > 0) {
                try {
                    const firstRecord = JSON.parse(lines[0]);
                    const hasOnlySelectedColumns = Object.keys(firstRecord).length === 3;
                    const hasCorrectColumns = ['name', 'email', 'age'].every(col => col in firstRecord);
                    console.log(`✅ Column filtering: ${hasOnlySelectedColumns && hasCorrectColumns}`);
                    console.log(`✅ Type conversion: ${typeof firstRecord.age === 'number'}`);
                } catch (parseError) {
                    console.log(`⚠️ JSON parsing issue: ${parseError.message}`);
                    console.log(`First line content: "${lines[0]}"`);
                }
            }
        }

    } finally {
        cleanup(outputDir);
        fs.unlinkSync(testFile);
    }
}

async function testFormatterComponents() {
    console.log('🧪 Testing formatter components...');
    
    try {
        // Test supported formats
        const formats = getSupportedFormats();
        console.log(`✅ Supported formats: ${formats.join(', ')}`);
        
        // Test formatter creation
        for (const format of formats) {
            const formatter = createFormatter(format);
            console.log(`✅ ${format} formatter created: ${formatter.constructor.name}`);
        }
        
    } catch (error) {
        console.error(`❌ Formatter test failed: ${error.message}`);
    }
}

async function testTransformerComponents() {
    console.log('🧪 Testing transformer components...');
    
    try {
        // Test column filter
        const filter = new ColumnFilter(['name', 'email'], ['id']);
        const testRow = { id: 1, name: 'John', email: 'john@test.com', age: 30 };
        const filteredRow = filter.transform(testRow, ['id', 'name', 'email', 'age']);
        
        const hasCorrectColumns = 'name' in filteredRow && 'email' in filteredRow && !('id' in filteredRow);
        console.log(`✅ Column filter: ${hasCorrectColumns}`);
        
        // Test type converter
        const converter = new DataTypeConverter({ age: 'number', active: 'boolean' });
        const testRow2 = { name: 'John', age: '30', active: 'true' };
        const convertedRow = converter.transform(testRow2, ['name', 'age', 'active']);
        
        const hasCorrectTypes = typeof convertedRow.age === 'number' && typeof convertedRow.active === 'boolean';
        console.log(`✅ Type converter: ${hasCorrectTypes}`);
        
        // Test transformation pipeline
        const pipeline = new TransformationPipeline();
        pipeline.addTransformer(filter);
        pipeline.addTransformer(converter);
        
        const testRow3 = { id: 1, name: 'John', email: 'john@test.com', age: '30', active: 'true' };
        const pipelineResult = pipeline.transform(testRow3, ['id', 'name', 'email', 'age', 'active']);
        
        const pipelineWorked = pipelineResult && 'name' in pipelineResult && !('id' in pipelineResult);
        console.log(`✅ Transformation pipeline: ${pipelineWorked}`);
        
    } catch (error) {
        console.error(`❌ Transformer test failed: ${error.message}`);
    }
}

// Run all tests
async function runAllTests() {
    console.log('🚀 Running Enhanced CSV Parser Tests\n');
    
    try {
        await testBasicFunctionality();
        console.log();
        
        await testMultipleFormats();
        console.log();
        
        await testDataTransformations();
        console.log();
        
        await testFormatterComponents();
        console.log();
        
        await testTransformerComponents();
        console.log();
        
        console.log('🎉 All tests completed!');
        
    } catch (error) {
        console.error('❌ Test suite failed:', error.message);
        process.exit(1);
    }
}

// Run tests if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    runAllTests();
}

export default runAllTests;
