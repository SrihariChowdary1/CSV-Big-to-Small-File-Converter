# CSV-Big-to-Small-File-Converter

A high-performance Node.js utility for splitting large CSV files into smaller, more manageable chunks.

## Features

- 📊 Split large CSV files into smaller files with configurable row limits
- 🔄 Preserves CSV headers in each output file
- ⚡ Optimized for performance with streaming architecture
- 📈 Real-time progress reporting
- 🛠️ Configurable options for input/output paths and chunk sizes
- 🧵 Support for single-threaded processing (multi-threading planned for future releases)

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/csv-big-to-small-file-converter.git

# Navigate to the project directory
cd csv-big-to-small-file-converter

# Install dependencies
npm install
```

## Usage

```javascript
import { CSVParser } from './src/csvparser.js';

// Initialize with default options
const parser = new CSVParser({
  inputFilePath: 'path/to/your/large-file.csv',
  outputDirectory: './output',
  maxRowsPerFile: 100000  // Adjust as needed
});

// Process the file
parser.process()
  .then(() => console.log('Processing complete!'))
  .catch(err => console.error('Error processing file:', err));
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `inputFilePath` | Path to the input CSV file | 'users_202506020651.csv' |
| `outputDirectory` | Directory where split files will be saved | './split_csv_output' |
| `maxRowsPerFile` | Maximum number of rows per output file | 100000 |
| `useMultipleProcesses` | Enable multi-process mode (not fully implemented) | false |
| `processCount` | Number of worker processes (for future use) | 4 |

## Performance

The utility provides real-time performance metrics during processing:
- Total rows processed
- Processing time
- Rows processed per second

## Example Output

```
🚀 Starting Enhanced CSV Parser...
📁 Input file: users_202506020651.csv
📁 Output directory: ./split_csv_output
📊 Max rows per file: 100,000
📊 Input file size: 1,250.75 MB
📁 Created output directory: ./split_csv_output
📋 Detected 15 columns in CSV
📋 Headers: id, first_name, last_name, email, gender...
📝 Created new CSV file: split_part_1_2023-07-15.csv
⏳ Processed 10000 rows in 1.5s (6667 rows/sec)
...
✅ Completed file 1 with 100000 records
📝 Created new CSV file: split_part_2_2023-07-15.csv
...
🎉 CSV processing completed successfully!
📊 Total rows processed: 1,500,000
📊 Total files created: 15
⏱️ Total time: 225.42 seconds
⚡ Average rate: 6,654 rows/second
```

## Future Enhancements

- Implement true multi-threading support for faster processing
- Add support for different output formats
- Provide a command-line interface
- Add data transformation capabilities

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
