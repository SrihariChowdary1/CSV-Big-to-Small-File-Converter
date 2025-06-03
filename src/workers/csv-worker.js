import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import fs from 'fs';
import csv from 'csv-parser';
import path from 'path';

/**
 * CSV Worker for multi-threaded processing
 * This worker handles processing chunks of CSV data in parallel
 */

if (!isMainThread) {
    // Worker thread code
    const { 
        inputFilePath, 
        outputDirectory, 
        startByte, 
        endByte, 
        chunkIndex, 
        headers,
        maxRowsPerFile,
        outputFormat,
        transformations
    } = workerData;

    async function processChunk() {
        try {
            const results = {
                chunkIndex,
                rowsProcessed: 0,
                filesCreated: [],
                errors: []
            };

            // Create a read stream for the specific byte range
            const readStream = fs.createReadStream(inputFilePath, {
                start: startByte,
                end: endByte,
                encoding: 'utf8'
            });

            let buffer = '';
            let currentFileIndex = 1;
            let currentRowCount = 0;
            let currentOutputStream = null;
            let isFirstChunk = startByte === 0;

            // Helper function to create new output file
            function createNewOutputStream() {
                if (currentOutputStream) {
                    currentOutputStream.end();
                }

                const timestamp = new Date().toISOString().split('T')[0];
                const outputFileName = `split_part_${chunkIndex}_${currentFileIndex}_${timestamp}.csv`;
                const outputFilePath = path.join(outputDirectory, outputFileName);

                currentOutputStream = fs.createWriteStream(outputFilePath, { encoding: 'utf8' });
                
                // Write headers if it's CSV format
                if (outputFormat === 'csv' && headers.length > 0) {
                    currentOutputStream.write(headers.join(',') + '\n');
                }

                results.filesCreated.push(outputFileName);
                currentRowCount = 0;
                currentFileIndex++;
            }

            // Helper function to escape CSV values
            function escapeCSVValue(value) {
                if (value === null || value === undefined) return '';
                const str = String(value);
                if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
                    return `"${str.replace(/"/g, '""')}"`;
                }
                return str;
            }

            // Helper function to apply transformations
            function applyTransformations(row) {
                let transformedRow = { ...row };
                
                if (transformations) {
                    // Apply column filtering
                    if (transformations.includeColumns) {
                        const filtered = {};
                        transformations.includeColumns.forEach(col => {
                            if (transformedRow[col] !== undefined) {
                                filtered[col] = transformedRow[col];
                            }
                        });
                        transformedRow = filtered;
                    }

                    // Apply data type conversions
                    if (transformations.typeConversions) {
                        Object.entries(transformations.typeConversions).forEach(([column, type]) => {
                            if (transformedRow[column] !== undefined) {
                                switch (type) {
                                    case 'number':
                                        transformedRow[column] = Number(transformedRow[column]) || 0;
                                        break;
                                    case 'boolean':
                                        transformedRow[column] = Boolean(transformedRow[column]);
                                        break;
                                    case 'date':
                                        transformedRow[column] = new Date(transformedRow[column]).toISOString();
                                        break;
                                    case 'uppercase':
                                        transformedRow[column] = String(transformedRow[column]).toUpperCase();
                                        break;
                                    case 'lowercase':
                                        transformedRow[column] = String(transformedRow[column]).toLowerCase();
                                        break;
                                }
                            }
                        });
                    }

                    // Apply custom validation
                    if (transformations.validation) {
                        // Skip rows that don't meet validation criteria
                        for (const [column, rule] of Object.entries(transformations.validation)) {
                            const value = transformedRow[column];
                            if (rule.required && (!value || value.toString().trim() === '')) {
                                return null; // Skip this row
                            }
                            if (rule.minLength && value && value.toString().length < rule.minLength) {
                                return null; // Skip this row
                            }
                            if (rule.pattern && value && !new RegExp(rule.pattern).test(value.toString())) {
                                return null; // Skip this row
                            }
                        }
                    }
                }

                return transformedRow;
            }

            // Process the chunk
            return new Promise((resolve, reject) => {
                readStream.on('data', (chunk) => {
                    buffer += chunk;
                });

                readStream.on('end', () => {
                    // Process the buffered data
                    const lines = buffer.split('\n');
                    
                    // Skip header if not first chunk
                    const startIndex = isFirstChunk ? 1 : 0;
                    
                    createNewOutputStream();

                    for (let i = startIndex; i < lines.length; i++) {
                        const line = lines[i].trim();
                        if (!line) continue;

                        try {
                            // Parse CSV line manually (simple approach)
                            const values = line.split(',').map(val => val.replace(/^"|"$/g, ''));
                            const row = {};
                            headers.forEach((header, index) => {
                                row[header] = values[index] || '';
                            });

                            // Apply transformations
                            const transformedRow = applyTransformations(row);
                            if (!transformedRow) continue; // Skip invalid rows

                            // Write to output based on format
                            if (outputFormat === 'csv') {
                                const rowValues = headers.map(header => escapeCSVValue(transformedRow[header] || ''));
                                currentOutputStream.write(rowValues.join(',') + '\n');
                            } else if (outputFormat === 'json') {
                                currentOutputStream.write(JSON.stringify(transformedRow) + '\n');
                            }

                            currentRowCount++;
                            results.rowsProcessed++;

                            // Create new file if current file is full
                            if (currentRowCount >= maxRowsPerFile) {
                                createNewOutputStream();
                            }

                        } catch (error) {
                            results.errors.push(`Error processing line ${i}: ${error.message}`);
                        }
                    }

                    if (currentOutputStream) {
                        currentOutputStream.end();
                    }

                    resolve(results);
                });

                readStream.on('error', reject);
            });

        } catch (error) {
            throw new Error(`Worker ${chunkIndex} failed: ${error.message}`);
        }
    }

    // Execute the chunk processing
    processChunk()
        .then(results => {
            parentPort.postMessage({ success: true, results });
        })
        .catch(error => {
            parentPort.postMessage({ success: false, error: error.message });
        });
}

export class WorkerPool {
    constructor(maxWorkers = 4) {
        this.maxWorkers = maxWorkers;
        this.workers = [];
        this.activeJobs = 0;
    }

    async processChunks(chunks, workerData) {
        const results = [];
        const promises = [];

        for (let i = 0; i < chunks.length; i++) {
            const chunk = chunks[i];
            const promise = this.runWorker({
                ...workerData,
                ...chunk,
                chunkIndex: i
            });
            promises.push(promise);

            // Limit concurrent workers
            if (promises.length >= this.maxWorkers) {
                const completed = await Promise.race(promises);
                results.push(completed);
                promises.splice(promises.findIndex(p => p === completed), 1);
            }
        }

        // Wait for remaining workers to complete
        const remaining = await Promise.all(promises);
        results.push(...remaining);

        return results;
    }

    runWorker(data) {
        return new Promise((resolve, reject) => {
            const worker = new Worker(new URL(import.meta.url), {
                workerData: data
            });

            worker.on('message', (message) => {
                if (message.success) {
                    resolve(message.results);
                } else {
                    reject(new Error(message.error));
                }
                worker.terminate();
            });

            worker.on('error', reject);
            worker.on('exit', (code) => {
                if (code !== 0) {
                    reject(new Error(`Worker stopped with exit code ${code}`));
                }
            });
        });
    }

    async terminate() {
        await Promise.all(this.workers.map(worker => worker.terminate()));
        this.workers = [];
    }
}

export default WorkerPool;
