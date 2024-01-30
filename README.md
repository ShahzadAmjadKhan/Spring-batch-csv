**Spring batch file processor**
- Batch processor can be triggered by calling endpoint POST/GET /lscv/api/v1/lscv/launch with file path as queryparam "file"
- Endpoint GET /lscv/api/v1/lscv/{id} can be used to check the status of running batch job status
- If file is a CSV, it will launch the csv file processor otherwise it will start the Excel file processor
- For CSV; FlatFileItemReader is used to read the CSV file record & FlatFileItemWriter will write the processed/updated record in a new CSV file.
- For XLS, XLSX; StreamingXlsxItemReader of xlsx-streamer [https://github.com/monitorjbl/excel-streaming-reader] is used to avoid out of memory issues for large files
- File processor simulates calling an external endpoint to process the record 
