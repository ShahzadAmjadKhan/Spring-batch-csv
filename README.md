Spring batch CSV processor
Batch processor can be triggered by calling endpoint POST/GET /lscv/api/v1/lscv/launch with file path as queryparam "file"
File processor simulates calling an external endpoint to process the record
FlatFileItemReader is used to read the CSV file record
FlatFileItemWriter will write the processed/updated record in a new CSV file. 

Endpoint GET /lscv/api/v1/lscv/{id} can be used to check the status of running batch job status
