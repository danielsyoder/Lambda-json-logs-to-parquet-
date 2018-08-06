# Lambda-json-logs-to-parquet-
Lambda function that ingests json.gz logs (composed of json on single lines) and transforms to parquet.

To package as a lambda function requires stripping extraneous dependencies and tests from packages prior to uploading zip to S3.

Required packages include: Fastparquet, pandas, numpy, numba, boto3, funcsigs, enum, llvmlite, pytz

It is capable of handling more than one record file at a time. 

Designed for: S3 <new log event> --> SQS --> Lambda --> S3 parquet
