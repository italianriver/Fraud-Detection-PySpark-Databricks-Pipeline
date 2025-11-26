from pyspark.sql import functions as F

#Spark structured streaming ingestion, from transactions json source → bronze transactions delta table

input_path = '/Workspace/Users/v.tibor+dbx1@gmail.com/fraud_detection/source/generated_transactions' #transaction source path
output_path = 'fraud_detection.bronze.bronze_transactions'                                           #output table path
checkpoint_path = '/Volumes/fraud_detection/bronze/bronze_checkpoints'                               #checkpoints are saved here
schema_path = '/Workspace/Users/v.tibor+dbx1@gmail.com/fraud_detection/bronze/tx_ingestion_schemas'  #schema location

df = (spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'json')
    .option("cloudFiles.inferColumnTypes", "true")
    .option('cloudFiles.schemaLocation', schema_path)
    .option('multiLine', False)
    .load(input_path)
    )

df = df.withColumn('ingestion_timestamp',
     F.date_format(F.current_timestamp(),
     'yyyy-MM-dd HH:mm:ss')
    )


(df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(output_path)
    )
