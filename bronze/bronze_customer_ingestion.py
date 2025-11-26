#Spark static customer ingestion → customer delta table

customer_path = ''              #customer source path
output_path = 'fraud_detection.bronze.bank_customers_table'
df = (spark.read
      .format('json')
      .option('multiLine', False)
      .load(customer_path)
)

(
      df.write
      .format('delta')
      .mode('overwrite')
      .saveAsTable(output_path)
)
