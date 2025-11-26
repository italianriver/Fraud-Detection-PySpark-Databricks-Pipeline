from pyspark.sql import functions as F

cust = spark.table('fraud_detection.bronze.bank_customers_table')                   #static lookup
tx_stream = spark.readStream.table('fraud_detection.bronze.bronze_transactions')    #streaming table
silver_checkpoint_path = 'path/to/checkpoint'                                       #checkpoint path for writestream

silver_transactions_core_df = (tx_stream.join(cust, tx_stream.customer_id == cust.customer_id, how = 'left')    
        .withColumn(                                    #Adding flags for suspicious transaction behaviour
                'flag_night_time',                      #Nighttime transactions
                F.when(F.hour('timestamp')
                .between(0, 5),1)
                .otherwise(0)
                )                                       
        .withColumn(
                'flag_high_amount',                     #High amounts respective to spending level
                F.when(F.col('amount') >
                (1000*F.col('spending_level')**2),1)
                .otherwise(0)
                )
        .withColumn(
                'flag_pin_mistake',                     #Multiple pin mistakes
                F.when(F.col('pin_mistakes') > 1,1)
                .otherwise(0)
                )
        .withColumn(
                'flag_category',                        #High-risk categories
                F.when(F.col('category')
                .isin('GAMBLING', 'BLACK-MARKET', 'LUXURY'),1)
                .otherwise(0)
                )
        .withColumn(
                'flag_country',                         #Transactions outside of residing country (unless travel mode enabled!)
                F.when(F.col('fraud_detection.bronze.bronze_transactions.country')
                != ('NL'),1)
                .otherwise(0)
                ) 
        .select(
                'transaction_id',
                tx_stream.customer_id,
                'flag_night_time',
                'flag_high_amount',
                'flag_pin_mistake',
                'flag_category',
                'flag_country',
                'amount',
                'spending_level',
                'category',
                tx_stream.country,
                'pin_mistakes',
                'timestamp',
                'tx_succesful'
    ))

( 
silver_transactions_core_df.writeStream     #write stream to the silver tx core table (includes only metrics for fraud detection)
        .format('delta')
        .option('checkpointLocation', silver_checkpoint_path)
        .outputMode('append')
        .trigger(availableNow=True)
        .toTable('fraud_detection.silver.silver_transactions_core')
        )
