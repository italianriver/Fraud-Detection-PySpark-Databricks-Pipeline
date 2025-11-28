from pyspark.sql import functions as F

silver_stream = spark.readStream.table('fraud_detection.silver.silver_transactions_core')
gold_checkpoint = 'path/to/gold-checkpoints'

gold_tx = (silver_stream
      .withColumn(
          'flag_count',
          F.col('flag_night_time') +
          F.col('flag_high_amount') +
          F.col('flag_pin_mistake') +
          F.col('flag_category') +
          F.col('flag_country')
      )                                          
      .withColumn(
          'severity',
          F.when(F.col('flag_count') >= 3, 'high')
          .when(F.col('flag_count') == 2, 'medium')
          .when(F.col('flag_count') == 1, 'low')
          .otherwise('none')
      )
      .withColumn(
          '2FA_account_locked',
          F.when(F.col('flag_count') >= 3, True)
          .otherwise(False)
          )
      
      .filter(F.col('flag_count') > 0)
      .select(
          'severity',
          '2FA_account_locked',
          'transaction_id',
          'customer_id',
          F.col('timestamp').alias('transaction_timestamp'),
          'pin_mistakes',
          'country',
          'category',
          'amount'
      )
)

(
gold_tx.writeStream
        .format('delta')
        .outputMode('append')
        .option('checkpointLocation', gold_checkpoint)
        .trigger(availableNow=True)
        .toTable('fraud_detection.gold.gold_flagged_transactions')
)
