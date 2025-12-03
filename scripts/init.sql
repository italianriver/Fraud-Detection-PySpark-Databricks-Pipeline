%sql
  
-- This script prevents schema and checkpoint inconsistencies 
-- Creates clean environment for fraud detection pipeline
-- WARNING: this will delete previously saved schemas and streaming checkpoints
  
CREATE CATALOG IF NOT EXISTS fraud_detection;

--Bronze environment init
CREATE SCHEMA IF NOT EXISTS fraud_detection.bronze;
DROP VOLUME IF EXISTS fraud_detection.bronze_checkpoints;
CREATE VOLUME IF NOT EXISTS bronze_checkpoints;
DROP TABLE IF EXISTS fraud_detection.bronze.bronze_transactions;

-- Silver environment init
CREATE SCHEMA IF NOT EXISTS fraud_detection.silver;
DROP VOLUME IF EXISTS fraud_detection.silver_checkpoints;
CREATE VOLUME IF NOT EXISTS fraud_detection.silver_checkpoints;
DROP TABLE IF EXISTS fraud_detection.silver.silver_transactions_core

-- Gold environment init
CREATE SCHEMA IF NOT EXISTS fraud_detection.gold;
DROP VOLUME IF EXISTS fraud_detection.gold.gold_checkpoints;
CREATE VOLUME IF NOT EXISTS fraud_detection.gold.gold_checkpoints;
DROP TABLE IF EXISTS fraud_detection.gold.gold_flagged_transactions;
