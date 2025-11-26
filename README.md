# Real-Time Fraud Detection Pipeline

## Welcome  
This project showcases a real-time fraud detection system built in Databricks using PySpark Structured Streaming and Delta Lake. It simulates a banking environment by generating synthetic customers and continuous transaction streams, then processes them through a Medallion (Bronze–Silver–Gold) architecture to detect suspicious behaviour and determine when an account should be temporarily locked until the user verifies it via 2FA.

---

## Project architecture

### Source layer  
Two custom Python generators are used:  
- A **customer generator** that produces a synthetic Dutch customer base with demographic and behavioural attributes  
- A **continuous transaction generator** that emits realistic banking transactions in near real time  

Both datasets serve as inputs to the streaming pipeline.

### Bronze layer  
Implemented using **Databricks Auto Loader** with PySpark Structured Streaming.  
- Customer dataset is loaded once into a **static Delta table**  
- Transactions are streamed into a **Delta table** with automatic schema inference  
- An **ingestion_timestamp** column is added for auditability  
- All raw data is preserved as the source of truth  

### Silver layer  
Real-time enrichment and fraud feature engineering implemented with **PySpark Structured Streaming**.  
- Streaming transaction table is joined with the static customer dimension  
- Behavioural flags are computed, including:  
  - night-time activity  
  - high-value anomalies  
  - repeated pin-code mistakes  
  - high-risk categories  
  - country mismatch  
- The output is a clean metrics-focused Silver table containing only fraud-relevant columns  

### Gold layer  
A scoring layer that determines the severity of the suspicious behaviour.  
- Flags are summed and mapped to a severity level (1, 2 or 3)  
- High-severity events represent cases where the bank would:  
  - temporarily lock the account  
  - require the customer to unlock it using **2FA**  
- This layer produces the table consumed by dashboards and analyst workflows  

### Consumption layer  
A real-time dashboard built with **Databricks SQL**.  
- Displays suspicious transactions as they occur  
- Shows severity levels and contributing detection signals  
- Provides an operational view suitable for FEC teams  

---

## Project requirements

### Objective  
Build a real-time fraud detection system capable of identifying suspicious transactions and determining whether customer accounts should be temporarily locked pending 2FA verification.

### Specifications  
- Generate synthetic customer and transaction data using Python  
- Ingest static and streaming data into **Delta Lake** using Databricks Auto Loader  
- Implement real-time data processing with **PySpark Structured Streaming**  
- Perform enrichment and fraud feature engineering in the Silver layer  
- Compute severity scoring in the Gold layer to support account-lock decisions  
- Provide a near real-time dashboard for analysts using Databricks SQL  
- Focus on operational freshness rather than historical warehousing  
- Maintain clear documentation suitable for analytics and Financial Economic Crime teams
