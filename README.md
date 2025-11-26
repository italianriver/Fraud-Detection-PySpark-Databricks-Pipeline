# Real-Time Fraud Detection Pipeline

## Welcome  
This project demonstrates a real-time fraud detection system built in the Databricks environment using PySpark Structured Streaming and Delta Lake. It generates synthetic customers and a continuous stream of transactions, and processes them through a Lakehouse architecture designed to identify suspicious activity. The primary objective is to determine whether a transaction is risky enough to temporarily lock the associated account and require the customer to unlock it through a 2FA verification step.

## Project architecture  
The pipeline begins with two Python generators: one creates a synthetic customer base and the other produces an ongoing stream of transactions. 

These are ingested into the Bronze layer using Databricks Auto Loader, with customer data stored as a static Delta table and all incoming transactions captured via a streaming read. An ingestion timestamp is added to maintain auditability.

The Silver layer reads the Bronze transaction table as a streaming DataFrame and enriches the live stream by joining it with the static customer lookup table. All fraud-related metrics are computed within this PySpark streaming transformation, including night-time usage, abnormal amounts, pin-code mistakes, category anomalies and country mismatches. Only columns relevant for detection are retained, resulting in an efficient real-time fraud feature model.

The Gold layer aggregates the Silver metrics into a unified severity score. This score determines whether the system would trigger an internal 2FA challenge and lock the account until the customer confirms the activity.  

The Consumption layer uses a Databricks dashboard to display incoming suspicious transactions in near real time. The dashboard visualises severity levels and all behavioural indicators that contributed to each decision.

## Project requirements  
This project requires PySpark Structured Streaming, Databricks Auto Loader for ingestion, Delta tables for storage, and a continuous Bronze → Silver → Gold transformation flow. Customer and transaction data must be generated synthetically, enriched in real time and evaluated with explainable fraud metrics. The system focuses on operational freshness rather than historisation and provides clear documentation for analytics and FEC teams.
