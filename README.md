# Event-Driven Data Lake with Medallion Architecture on AWS

This project implements a scalable and modular **event-driven data processing pipeline** using AWS services like **Glue, Lambda, EventBridge, Step Functions, and SNS**. It follows the **Medallion Architecture** (Bronze, Silver, and Gold layers) and automates infrastructure provisioning and deployment through **Boto3** and **GitHub Actions CI/CD** workflows.

---

## 📁 Project Structure

<pre><code>.
├── .github/workflows/                 # CI/CD workflows (e.g., deploy.yaml)
├── glue_jobs/                         # Standalone job scripts
│   ├── bootstrap/                     # Iceberg table migration logic
│   ├── bronze/                        # Raw ingestion Glue jobs
│   ├── silver/                        # Dimension & fact ETL jobs
│   └── gold/                          # Summary and reporting jobs
├── iceberg_schema/                   # YAMLs for Iceberg table creation (like migrations)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── infrastructure/                   # AWS Infra automation scripts (Glue, Lambda, EventBridge)
├── src/                              
│   ├── glue_etl/                      # Python package for ETL job logic
│   │   ├── bootstrap/
│   │   ├── bronze/
│   │   ├── silver/
│   │   ├── gold/
│   │   └── helpers/                   # Logging, constants, ingestion utils
│   ├── lambdas/                       # Lambda functions
│   │   ├── glue_failed_job/
│   │   └── unzip_and_upload_to_bronze/
│   ├── eventbridge/                  # Rule and target config for EventBridge
│   └── step_functions/              # State machine JSON for orchestration
├── setup.py                          # Python packaging configuration
├── requirements.txt                  # Python dependencies
├── README.md                         # Project overview
└── folder structure.txt              # Project structure summary
</code></pre>

## 🧱 Architecture
## Medallion Data Lake Zones
<pre><code>
Bronze: Ingest raw CSV files from S3. Lambda unzips & uploads → EventBridge triggers → Glue ETL Bronze jobs.
Silver: Cleaned & transformed data, separated into dimensions and fact tables (with SCD Type 3 logic).
Gold: Aggregated reports (e.g., summary by country/supplier/month).
</code></pre>
## Technologies Used
<pre><code>
AWS Glue for scalable ETL processing.
AWS Lambda for preprocessing (e.g., unzip and metadata registration).
Amazon EventBridge for event-driven orchestration.
AWS Step Functions for coordinating multi-step ETL workflows.
Amazon SNS for job failure notifications.
Apache Iceberg for table versioning, schema evolution, and ACID compliance.
GitHub Actions for CI/CD pipelines (testing, packaging as wheel, deploying ETL jobs and infra).
</code></pre>
## 🚀 CI/CD Workflow (GitHub Actions)
<pre><code>
Builds ETL code into a wheel and uploads to S3.
Deploys infrastructure (Glue jobs, Step Functions, Lambda, EventBridge) via Boto3 scripts.
Uses matrix strategy to deploy multiple Glue jobs and track schema migrations with versioned YAMLs.
Supports skipping specific job deployments using commit message tags like [cli skip].
</code></pre>
## 🛠️ Key Features
<pre><code>
Event-Driven: Fully reactive to data file arrival.
Modular ETL Code: Reusable, testable logic in Python package (src/glue_etl/).
Schema Migration: Version-controlled YAML files for Iceberg tables.
Observability: Glue job failure handling via Lambda + SNS.
SCD Support: Silver zone includes Slowly Changing Dimensions (Type 2).
Gold Reporting: Daily/monthly summaries for business insights.
</code></pre>
## 📊 Sample Data Domains
<pre><code>
Customers
Products
Suppliers
Warehouses
Sales Orders
Each domain has corresponding ingestion (Bronze), transformation (Silver), and reporting (Gold) scripts with Iceberg tables.
</code></pre>
## 🧩 Folder-Specific Highlights
<pre><code>
Folder                   | Purpose
glue_jobs/               | Source ETL scripts (for dev/debug)
src/glue_etl/            | Modular Python code for packaging & deployment
iceberg_schema/          | Migration-like Iceberg YAMLs per layer
infrastructure/          | Boto3 scripts to automate AWS infra
src/lambdas/             | Lambda functions for file processing & monitoring
src/eventbridge/         | EventBridge rule and target configuration
src/step_functions/      | State machine definitions
</code></pre>

## 🔁 Future Enhancements
<pre><code>
Add unit tests and mocking for ETL scripts
Include data quality checks using Deequ or Great Expectations
Integrate with Amazon Athena and QuickSight for visualization
Add rollback support in case of schema migration failure
</code></pre>
## 🧑‍💻 Author
Dipak Vaidya
Senior Software Engineer | Cloud & Data Engineering Enthusiast
<a href="https://bit.ly/2Se2UE7">LinkedIn Profile</a> | <a href="https://bit.ly/2ZaNzWp">GitHub</a>
