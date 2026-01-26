# Healthcare Metrics Project - Technical Documentation

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Data Pipeline](#data-pipeline)
4. [Data Model](#data-model)
5. [Metrics Definitions](#metrics-definitions)
6. [Deployment Guide](#deployment-guide)
7. [Troubleshooting](#troubleshooting)

---

## 1. Project Overview

### Purpose
Analyze CMS Payroll-Based Journal (PBJ) nursing home staffing data to identify staffing patterns, benchmark compliance, and facilities with concerning metrics.

### Data Source
- **Primary**: PBJ_Daily_Nurse_Staffing_Q2_2024.csv (1,048,575 records)
- **Supporting**: 20 additional CMS datasets (provider info, quality measures, citations, etc.)

### Technology Stack
| Component | Technology |
|-----------|------------|
| Cloud Platform | AWS |
| Data Lake Storage | Amazon S3 |
| ETL Processing | AWS Glue (PySpark) |
| Data Catalog | AWS Glue Data Catalog |
| Data Warehouse | Amazon Redshift Serverless |
| Orchestration | AWS Step Functions |
| Infrastructure | AWS CloudFormation |
| Dashboard | Streamlit + Plotly |
| Languages | Python, SQL |

---

## 2. Architecture

### Medallion Architecture

The data lake follows the medallion (multi-hop) architecture:

| Layer | Purpose | Format | Location |
|-------|---------|--------|----------|
| **Bronze (Raw)** | Original data as ingested | CSV | `s3://bucket/raw/` |
| **Silver (Processed)** | Cleaned, typed, validated | Parquet | `s3://bucket/processed/` |
| **Gold (Curated)** | Business-ready facts & dimensions | Parquet | `s3://bucket/curated/` |

### AWS Resources

| Resource | Name | Purpose |
|----------|------|---------|
| S3 Bucket | `healthcare-metrics-datalake-{account_id}` | Data lake storage |
| Glue Database | `healthcare-metrics_curated` | Schema catalog |
| Glue Job 1 | `healthcare-metrics-raw-to-processed` | Bronze → Silver ETL |
| Glue Job 2 | `healthcare-metrics-processed-to-curated` | Silver → Gold ETL |
| Glue Crawler | `healthcare-metrics-curated-crawler` | Schema detection |
| Redshift Namespace | `healthcare-metrics-namespace` | Serverless namespace |
| Redshift Workgroup | `healthcare-metrics-workgroup` | Compute endpoint |
| IAM Role | `healthcare-metrics-glue-role` | Glue permissions |
| IAM Role | `healthcare-metrics-redshift-role` | Redshift S3/Glue access |

---

## 3. Data Pipeline

### Pipeline Flow

```
1. Data Ingestion (Manual/Simulated)
   └── upload_to_s3.py uploads CSV files to S3 raw layer

2. Bronze → Silver (Glue Job 1)
   ├── Read CSV from s3://bucket/raw/nursing_staffing/
   ├── Parse dates (WorkDate YYYYMMDD → DATE)
   ├── Cast columns to proper types
   ├── Calculate derived metrics (total_hours, HPPD, etc.)
   ├── Add metadata columns (processed_at, source_file)
   └── Write Parquet to s3://bucket/processed/nursing_staffing/

3. Silver → Gold (Glue Job 2)
   ├── Read Parquet from processed layer
   ├── Create dim_date (date dimension)
   ├── Create dim_provider (facility dimension)
   ├── Create dim_state (state dimension)
   ├── Create fact_daily_staffing (fact table)
   ├── Create agg_provider_monthly (aggregate)
   ├── Create agg_state_monthly (aggregate)
   └── Write to s3://bucket/curated/

4. Schema Cataloging (Glue Crawler)
   └── Crawl curated layer → Update Glue Data Catalog

5. Analytics (Redshift)
   ├── External schema links to Glue Catalog
   └── Views add calculated metrics

6. Visualization (Streamlit)
   └── Dashboard queries Redshift views
```

### ETL Transformations

#### Glue Job 1: Raw to Processed

| Transformation | Description |
|----------------|-------------|
| Date Parsing | `WorkDate` (INT 20240401) → `work_date` (DATE) |
| Type Casting | Hour columns → DOUBLE, Census → INTEGER |
| String Cleaning | TRIM, UPPERCASE for state codes |
| Calculated Fields | `total_nursing_hours`, `nursing_hppd`, `contract_ratio` |
| Partitioning | Output partitioned by `cy_qtr`, `state` |

#### Glue Job 2: Processed to Curated

| Output Table | Description |
|--------------|-------------|
| `dim_date` | Date dimension with year, quarter, month, day, is_weekend |
| `dim_provider` | Provider attributes (ID, name, city, state, county) |
| `dim_state` | State-level aggregates |
| `fact_daily_staffing` | Daily staffing facts with all measures |
| `agg_provider_monthly` | Provider-month aggregates |
| `agg_state_monthly` | State-month aggregates |

---

## 4. Data Model

### Entity Relationship Diagram

```
                    ┌─────────────────┐
                    │    dim_date     │
                    ├─────────────────┤
                    │ date_key (PK)   │
                    │ full_date       │
                    │ year            │
                    │ quarter         │
                    │ month           │
                    │ day             │
                    │ is_weekend      │
                    └────────┬────────┘
                             │
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_provider   │          │          │   dim_state     │
├─────────────────┤          │          ├─────────────────┤
│ provider_id(PK) │          │          │ state (PK)      │
│ provider_name   │          │          │ provider_count  │
│ city            │          │          │ county_count    │
│ state (FK)      │──────────┼──────────│ avg_hppd        │
│ county_name     │          │          └─────────────────┘
│ county_fips     │          │
└────────┬────────┘          │
         │                   │
         │    ┌──────────────┴──────────────┐
         │    │    fact_daily_staffing      │
         │    ├─────────────────────────────┤
         └────│ provider_id (FK)            │
              │ date_key (FK)               │────┘
              │ state (FK)                  │
              │ patient_census              │
              │ hrs_rn, hrs_lpn, hrs_cna    │
              │ total_nursing_hours         │
              │ nursing_hppd                │
              │ contract_ratio              │
              └─────────────────────────────┘
```

### Table Specifications

#### fact_daily_staffing
| Column | Type | Description |
|--------|------|-------------|
| provider_id | VARCHAR | Facility CMS ID |
| date_key | INTEGER | YYYYMMDD format |
| state | VARCHAR(2) | State code |
| patient_census | INTEGER | MDS census count |
| hrs_rn | DOUBLE | RN direct care hours |
| hrs_lpn | DOUBLE | LPN hours |
| hrs_cna | DOUBLE | CNA hours |
| total_nursing_hours | DOUBLE | Sum of RN+LPN+CNA |
| nursing_hppd | DOUBLE | Hours per patient day |
| contract_ratio | DOUBLE | Contract hours / Total |

#### dim_provider
| Column | Type | Description |
|--------|------|-------------|
| provider_id | VARCHAR | Primary key |
| provider_name | VARCHAR | Facility name |
| city | VARCHAR | City |
| state | VARCHAR(2) | State code |
| county_name | VARCHAR | County |
| county_fips | INTEGER | FIPS code |

---

## 5. Metrics Definitions

### Primary Metrics

#### Nursing Hours Per Patient Day (HPPD)
```
HPPD = (RN Hours + LPN Hours + CNA Hours) / Patient Census
```
- **Benchmark**: CMS recommends ≥ 4.1 HPPD
- **Usage**: Primary measure of staffing adequacy

#### RN Skill Mix
```
RN Skill Mix % = RN Hours / Total Nursing Hours × 100
```
- **Benchmark**: 15-25% typical for nursing homes
- **Usage**: Measures proportion of higher-skilled staff

#### Contract Staff Ratio
```
Contract Ratio % = Contract Hours / Total Hours × 100
```
- **Benchmark**: < 15% is typical; > 30% indicates high reliance
- **Usage**: Workforce stability indicator

### Staffing Level Categories

| Category | HPPD Range | Color Code |
|----------|------------|------------|
| Meets CMS Benchmark | ≥ 4.1 | Green |
| Near Benchmark | 3.5 - 4.0 | Yellow |
| Below Benchmark | 2.5 - 3.4 | Orange |
| Critical Understaffing | < 2.5 | Red |

---

## 6. Deployment Guide

### Prerequisites
- AWS Account with admin access
- AWS CLI configured (`aws configure`)
- Python 3.8+ with pip

### Deployment Steps

#### Step 1: Deploy Infrastructure
```powershell
cd aws_deployment
.\deploy.ps1
```

#### Step 2: Verify Data in Redshift
```sql
SELECT COUNT(*) FROM healthcare_lake.facts;
-- Expected: 1,048,575 records
```

#### Step 3: Create Analytics Views
```sql
-- Run all CREATE VIEW statements from metrics_setup.sql
```

#### Step 4: Run Dashboard
```powershell
cd step6_dashboard
pip install -r requirements.txt
streamlit run app.py
```

### Configuration Files

#### .streamlit/secrets.toml
```toml
[redshift]
host = "healthcare-metrics-workgroup.{account}.us-east-1.redshift-serverless.amazonaws.com"
database = "analytics"
user = "admin"
password = "YOUR_PASSWORD"
port = "5439"
```

---

## 7. Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Connection timeout | Security group blocks IP | Add inbound rule for port 5439 |
| Decimal type error | Redshift returns Decimal | Use `to_float()` helper function |
| No tables in schema | Crawler hasn't run | Run Glue Crawler |
| Glue job fails | Script location wrong | Verify S3 path in job config |
| Permission denied | IAM role missing policy | Add required permissions |

### Useful Commands

```powershell
# Check Glue job status
aws glue get-job-runs --job-name healthcare-metrics-raw-to-processed

# Check Redshift endpoint
aws redshift-serverless get-workgroup --workgroup-name healthcare-metrics-workgroup

# List S3 contents
aws s3 ls s3://healthcare-metrics-datalake-{account}/curated/ --recursive

# Check security group
aws ec2 describe-security-groups --group-ids sg-xxxxx
```

### Logs Location
- Glue Jobs: CloudWatch Logs `/aws-glue/jobs/`
- Redshift: CloudWatch Logs (if enabled)
- Streamlit: Terminal output

---

## Appendix: File Inventory

| File | Purpose |
|------|---------|
| `cloudformation_template.yaml` | AWS infrastructure definition |
| `deploy.ps1` | Windows deployment script |
| `upload_to_s3.py` | Data ingestion script |
| `glue_job_1_raw_to_processed.py` | Bronze→Silver ETL |
| `glue_job_2_processed_to_curated.py` | Silver→Gold ETL |
| `metrics_setup.sql` | Redshift analytics views |
| `app.py` | Streamlit dashboard |
| `requirements.txt` | Python dependencies |
| `secrets.toml` | Database credentials |
