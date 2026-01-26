# Healthcare Metrics Project - Executive Summary

## Project Overview

This project delivers an end-to-end data pipeline and analytics dashboard for analyzing nursing home staffing patterns using CMS Payroll-Based Journal (PBJ) data from Q2 2024.

**Author:** Samuel Okeowo  
**Date:** December 2025  
**Data Source:** CMS PBJ Daily Nurse Staffing Data (Q2 2024)

---

## Business Problem

Nursing home staffing levels directly impact patient care quality, safety outcomes, and regulatory compliance. Healthcare administrators, policymakers, and regulators need visibility into:

- Are facilities meeting CMS staffing benchmarks?
- Which states/facilities show concerning staffing patterns?
- What is the reliance on contract vs. employed staff?
- How do staffing levels vary over time?

---

## Solution Delivered

### Data Pipeline (AWS)

A production-ready data pipeline built on AWS services:

```
Google Drive → Lambda → S3 (Raw) → Glue ETL → S3 (Curated) → Redshift → Streamlit
```

**AWS Services Used:**
- **S3**: Data lake with medallion architecture (Bronze/Silver/Gold)
- **AWS Glue**: ETL jobs for data transformation
- **Glue Data Catalog**: Schema management
- **Redshift Serverless**: Data warehouse for analytics
- **CloudFormation**: Infrastructure as Code

### Analytics Dashboard (Streamlit)

An interactive dashboard with 5 pages:
1. **Executive Summary**: KPIs, HPPD distribution, staffing mix
2. **State Analysis**: Geographic comparison, rankings
3. **Provider Details**: Facility-level drill-down
4. **Staffing Alerts**: Facilities with concerning metrics
5. **Trends**: Time series analysis

---

## Key Findings

| Metric | Value | Insight |
|--------|-------|---------|
| **Total Providers** | 11,523 | Comprehensive national coverage |
| **Total Records** | 1,048,260 | 90 days × ~11.5K facilities |
| **Average HPPD** | 3.37 | **18% below** CMS benchmark of 4.1 |
| **% Meeting Benchmark** | 15.5% | Only 1 in 6 facility-days meet standard |
| **RN Skill Mix** | 13.3% | Within typical range (15-25%) |

### Critical Insight
**84.5% of facility-days fail to meet the CMS staffing benchmark**, indicating a systemic staffing challenge across US nursing homes.

---

## Technical Deliverables

| Step | Deliverable | Status |
|------|-------------|--------|
| 1 | Data Acquisition (1M+ records) | ✅ Complete |
| 2 | Data Profiling & Analysis | ✅ Complete |
| 3 | Pipeline Architecture Design | ✅ Complete |
| 4 | AWS Pipeline Implementation | ✅ Complete |
| 5 | Metrics Definition | ✅ Complete |
| 6 | Streamlit Dashboard | ✅ Complete |
| 7 | Final Submission | ✅ Complete |

---

## Data Model

### Dimensional Model (Gold Layer)

**Fact Table:**
- `fact_daily_staffing`: 1M+ daily staffing records with hours, census, HPPD

**Dimension Tables:**
- `dim_provider`: 11,523 nursing facilities
- `dim_date`: Date attributes
- `dim_state`: 44 states with aggregates

**Aggregate Tables:**
- `agg_provider_monthly`: Provider-month summaries
- `agg_state_monthly`: State-month summaries
- `agg_overall_metrics`: Dashboard KPIs

---

## Metrics Defined

| Metric | Formula | Benchmark |
|--------|---------|-----------|
| **Nursing HPPD** | (RN + LPN + CNA hours) / Census | ≥ 4.1 |
| **RN HPPD** | RN hours / Census | ≥ 0.75 |
| **RN Skill Mix** | RN hours / Total nursing hours | 15-25% |
| **Contract Ratio** | Contract hours / Total hours | < 15% |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              AWS CLOUD                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐               │
│  │  S3 BUCKET   │    │  S3 BUCKET   │    │  S3 BUCKET   │               │
│  │    (Raw)     │───▶│ (Processed)  │───▶│  (Curated)   │               │
│  │   Bronze     │    │    Silver    │    │     Gold     │               │
│  │    CSV       │    │   Parquet    │    │  Facts/Dims  │               │
│  └──────────────┘    └──────────────┘    └──────────────┘               │
│         │                   │                   │                        │
│         │                   │                   │                        │
│         ▼                   ▼                   ▼                        │
│  ┌──────────────────────────────────────────────────────┐               │
│  │                    AWS GLUE                          │               │
│  │  • ETL Job 1: Raw → Processed (PySpark)              │               │
│  │  • ETL Job 2: Processed → Curated (PySpark)          │               │
│  │  • Crawler: Schema detection                          │               │
│  └──────────────────────────────────────────────────────┘               │
│                                │                                         │
│                                ▼                                         │
│                    ┌──────────────────────┐                             │
│                    │   GLUE DATA CATALOG  │                             │
│                    │   (Schema Registry)  │                             │
│                    └──────────────────────┘                             │
│                                │                                         │
│                                ▼                                         │
│                    ┌──────────────────────┐                             │
│                    │ REDSHIFT SERVERLESS  │                             │
│                    │  (Data Warehouse)    │                             │
│                    │  • External Schema   │                             │
│                    │  • Analytics Views   │                             │
│                    └──────────────────────┘                             │
│                                │                                         │
└────────────────────────────────┼─────────────────────────────────────────┘
                                 │
                                 ▼
                    ┌──────────────────────┐
                    │  STREAMLIT DASHBOARD │
                    │   (localhost:8501)   │
                    └──────────────────────┘
```

---

## Cost Analysis

| Service | Monthly Cost |
|---------|--------------|
| S3 Storage (~5 GB) | $0.12 |
| Glue Jobs (4 runs) | $5.00 |
| Redshift Serverless | $32.00* |
| **Total** | **~$37/month** |

*Redshift scales to zero when not in use

---

## Future Enhancements

1. **Automated Ingestion**: Implement Google Drive API integration for automatic quarterly updates
2. **Predictive Analytics**: ML models to predict staffing shortages
3. **Alerting System**: SNS notifications for critical staffing events
4. **Additional Data Sources**: Integrate quality ratings, inspection data
5. **Cloud Deployment**: Deploy Streamlit to EC2 or Streamlit Cloud

---

## Conclusion

This project demonstrates a complete modern data engineering solution:

- **Scalable Architecture**: AWS services handle growth from 1M to 100M+ records
- **Medallion Pattern**: Clean separation of raw, processed, and curated data
- **Infrastructure as Code**: CloudFormation enables reproducible deployments
- **Self-Service Analytics**: Streamlit dashboard empowers non-technical users

The analysis reveals that **staffing challenges are widespread** across US nursing homes, with only 15.5% of facility-days meeting CMS benchmarks - a finding with significant implications for healthcare policy and patient safety.
