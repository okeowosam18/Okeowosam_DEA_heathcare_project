[README.md](https://github.com/user-attachments/files/24852004/README.md)
# Healthcare Metrics Project

## Nursing Home Staffing Analytics

A comprehensive data engineering project analyzing CMS Payroll-Based Journal (PBJ) nursing staffing data to identify staffing patterns, benchmark compliance, and facilities with concerning metrics.

---

## Project Structure

```
healthcare-metrics-project/
│
├── 01_Executive_Summary.md          # Project overview and key findings
├── 02_Technical_Documentation.md    # Architecture and implementation details
├── 03_Data_Dictionary.md            # Data definitions and schemas
├── README.md                        # This file
│
├── aws_deployment/                  # AWS Infrastructure
│   ├── cloudformation_template.yaml # CloudFormation IaC
│   ├── deploy.ps1                   # Windows deployment script
│   ├── upload_to_s3.py             # Data ingestion script
│   ├── glue_job_1_raw_to_processed.py    # Bronze→Silver ETL
│   ├── glue_job_2_processed_to_curated.py # Silver→Gold ETL
│   └── redshift_setup.sql          # Redshift views
│
├── dashboard/                       # Streamlit Dashboard
│   ├── app.py                      # Main application
│   ├── requirements.txt            # Python dependencies
│   └── .streamlit/
│       └── secrets.toml            # Database credentials
│
└── metrics/                         # Metrics Definitions
    ├── metrics_setup.sql           # SQL view definitions
    └── metrics_documentation.md    # Business documentation
```

---

## Quick Start

### Prerequisites
- AWS Account with admin access
- AWS CLI installed and configured
- Python 3.8+

### 1. Deploy AWS Infrastructure
```powershell
cd aws_deployment
.\deploy.ps1
```

### 2. Run Streamlit Dashboard
```powershell
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

### 3. Access Dashboard
Open browser to `http://localhost:8501`

---

## Key Findings

| Metric | Value |
|--------|-------|
| Total Providers | 11,523 |
| Total States | 44 |
| Total Records | 1,048,260 |
| Average HPPD | 3.37 |
| % Meeting CMS Benchmark (4.1) | 15.5% |

**Key Insight**: 84.5% of facility-days fail to meet the CMS staffing benchmark, indicating widespread staffing challenges.

---

## Architecture

```
S3 (Raw/CSV) → Glue ETL → S3 (Curated/Parquet) → Redshift → Streamlit
```

- **Medallion Architecture**: Bronze → Silver → Gold layers
- **Infrastructure as Code**: CloudFormation
- **Serverless Analytics**: Redshift Serverless

---

## Dashboard Features

1. **📊 Overview**: KPIs, HPPD distribution, staffing mix
2. **🗺️ State Analysis**: Geographic comparison map
3. **🏢 Provider Details**: Facility drill-down
4. **⚠️ Staffing Alerts**: Low-staffing facilities
5. **📈 Trends**: Time series analysis

---

## Author

**Samuel Okeowo**  
Data Engineering Student  
December 2025
