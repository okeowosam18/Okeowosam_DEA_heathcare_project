# Healthcare Metrics Project - Data Dictionary

## Source Data

### Primary Dataset: PBJ_Daily_Nurse_Staffing_Q2_2024.csv

| Column | Data Type | Description | Example |
|--------|-----------|-------------|---------|
| PROVNUM | VARCHAR | CMS Provider Number (unique facility ID) | 015001 |
| PROVNAME | VARCHAR | Provider/Facility Name | CROWNE HEALTH CARE OF GREENVILLE |
| CITY | VARCHAR | City location | GREENVILLE |
| STATE | VARCHAR(2) | State abbreviation | AL |
| COUNTY_NAME | VARCHAR | County name | BUTLER |
| COUNTY_FIPS | INTEGER | Federal county code | 1013 |
| CY_Qtr | VARCHAR | Calendar year quarter | 2024Q2 |
| WorkDate | INTEGER | Work date (YYYYMMDD format) | 20240401 |
| MDScensus | INTEGER | MDS patient census count | 85 |
| Hrs_RNDON | DECIMAL | RN Director of Nursing hours | 8.0 |
| Hrs_RNDON_emp | DECIMAL | RN DON hours (employed) | 8.0 |
| Hrs_RNDON_ctr | DECIMAL | RN DON hours (contract) | 0.0 |
| Hrs_RNadmin | DECIMAL | RN Administrative hours | 8.0 |
| Hrs_RNadmin_emp | DECIMAL | RN Admin hours (employed) | 8.0 |
| Hrs_RNadmin_ctr | DECIMAL | RN Admin hours (contract) | 0.0 |
| Hrs_RN | DECIMAL | RN direct care hours | 24.5 |
| Hrs_RN_emp | DECIMAL | RN direct care (employed) | 16.0 |
| Hrs_RN_ctr | DECIMAL | RN direct care (contract) | 8.5 |
| Hrs_LPNadmin | DECIMAL | LPN Administrative hours | 0.0 |
| Hrs_LPNadmin_emp | DECIMAL | LPN Admin (employed) | 0.0 |
| Hrs_LPNadmin_ctr | DECIMAL | LPN Admin (contract) | 0.0 |
| Hrs_LPN | DECIMAL | LPN direct care hours | 32.0 |
| Hrs_LPN_emp | DECIMAL | LPN direct care (employed) | 24.0 |
| Hrs_LPN_ctr | DECIMAL | LPN direct care (contract) | 8.0 |
| Hrs_CNA | DECIMAL | CNA direct care hours | 120.0 |
| Hrs_CNA_emp | DECIMAL | CNA direct care (employed) | 96.0 |
| Hrs_CNA_ctr | DECIMAL | CNA direct care (contract) | 24.0 |
| Hrs_NAtrn | DECIMAL | Nurse Aide trainee hours | 0.0 |
| Hrs_NAtrn_emp | DECIMAL | NA trainee (employed) | 0.0 |
| Hrs_NAtrn_ctr | DECIMAL | NA trainee (contract) | 0.0 |
| Hrs_MedAide | DECIMAL | Medication Aide hours | 8.0 |
| Hrs_MedAide_emp | DECIMAL | Med Aide (employed) | 8.0 |
| Hrs_MedAide_ctr | DECIMAL | Med Aide (contract) | 0.0 |

---

## Processed Data (Silver Layer)

### nursing_staffing (Parquet)

All source columns plus:

| Column | Data Type | Description | Calculation |
|--------|-----------|-------------|-------------|
| work_date | DATE | Parsed work date | TO_DATE(WorkDate, 'YYYYMMDD') |
| work_year | INTEGER | Year extracted | YEAR(work_date) |
| work_month | INTEGER | Month extracted | MONTH(work_date) |
| work_day_of_week | INTEGER | Day of week (1=Sun) | DAYOFWEEK(work_date) |
| total_nursing_hours | DECIMAL | Total nursing hours | hrs_rn + hrs_lpn + hrs_cna |
| total_employed_hours | DECIMAL | Total employed hours | hrs_rn_emp + hrs_lpn_emp + hrs_cna_emp |
| total_contract_hours | DECIMAL | Total contract hours | hrs_rn_ctr + hrs_lpn_ctr + hrs_cna_ctr |
| nursing_hppd | DECIMAL | Hours per patient day | total_nursing_hours / mds_census |
| contract_ratio | DECIMAL | Contract staff ratio | total_contract_hours / total_nursing_hours |
| processed_at | TIMESTAMP | Processing timestamp | CURRENT_TIMESTAMP() |
| source_file | VARCHAR | Source file path | INPUT_FILE_NAME() |

**Partitioning**: `cy_qtr`, `state`

---

## Curated Data (Gold Layer)

### dim_date

| Column | Data Type | Description | Example |
|--------|-----------|-------------|---------|
| date_key | INTEGER | Primary key (YYYYMMDD) | 20240401 |
| full_date | DATE | Full date | 2024-04-01 |
| year | INTEGER | Year | 2024 |
| quarter | INTEGER | Quarter (1-4) | 2 |
| month | INTEGER | Month (1-12) | 4 |
| day | INTEGER | Day of month | 1 |
| week_of_year | INTEGER | Week number | 14 |
| day_of_week | INTEGER | Day of week (1=Sun) | 2 |
| day_name | VARCHAR | Day name | Monday |
| is_weekend | BOOLEAN | Weekend flag | FALSE |
| year_month | VARCHAR | Year-Month | 2024-04 |
| year_quarter | VARCHAR | Year-Quarter | 2024Q2 |

---

### dim_provider

| Column | Data Type | Description | Example |
|--------|-----------|-------------|---------|
| provider_sk | BIGINT | Surrogate key | 1 |
| provider_id | VARCHAR | CMS Provider ID (NK) | 015001 |
| provider_name | VARCHAR | Facility name | CROWNE HEALTH CARE |
| city | VARCHAR | City | GREENVILLE |
| state | VARCHAR(2) | State code | AL |
| county_name | VARCHAR | County | BUTLER |
| county_fips | INTEGER | FIPS code | 1013 |

---

### dim_state

| Column | Data Type | Description | Example |
|--------|-----------|-------------|---------|
| state_sk | BIGINT | Surrogate key | 1 |
| state | VARCHAR(2) | State code (NK) | TX |
| provider_count | INTEGER | Number of facilities | 1,234 |
| county_count | INTEGER | Number of counties | 254 |
| city_count | INTEGER | Number of cities | 456 |
| total_patient_days | BIGINT | Total census | 5,678,901 |
| avg_hppd | DECIMAL | Average HPPD | 3.45 |

---

### fact_daily_staffing

| Column | Data Type | Description |
|--------|-----------|-------------|
| provider_id | VARCHAR | FK to dim_provider |
| date_key | INTEGER | FK to dim_date |
| state | VARCHAR(2) | FK to dim_state |
| cy_qtr | VARCHAR | Calendar quarter |
| work_date | DATE | Work date |
| patient_census | INTEGER | Daily patient count |
| hrs_rn | DECIMAL | RN direct care hours |
| hrs_rn_emp | DECIMAL | RN hours (employed) |
| hrs_rn_ctr | DECIMAL | RN hours (contract) |
| hrs_lpn | DECIMAL | LPN direct care hours |
| hrs_lpn_emp | DECIMAL | LPN hours (employed) |
| hrs_lpn_ctr | DECIMAL | LPN hours (contract) |
| hrs_cna | DECIMAL | CNA direct care hours |
| hrs_cna_emp | DECIMAL | CNA hours (employed) |
| hrs_cna_ctr | DECIMAL | CNA hours (contract) |
| hrs_rndon | DECIMAL | RN DON hours |
| hrs_rnadmin | DECIMAL | RN admin hours |
| hrs_lpnadmin | DECIMAL | LPN admin hours |
| hrs_natrn | DECIMAL | Nurse aide trainee hours |
| hrs_medaide | DECIMAL | Medication aide hours |
| total_nursing_hours | DECIMAL | Total nursing hours |
| total_employed_hours | DECIMAL | Total employed hours |
| total_contract_hours | DECIMAL | Total contract hours |
| nursing_hppd | DECIMAL | Hours per patient day |
| contract_ratio | DECIMAL | Contract staff ratio |
| processed_at | TIMESTAMP | ETL timestamp |

**Partitioning**: `state`

---

### agg_provider_monthly

| Column | Data Type | Description |
|--------|-----------|-------------|
| provider_id | VARCHAR | Provider ID |
| provider_name | VARCHAR | Facility name |
| state | VARCHAR(2) | State code |
| year | INTEGER | Year |
| month | INTEGER | Month |
| year_month | VARCHAR | YYYY-MM format |
| avg_daily_census | DECIMAL | Average daily census |
| total_patient_days | INTEGER | Sum of daily census |
| total_rn_hours | DECIMAL | Total RN hours |
| total_lpn_hours | DECIMAL | Total LPN hours |
| total_cna_hours | DECIMAL | Total CNA hours |
| total_nursing_hours | DECIMAL | Total nursing hours |
| total_employed_hours | DECIMAL | Total employed hours |
| total_contract_hours | DECIMAL | Total contract hours |
| avg_nursing_hppd | DECIMAL | Average HPPD |
| min_nursing_hppd | DECIMAL | Minimum HPPD |
| max_nursing_hppd | DECIMAL | Maximum HPPD |
| avg_contract_ratio | DECIMAL | Average contract ratio |
| days_reported | INTEGER | Days with data |

**Partitioning**: `state`

---

### agg_state_monthly

| Column | Data Type | Description |
|--------|-----------|-------------|
| state | VARCHAR(2) | State code |
| year | INTEGER | Year |
| month | INTEGER | Month |
| year_month | VARCHAR | YYYY-MM format |
| provider_count | INTEGER | Unique providers |
| total_patient_days | BIGINT | Total census |
| avg_daily_census | DECIMAL | Average census |
| total_nursing_hours | DECIMAL | Total nursing hours |
| total_rn_hours | DECIMAL | Total RN hours |
| total_lpn_hours | DECIMAL | Total LPN hours |
| total_cna_hours | DECIMAL | Total CNA hours |
| avg_nursing_hppd | DECIMAL | Average HPPD |
| median_nursing_hppd | DECIMAL | Median HPPD |
| p25_hppd | DECIMAL | 25th percentile HPPD |
| p75_hppd | DECIMAL | 75th percentile HPPD |
| avg_rn_mix_pct | DECIMAL | Average RN mix % |
| avg_contract_pct | DECIMAL | Average contract % |
| total_records | INTEGER | Record count |

---

### agg_overall_metrics

| Column | Data Type | Description |
|--------|-----------|-------------|
| total_providers | INTEGER | Total unique providers |
| total_states | INTEGER | Total states |
| total_patient_days | BIGINT | Total patient days |
| total_nursing_hours | DECIMAL | Total nursing hours |
| overall_avg_hppd | DECIMAL | Overall average HPPD |
| overall_contract_ratio | DECIMAL | Overall contract ratio |
| data_start_date | DATE | Earliest date |
| data_end_date | DATE | Latest date |
| total_days | INTEGER | Date range days |
| total_records | INTEGER | Total records |
| snapshot_date | DATE | Calculation date |

---

## Redshift Analytics Views

### healthcare.v_daily_metrics

Base view with calculated metrics for each daily record.

| Column | Description |
|--------|-------------|
| All fact columns | From fact_daily_staffing |
| nursing_hppd | Calculated HPPD |
| rn_skill_mix_pct | RN % of total hours |
| staffing_level_category | 'Meets CMS Benchmark', 'Near Benchmark', 'Below Benchmark', 'Critical Understaffing' |

---

### healthcare.v_kpi_summary

Single-row summary for dashboard KPI cards.

| Column | Description |
|--------|-------------|
| total_providers | Count of unique providers |
| total_states | Count of states |
| total_records | Total daily records |
| overall_avg_hppd | Average HPPD across all records |
| overall_rn_mix_pct | Average RN mix percentage |
| pct_meeting_benchmark | % of records with HPPD ≥ 4.1 |
| pct_critical_understaffing | % of records with HPPD < 2.5 |

---

## Glossary

| Term | Definition |
|------|------------|
| **HPPD** | Hours Per Patient Day - total nursing hours divided by patient census |
| **CMS** | Centers for Medicare & Medicaid Services |
| **PBJ** | Payroll-Based Journal - CMS staffing reporting system |
| **RN** | Registered Nurse |
| **LPN** | Licensed Practical Nurse |
| **CNA** | Certified Nursing Assistant |
| **MDS Census** | Minimum Data Set census - standardized patient count |
| **DON** | Director of Nursing |
| **Contract Staff** | Non-employed agency/temporary staff |
| **Employed Staff** | Direct facility employees |
| **FIPS** | Federal Information Processing Standards - geographic codes |
