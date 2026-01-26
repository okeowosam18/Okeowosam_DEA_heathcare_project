-- ============================================================================
-- Healthcare Metrics Pipeline - Redshift Setup Script
-- ============================================================================
-- Run this script in Redshift Query Editor after:
-- 1. CloudFormation stack is deployed
-- 2. Glue jobs have run successfully
-- 3. Glue Crawler has updated the Data Catalog
--
-- Replace placeholders:
--   {BUCKET_NAME} = Your S3 bucket name
--   {IAM_ROLE_ARN} = healthcare-metrics-redshift-role ARN
--   {GLUE_DATABASE} = healthcare-metrics_curated
-- ============================================================================

-- ============================================================================
-- PART 1: CREATE EXTERNAL SCHEMA (Connects to Glue Data Catalog)
-- ============================================================================

-- Create external schema from Glue Data Catalog
CREATE EXTERNAL SCHEMA IF NOT EXISTS healthcare_lake
FROM DATA CATALOG
DATABASE 'healthcare-metrics_curated'
IAM_ROLE 'arn:aws:iam::{ACCOUNT_ID}:role/healthcare-metrics-redshift-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Verify external tables are visible
-- SELECT * FROM svv_external_tables WHERE schemaname = 'healthcare_lake';

-- ============================================================================
-- PART 2: CREATE LOCAL SCHEMA FOR VIEWS AND MATERIALIZED VIEWS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS healthcare;

-- ============================================================================
-- PART 3: CREATE VIEWS FOR EASY ACCESS
-- ============================================================================

-- -----------------------------------------------------------------------------
-- View: Daily Staffing Fact
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW healthcare.v_daily_staffing AS
SELECT 
    provider_id,
    date_key,
    state,
    cy_qtr,
    work_date,
    patient_census,
    hrs_rn,
    hrs_lpn,
    hrs_cna,
    total_nursing_hours,
    nursing_hppd,
    contract_ratio,
    total_employed_hours,
    total_contract_hours
FROM healthcare_lake.fact_daily_staffing;

-- -----------------------------------------------------------------------------
-- View: Provider Dimension
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW healthcare.v_provider AS
SELECT 
    provider_id,
    provider_name,
    city,
    state,
    county_name,
    county_fips
FROM healthcare_lake.dim_provider;

-- -----------------------------------------------------------------------------
-- View: Provider Monthly Aggregates
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW healthcare.v_provider_monthly AS
SELECT 
    provider_id,
    provider_name,
    state,
    year,
    month,
    year_month,
    avg_daily_census,
    total_nursing_hours,
    avg_nursing_hppd,
    avg_contract_ratio,
    days_reported
FROM healthcare_lake.agg_provider_monthly;

-- -----------------------------------------------------------------------------
-- View: State Monthly Aggregates
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW healthcare.v_state_monthly AS
SELECT 
    state,
    year,
    month,
    year_month,
    provider_count,
    total_patient_days,
    total_nursing_hours,
    avg_nursing_hppd,
    median_nursing_hppd,
    avg_contract_ratio,
    total_records
FROM healthcare_lake.agg_state_monthly;

-- ============================================================================
-- PART 4: CREATE MATERIALIZED VIEWS FOR DASHBOARD PERFORMANCE
-- ============================================================================

-- -----------------------------------------------------------------------------
-- Materialized View: Staffing Summary for Dashboard
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW healthcare.mv_staffing_summary AS
SELECT 
    f.state,
    d.year,
    d.month,
    d.year_month,
    COUNT(DISTINCT f.provider_id) AS provider_count,
    SUM(f.patient_census) AS total_patient_days,
    AVG(f.patient_census) AS avg_daily_census,
    SUM(f.total_nursing_hours) AS total_nursing_hours,
    AVG(f.nursing_hppd) AS avg_nursing_hppd,
    AVG(f.contract_ratio) AS avg_contract_ratio,
    SUM(f.hrs_rn) AS total_rn_hours,
    SUM(f.hrs_lpn) AS total_lpn_hours,
    SUM(f.hrs_cna) AS total_cna_hours,
    COUNT(*) AS record_count
FROM healthcare_lake.fact_daily_staffing f
JOIN healthcare_lake.dim_date d ON f.date_key = d.date_key
GROUP BY f.state, d.year, d.month, d.year_month;

-- -----------------------------------------------------------------------------
-- Materialized View: Top Providers by HPPD
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW healthcare.mv_provider_rankings AS
SELECT 
    p.provider_id,
    p.provider_name,
    p.city,
    p.state,
    pm.year_month,
    pm.avg_nursing_hppd,
    pm.avg_daily_census,
    pm.total_nursing_hours,
    pm.avg_contract_ratio,
    pm.days_reported,
    RANK() OVER (PARTITION BY p.state, pm.year_month ORDER BY pm.avg_nursing_hppd DESC) AS state_rank_hppd,
    RANK() OVER (PARTITION BY pm.year_month ORDER BY pm.avg_nursing_hppd DESC) AS national_rank_hppd
FROM healthcare_lake.dim_provider p
JOIN healthcare_lake.agg_provider_monthly pm ON p.provider_id = pm.provider_id;

-- ============================================================================
-- PART 5: USEFUL QUERIES FOR DASHBOARD
-- ============================================================================

-- -----------------------------------------------------------------------------
-- Query 1: Overall KPI Summary
-- -----------------------------------------------------------------------------
/*
SELECT 
    COUNT(DISTINCT provider_id) AS total_providers,
    COUNT(DISTINCT state) AS total_states,
    SUM(patient_census) AS total_patient_days,
    SUM(total_nursing_hours) AS total_nursing_hours,
    AVG(nursing_hppd) AS avg_hppd,
    AVG(contract_ratio) * 100 AS avg_contract_pct
FROM healthcare.v_daily_staffing;
*/

-- -----------------------------------------------------------------------------
-- Query 2: State Comparison
-- -----------------------------------------------------------------------------
/*
SELECT 
    state,
    COUNT(DISTINCT provider_id) AS providers,
    AVG(nursing_hppd) AS avg_hppd,
    AVG(contract_ratio) * 100 AS contract_pct,
    SUM(total_nursing_hours) AS total_hours
FROM healthcare.v_daily_staffing
GROUP BY state
ORDER BY avg_hppd DESC;
*/

-- -----------------------------------------------------------------------------
-- Query 3: Monthly Trends
-- -----------------------------------------------------------------------------
/*
SELECT 
    year_month,
    SUM(total_nursing_hours) AS total_hours,
    AVG(avg_nursing_hppd) AS avg_hppd,
    SUM(provider_count) AS providers_reporting
FROM healthcare.mv_staffing_summary
GROUP BY year_month
ORDER BY year_month;
*/

-- -----------------------------------------------------------------------------
-- Query 4: Low HPPD Facilities (Potential Understaffing)
-- -----------------------------------------------------------------------------
/*
SELECT 
    p.provider_id,
    p.provider_name,
    p.city,
    p.state,
    pm.avg_nursing_hppd,
    pm.avg_daily_census,
    pm.year_month
FROM healthcare_lake.dim_provider p
JOIN healthcare_lake.agg_provider_monthly pm ON p.provider_id = pm.provider_id
WHERE pm.avg_nursing_hppd < 3.0 
  AND pm.avg_daily_census > 20
ORDER BY pm.avg_nursing_hppd
LIMIT 100;
*/

-- -----------------------------------------------------------------------------
-- Query 5: Contract vs Employed Staff Trends
-- -----------------------------------------------------------------------------
/*
SELECT 
    state,
    year_month,
    SUM(total_employed_hours) AS employed_hours,
    SUM(total_contract_hours) AS contract_hours,
    SUM(total_contract_hours) / NULLIF(SUM(total_nursing_hours), 0) * 100 AS contract_pct
FROM healthcare.v_daily_staffing f
JOIN healthcare_lake.dim_date d ON f.date_key = d.date_key
GROUP BY state, year_month
ORDER BY state, year_month;
*/

-- ============================================================================
-- PART 6: REFRESH COMMANDS
-- ============================================================================

-- Refresh materialized views (run after new data loads)
-- REFRESH MATERIALIZED VIEW healthcare.mv_staffing_summary;
-- REFRESH MATERIALIZED VIEW healthcare.mv_provider_rankings;

-- ============================================================================
-- PART 7: GRANT PERMISSIONS (if using IAM authentication)
-- ============================================================================

-- Grant usage on schemas
-- GRANT USAGE ON SCHEMA healthcare TO PUBLIC;
-- GRANT USAGE ON SCHEMA healthcare_lake TO PUBLIC;

-- Grant select on all tables/views
-- GRANT SELECT ON ALL TABLES IN SCHEMA healthcare TO PUBLIC;
-- GRANT SELECT ON ALL TABLES IN SCHEMA healthcare_lake TO PUBLIC;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check external tables
-- SELECT schemaname, tablename, location FROM svv_external_tables WHERE schemaname = 'healthcare_lake';

-- Check row counts
-- SELECT 'fact_daily_staffing' as table_name, COUNT(*) as row_count FROM healthcare_lake.fact_daily_staffing
-- UNION ALL
-- SELECT 'dim_provider', COUNT(*) FROM healthcare_lake.dim_provider
-- UNION ALL
-- SELECT 'dim_date', COUNT(*) FROM healthcare_lake.dim_date
-- UNION ALL
-- SELECT 'agg_provider_monthly', COUNT(*) FROM healthcare_lake.agg_provider_monthly
-- UNION ALL
-- SELECT 'agg_state_monthly', COUNT(*) FROM healthcare_lake.agg_state_monthly;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
