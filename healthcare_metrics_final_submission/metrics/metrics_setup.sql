-- ============================================================================
-- Healthcare Metrics - Step 5: Metrics Definitions and Calculations
-- ============================================================================
-- Run these in Redshift Query Editor v2 after connecting to your workgroup
-- ============================================================================

-- ============================================================================
-- PART 1: CREATE LOCAL SCHEMA FOR VIEWS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS healthcare;

-- ============================================================================
-- PART 2: CORE METRIC VIEWS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- VIEW 1: Daily Staffing Metrics (Base View)
-- ----------------------------------------------------------------------------
-- Purpose: Adds calculated metrics to daily staffing facts
-- Key Metrics: HPPD, Contract Ratio, Skill Mix
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW healthcare.v_daily_metrics AS
SELECT 
    -- Identifiers
    provider_id,
    state,
    cy_qtr,
    work_date,
    
    -- Patient Census
    patient_census,
    
    -- Raw Hours by Staff Type
    COALESCE(hrs_rn, 0) AS hrs_rn,
    COALESCE(hrs_lpn, 0) AS hrs_lpn,
    COALESCE(hrs_cna, 0) AS hrs_cna,
    COALESCE(hrs_rn_emp, 0) AS hrs_rn_emp,
    COALESCE(hrs_lpn_emp, 0) AS hrs_lpn_emp,
    COALESCE(hrs_cna_emp, 0) AS hrs_cna_emp,
    COALESCE(hrs_rn_ctr, 0) AS hrs_rn_ctr,
    COALESCE(hrs_lpn_ctr, 0) AS hrs_lpn_ctr,
    COALESCE(hrs_cna_ctr, 0) AS hrs_cna_ctr,
    
    -- Total Nursing Hours
    COALESCE(total_nursing_hours, 0) AS total_nursing_hours,
    
    -- METRIC 1: Nursing Hours Per Patient Day (HPPD)
    -- Formula: Total Nursing Hours / Patient Census
    -- Benchmark: CMS recommends minimum 4.1 HPPD
    CASE 
        WHEN patient_census > 0 
        THEN ROUND(CAST(total_nursing_hours AS DECIMAL(10,2)) / patient_census, 2)
        ELSE NULL 
    END AS nursing_hppd,
    
    -- METRIC 2: RN Hours Per Patient Day
    CASE 
        WHEN patient_census > 0 
        THEN ROUND(CAST(COALESCE(hrs_rn, 0) AS DECIMAL(10,2)) / patient_census, 2)
        ELSE NULL 
    END AS rn_hppd,
    
    -- METRIC 3: Contract Staff Ratio (% of hours from contract staff)
    -- Higher ratio may indicate staffing instability
    CASE 
        WHEN total_nursing_hours > 0 
        THEN ROUND(
            CAST(COALESCE(hrs_rn_ctr, 0) + COALESCE(hrs_lpn_ctr, 0) + COALESCE(hrs_cna_ctr, 0) AS DECIMAL(10,4)) 
            / total_nursing_hours * 100, 2)
        ELSE 0 
    END AS contract_staff_pct,
    
    -- METRIC 4: RN Skill Mix (% of nursing hours from RNs)
    -- Higher RN mix associated with better outcomes
    CASE 
        WHEN total_nursing_hours > 0 
        THEN ROUND(CAST(COALESCE(hrs_rn, 0) AS DECIMAL(10,4)) / total_nursing_hours * 100, 2)
        ELSE 0 
    END AS rn_skill_mix_pct,
    
    -- METRIC 5: CNA Ratio (% from CNAs)
    CASE 
        WHEN total_nursing_hours > 0 
        THEN ROUND(CAST(COALESCE(hrs_cna, 0) AS DECIMAL(10,4)) / total_nursing_hours * 100, 2)
        ELSE 0 
    END AS cna_ratio_pct,
    
    -- METRIC 6: Staffing Level Category
    CASE 
        WHEN patient_census = 0 THEN 'No Census'
        WHEN total_nursing_hours / NULLIF(patient_census, 0) >= 4.1 THEN 'Meets CMS Benchmark'
        WHEN total_nursing_hours / NULLIF(patient_census, 0) >= 3.5 THEN 'Near Benchmark'
        WHEN total_nursing_hours / NULLIF(patient_census, 0) >= 2.5 THEN 'Below Benchmark'
        ELSE 'Critical Understaffing'
    END AS staffing_level_category

FROM healthcare_lake.facts;


-- ----------------------------------------------------------------------------
-- VIEW 2: Provider Monthly Summary
-- ----------------------------------------------------------------------------
-- Purpose: Aggregate metrics by provider and month for trending
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW healthcare.v_provider_monthly AS
SELECT 
    provider_id,
    state,
    DATE_TRUNC('month', work_date) AS month_date,
    TO_CHAR(work_date, 'YYYY-MM') AS year_month,
    
    -- Volume Metrics
    COUNT(*) AS days_reported,
    ROUND(AVG(patient_census), 1) AS avg_daily_census,
    SUM(patient_census) AS total_patient_days,
    
    -- Hours Totals
    SUM(total_nursing_hours) AS total_nursing_hours,
    SUM(hrs_rn) AS total_rn_hours,
    SUM(hrs_lpn) AS total_lpn_hours,
    SUM(hrs_cna) AS total_cna_hours,
    
    -- HPPD Metrics
    ROUND(AVG(nursing_hppd), 2) AS avg_hppd,
    ROUND(MIN(nursing_hppd), 2) AS min_hppd,
    ROUND(MAX(nursing_hppd), 2) AS max_hppd,
    ROUND(STDDEV(nursing_hppd), 2) AS stddev_hppd,
    
    -- Staffing Mix
    ROUND(AVG(rn_skill_mix_pct), 2) AS avg_rn_mix_pct,
    ROUND(AVG(contract_staff_pct), 2) AS avg_contract_pct,
    
    -- Performance Flags
    SUM(CASE WHEN staffing_level_category = 'Critical Understaffing' THEN 1 ELSE 0 END) AS critical_days,
    SUM(CASE WHEN staffing_level_category = 'Meets CMS Benchmark' THEN 1 ELSE 0 END) AS benchmark_met_days

FROM healthcare.v_daily_metrics
WHERE patient_census > 0
GROUP BY provider_id, state, DATE_TRUNC('month', work_date), TO_CHAR(work_date, 'YYYY-MM');


-- ----------------------------------------------------------------------------
-- VIEW 3: State Monthly Summary
-- ----------------------------------------------------------------------------
-- Purpose: State-level aggregates for geographic comparison
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW healthcare.v_state_monthly AS
SELECT 
    state,
    DATE_TRUNC('month', work_date) AS month_date,
    TO_CHAR(work_date, 'YYYY-MM') AS year_month,
    
    -- Provider Coverage
    COUNT(DISTINCT provider_id) AS provider_count,
    
    -- Volume
    SUM(patient_census) AS total_patient_days,
    ROUND(AVG(patient_census), 1) AS avg_daily_census,
    
    -- Hours
    SUM(total_nursing_hours) AS total_nursing_hours,
    SUM(hrs_rn) AS total_rn_hours,
    SUM(hrs_lpn) AS total_lpn_hours,
    SUM(hrs_cna) AS total_cna_hours,
    
    -- HPPD (weighted by census would be more accurate, using simple avg here)
    ROUND(AVG(nursing_hppd), 2) AS avg_hppd,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY nursing_hppd), 2) AS median_hppd,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY nursing_hppd), 2) AS p25_hppd,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY nursing_hppd), 2) AS p75_hppd,
    
    -- Staffing Mix
    ROUND(AVG(rn_skill_mix_pct), 2) AS avg_rn_mix_pct,
    ROUND(AVG(contract_staff_pct), 2) AS avg_contract_pct,
    
    -- Performance Distribution
    ROUND(100.0 * SUM(CASE WHEN staffing_level_category = 'Meets CMS Benchmark' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_meeting_benchmark,
    ROUND(100.0 * SUM(CASE WHEN staffing_level_category = 'Critical Understaffing' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_critical

FROM healthcare.v_daily_metrics
WHERE patient_census > 0
GROUP BY state, DATE_TRUNC('month', work_date), TO_CHAR(work_date, 'YYYY-MM');


-- ----------------------------------------------------------------------------
-- VIEW 4: Overall KPI Summary (Dashboard Header)
-- ----------------------------------------------------------------------------
-- Purpose: High-level KPIs for dashboard summary cards
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW healthcare.v_kpi_summary AS
SELECT 
    -- Data Coverage
    COUNT(DISTINCT provider_id) AS total_providers,
    COUNT(DISTINCT state) AS total_states,
    MIN(work_date) AS data_start_date,
    MAX(work_date) AS data_end_date,
    COUNT(DISTINCT work_date) AS total_days,
    COUNT(*) AS total_records,
    
    -- Patient Volume
    SUM(patient_census) AS total_patient_days,
    ROUND(AVG(patient_census), 1) AS avg_daily_census,
    
    -- Staffing Volume
    ROUND(SUM(total_nursing_hours), 0) AS total_nursing_hours,
    ROUND(SUM(hrs_rn), 0) AS total_rn_hours,
    ROUND(SUM(hrs_lpn), 0) AS total_lpn_hours,
    ROUND(SUM(hrs_cna), 0) AS total_cna_hours,
    
    -- Key Metrics (Averages)
    ROUND(AVG(nursing_hppd), 2) AS overall_avg_hppd,
    ROUND(AVG(rn_skill_mix_pct), 1) AS overall_rn_mix_pct,
    ROUND(AVG(contract_staff_pct), 1) AS overall_contract_pct,
    
    -- Benchmark Performance
    ROUND(100.0 * SUM(CASE WHEN staffing_level_category = 'Meets CMS Benchmark' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_meeting_benchmark,
    ROUND(100.0 * SUM(CASE WHEN staffing_level_category = 'Critical Understaffing' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_critical_understaffing

FROM healthcare.v_daily_metrics
WHERE patient_census > 0;


-- ----------------------------------------------------------------------------
-- VIEW 5: Provider Rankings
-- ----------------------------------------------------------------------------
-- Purpose: Rank providers by key metrics for comparison
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW healthcare.v_provider_rankings AS
SELECT 
    provider_id,
    state,
    days_reported,
    avg_daily_census,
    avg_hppd,
    avg_rn_mix_pct,
    avg_contract_pct,
    critical_days,
    
    -- Rankings (within state)
    RANK() OVER (PARTITION BY state ORDER BY avg_hppd DESC) AS state_rank_hppd,
    RANK() OVER (PARTITION BY state ORDER BY avg_rn_mix_pct DESC) AS state_rank_rn_mix,
    RANK() OVER (PARTITION BY state ORDER BY avg_contract_pct ASC) AS state_rank_stability,
    
    -- Rankings (national)
    RANK() OVER (ORDER BY avg_hppd DESC) AS national_rank_hppd,
    
    -- Percentiles (national)
    ROUND(PERCENT_RANK() OVER (ORDER BY avg_hppd) * 100, 1) AS hppd_percentile

FROM healthcare.v_provider_monthly
WHERE days_reported >= 30;  -- Only providers with at least 30 days of data


-- ----------------------------------------------------------------------------
-- VIEW 6: Low Staffing Alerts
-- ----------------------------------------------------------------------------
-- Purpose: Identify facilities with concerning staffing levels
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW healthcare.v_staffing_alerts AS
SELECT 
    provider_id,
    state,
    year_month,
    avg_daily_census,
    avg_hppd,
    avg_rn_mix_pct,
    avg_contract_pct,
    critical_days,
    days_reported,
    
    -- Alert Level
    CASE 
        WHEN avg_hppd < 2.5 AND avg_daily_census > 20 THEN 'HIGH - Critical Understaffing'
        WHEN avg_hppd < 3.0 AND avg_daily_census > 20 THEN 'MEDIUM - Below Minimum'
        WHEN avg_contract_pct > 50 THEN 'MEDIUM - High Contract Reliance'
        WHEN critical_days > 10 THEN 'MEDIUM - Frequent Critical Days'
        ELSE 'LOW'
    END AS alert_level,
    
    -- Alert Reasons
    CASE 
        WHEN avg_hppd < 2.5 THEN 'HPPD below 2.5'
        WHEN avg_hppd < 3.0 THEN 'HPPD below 3.0'
        ELSE ''
    END AS hppd_alert,
    
    CASE 
        WHEN avg_contract_pct > 50 THEN 'Contract staff >50%'
        WHEN avg_contract_pct > 30 THEN 'Contract staff >30%'
        ELSE ''
    END AS contract_alert

FROM healthcare.v_provider_monthly
WHERE avg_hppd < 3.5 OR avg_contract_pct > 30 OR critical_days > 5;


-- ----------------------------------------------------------------------------
-- VIEW 7: Weekday vs Weekend Analysis
-- ----------------------------------------------------------------------------
-- Purpose: Compare staffing patterns by day of week
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW healthcare.v_day_of_week_analysis AS
SELECT 
    state,
    EXTRACT(DOW FROM work_date) AS day_of_week,
    CASE EXTRACT(DOW FROM work_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    CASE WHEN EXTRACT(DOW FROM work_date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    
    COUNT(*) AS record_count,
    ROUND(AVG(nursing_hppd), 2) AS avg_hppd,
    ROUND(AVG(rn_skill_mix_pct), 2) AS avg_rn_mix_pct,
    ROUND(AVG(contract_staff_pct), 2) AS avg_contract_pct

FROM healthcare.v_daily_metrics
WHERE patient_census > 0
GROUP BY state, EXTRACT(DOW FROM work_date), 
         CASE EXTRACT(DOW FROM work_date)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
         END,
         CASE WHEN EXTRACT(DOW FROM work_date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END;


-- ============================================================================
-- PART 3: VERIFICATION QUERIES
-- ============================================================================

-- Run these to verify the views are working:

-- Check KPI Summary
-- SELECT * FROM healthcare.v_kpi_summary;

-- Check State Summary
-- SELECT * FROM healthcare.v_state_monthly ORDER BY state, year_month LIMIT 20;

-- Check Provider Rankings (Top 10 by HPPD)
-- SELECT * FROM healthcare.v_provider_rankings ORDER BY national_rank_hppd LIMIT 10;

-- Check Staffing Alerts
-- SELECT alert_level, COUNT(*) FROM healthcare.v_staffing_alerts GROUP BY alert_level;


-- ============================================================================
-- END OF METRICS SETUP
-- ============================================================================
