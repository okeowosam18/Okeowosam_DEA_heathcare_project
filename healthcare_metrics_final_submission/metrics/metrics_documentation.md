# Healthcare Metrics - Step 5: Metrics Definitions

## Executive Summary

This document defines the key performance indicators (KPIs) and metrics for the Healthcare Staffing Analytics Dashboard. These metrics are derived from CMS Payroll-Based Journal (PBJ) nursing staffing data and are designed to measure staffing adequacy, workforce stability, and care quality indicators.

---

## Core Metrics

### 1. Nursing Hours Per Patient Day (HPPD)

| Attribute | Value |
|-----------|-------|
| **Formula** | `(RN Hours + LPN Hours + CNA Hours) / Patient Census` |
| **Unit** | Hours per patient per day |
| **Benchmark** | CMS recommends minimum **4.1 HPPD** |
| **Data Source** | `hrs_rn`, `hrs_lpn`, `hrs_cna`, `patient_census` |

**Business Context:**
HPPD is the primary measure of nursing home staffing adequacy. Higher HPPD is associated with better patient outcomes, fewer falls, lower infection rates, and reduced hospitalizations. CMS uses this metric for quality ratings.

**Interpretation:**
- **≥ 4.1 HPPD**: Meets CMS benchmark - adequate staffing
- **3.5 - 4.0 HPPD**: Near benchmark - acceptable but could improve
- **2.5 - 3.4 HPPD**: Below benchmark - potential quality concerns
- **< 2.5 HPPD**: Critical understaffing - immediate attention needed

---

### 2. RN Hours Per Patient Day (RN HPPD)

| Attribute | Value |
|-----------|-------|
| **Formula** | `RN Hours / Patient Census` |
| **Unit** | Hours per patient per day |
| **Benchmark** | CMS recommends minimum **0.75 RN HPPD** |
| **Data Source** | `hrs_rn`, `patient_census` |

**Business Context:**
RN staffing is particularly critical as RNs perform assessments, care planning, and complex clinical tasks that LPNs and CNAs cannot. Research shows RN staffing levels have the strongest correlation with quality outcomes.

---

### 3. Contract Staff Ratio

| Attribute | Value |
|-----------|-------|
| **Formula** | `(Contract RN + Contract LPN + Contract CNA Hours) / Total Nursing Hours × 100` |
| **Unit** | Percentage |
| **Benchmark** | < 15% is typical; > 30% indicates high reliance |
| **Data Source** | `hrs_rn_ctr`, `hrs_lpn_ctr`, `hrs_cna_ctr`, `total_nursing_hours` |

**Business Context:**
High contract/agency staff usage may indicate:
- Difficulty retaining permanent staff
- Staffing instability
- Higher labor costs
- Potential continuity of care issues

While contract staff fill important gaps, excessive reliance (>30%) may signal workforce management challenges.

---

### 4. RN Skill Mix

| Attribute | Value |
|-----------|-------|
| **Formula** | `RN Hours / Total Nursing Hours × 100` |
| **Unit** | Percentage |
| **Benchmark** | 15-25% is typical for nursing homes |
| **Data Source** | `hrs_rn`, `total_nursing_hours` |

**Business Context:**
Skill mix measures the proportion of care delivered by RNs versus LPNs and CNAs. A higher RN skill mix is associated with:
- Better clinical outcomes
- More effective care coordination
- Improved patient assessments

However, cost constraints mean most facilities operate with 15-25% RN mix.

---

### 5. CNA Ratio

| Attribute | Value |
|-----------|-------|
| **Formula** | `CNA Hours / Total Nursing Hours × 100` |
| **Unit** | Percentage |
| **Benchmark** | 50-65% is typical |
| **Data Source** | `hrs_cna`, `total_nursing_hours` |

**Business Context:**
CNAs provide the majority of direct patient care (bathing, feeding, mobility assistance). An appropriate CNA ratio ensures adequate hands-on care for residents.

---

## Staffing Level Categories

Based on HPPD, facilities are categorized into staffing levels:

| Category | HPPD Range | Description |
|----------|------------|-------------|
| **Meets CMS Benchmark** | ≥ 4.1 | Adequate staffing per CMS standards |
| **Near Benchmark** | 3.5 - 4.0 | Slightly below target but acceptable |
| **Below Benchmark** | 2.5 - 3.4 | Concerning - may impact quality |
| **Critical Understaffing** | < 2.5 | Severe understaffing - immediate risk |

---

## Aggregate Metrics

### State-Level Metrics

| Metric | Description |
|--------|-------------|
| **Provider Count** | Number of facilities reporting in state |
| **Avg HPPD** | Mean HPPD across all facilities |
| **Median HPPD** | Middle value (less affected by outliers) |
| **% Meeting Benchmark** | Proportion of facility-days meeting 4.1 HPPD |
| **% Critical** | Proportion of facility-days below 2.5 HPPD |

### Provider-Level Metrics

| Metric | Description |
|--------|-------------|
| **Days Reported** | Number of days with data (data completeness) |
| **Avg Daily Census** | Average patient count |
| **Avg HPPD** | Average hours per patient day |
| **HPPD Variance** | Standard deviation (consistency measure) |
| **Critical Days** | Count of days below 2.5 HPPD |
| **State Rank** | Ranking within state by HPPD |
| **National Percentile** | Percentile rank nationally |

---

## Alert Thresholds

The system generates alerts based on these thresholds:

| Alert Level | Conditions |
|-------------|------------|
| **HIGH** | HPPD < 2.5 AND Census > 20 |
| **MEDIUM** | HPPD < 3.0 AND Census > 20, OR Contract % > 50%, OR Critical Days > 10 |
| **LOW** | All other concerning patterns |

---

## Data Quality Considerations

### Exclusions
- Records with `patient_census = 0` are excluded from HPPD calculations
- Facilities with < 30 days of data are excluded from rankings

### Known Limitations
- Self-reported data (potential for reporting errors)
- Q2 2024 data only (seasonal variations not captured)
- No adjustment for patient acuity

---

## Views Created in Redshift

| View Name | Purpose |
|-----------|---------|
| `healthcare.v_daily_metrics` | Base view with all calculated metrics |
| `healthcare.v_provider_monthly` | Monthly aggregates by provider |
| `healthcare.v_state_monthly` | Monthly aggregates by state |
| `healthcare.v_kpi_summary` | Overall dashboard KPIs |
| `healthcare.v_provider_rankings` | Provider rankings by metrics |
| `healthcare.v_staffing_alerts` | Facilities with concerning metrics |
| `healthcare.v_day_of_week_analysis` | Weekday vs weekend patterns |

---

## References

- CMS Staffing Data Specifications: https://www.cms.gov/medicare/quality/nursing-home-improvement/staffing-data-submission-pbj
- Research on staffing and outcomes: Harrington et al., "Nurse Staffing and Deficiencies in the Largest For-Profit Nursing Home Chains"
- CMS Five-Star Quality Rating System Technical Users' Guide

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2024-12-28 | Samuel | Initial metrics definition |
