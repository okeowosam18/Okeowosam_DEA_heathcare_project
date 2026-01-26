"""
Healthcare Metrics Pipeline - AWS Glue Job 1: Raw to Processed
==============================================================
Transforms raw CSV data to cleaned Parquet (Bronze → Silver)

Deploy to: s3://{bucket}/scripts/glue_job_1_raw_to_processed.py

Arguments:
    --source_bucket: S3 bucket name
    --target_bucket: S3 bucket name (usually same as source)

Author: Samuel
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# =============================================================================
# INITIALIZE GLUE CONTEXT
# =============================================================================
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
SOURCE_BUCKET = args['source_bucket']
TARGET_BUCKET = args['target_bucket']
RAW_PATH = f"s3://{SOURCE_BUCKET}/raw/nursing_staffing/"
PROCESSED_PATH = f"s3://{TARGET_BUCKET}/processed/nursing_staffing/"

print("="*60)
print("GLUE JOB 1: RAW TO PROCESSED (Bronze → Silver)")
print(f"Source: {RAW_PATH}")
print(f"Target: {PROCESSED_PATH}")
print("="*60)

# =============================================================================
# COLUMN DEFINITIONS
# =============================================================================
HOUR_COLUMNS = [
    'Hrs_RNDON', 'Hrs_RNDON_emp', 'Hrs_RNDON_ctr',
    'Hrs_RNadmin', 'Hrs_RNadmin_emp', 'Hrs_RNadmin_ctr',
    'Hrs_RN', 'Hrs_RN_emp', 'Hrs_RN_ctr',
    'Hrs_LPNadmin', 'Hrs_LPNadmin_emp', 'Hrs_LPNadmin_ctr',
    'Hrs_LPN', 'Hrs_LPN_emp', 'Hrs_LPN_ctr',
    'Hrs_CNA', 'Hrs_CNA_emp', 'Hrs_CNA_ctr',
    'Hrs_NAtrn', 'Hrs_NAtrn_emp', 'Hrs_NAtrn_ctr',
    'Hrs_MedAide', 'Hrs_MedAide_emp', 'Hrs_MedAide_ctr'
]

# =============================================================================
# READ RAW DATA
# =============================================================================
print("\n--- Reading Raw Data ---")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("recursiveFileLookup", "true") \
    .csv(RAW_PATH)

initial_count = df.count()
print(f"Loaded {initial_count:,} rows from raw layer")
print(f"Columns: {len(df.columns)}")

# =============================================================================
# TRANSFORMATIONS
# =============================================================================
print("\n--- Applying Transformations ---")

# 1. Parse WorkDate (YYYYMMDD integer → DATE)
print("  Parsing WorkDate...")
df = df.withColumn(
    "work_date",
    F.to_date(F.col("WorkDate").cast("string"), "yyyyMMdd")
)

# 2. Cast hour columns to DOUBLE
print("  Casting hour columns...")
for col_name in HOUR_COLUMNS:
    if col_name in df.columns:
        df = df.withColumn(col_name.lower(), F.col(col_name).cast(DoubleType()))

# 3. Cast integer columns
print("  Casting integer columns...")
df = df.withColumn("mds_census", F.col("MDScensus").cast(IntegerType()))
df = df.withColumn("county_fips", F.col("COUNTY_FIPS").cast(IntegerType()))

# 4. Standardize string columns
print("  Standardizing strings...")
df = df.withColumn("provider_id", F.trim(F.col("PROVNUM")))
df = df.withColumn("provider_name", F.trim(F.col("PROVNAME")))
df = df.withColumn("city", F.trim(F.upper(F.col("CITY"))))
df = df.withColumn("state", F.trim(F.upper(F.col("STATE"))))
df = df.withColumn("county_name", F.trim(F.col("COUNTY_NAME")))
df = df.withColumn("cy_qtr", F.trim(F.col("CY_Qtr")))

# 5. Add time dimension columns
print("  Adding time dimensions...")
df = df.withColumn("work_year", F.year("work_date"))
df = df.withColumn("work_month", F.month("work_date"))
df = df.withColumn("work_day_of_week", F.dayofweek("work_date"))

# 6. Calculate total nursing hours
print("  Calculating total nursing hours...")
df = df.withColumn(
    "total_nursing_hours",
    F.coalesce(F.col("hrs_rn"), F.lit(0.0)) +
    F.coalesce(F.col("hrs_lpn"), F.lit(0.0)) +
    F.coalesce(F.col("hrs_cna"), F.lit(0.0))
)

# 7. Calculate employed vs contract hours
df = df.withColumn(
    "total_employed_hours",
    F.coalesce(F.col("hrs_rn_emp"), F.lit(0.0)) +
    F.coalesce(F.col("hrs_lpn_emp"), F.lit(0.0)) +
    F.coalesce(F.col("hrs_cna_emp"), F.lit(0.0))
)

df = df.withColumn(
    "total_contract_hours",
    F.coalesce(F.col("hrs_rn_ctr"), F.lit(0.0)) +
    F.coalesce(F.col("hrs_lpn_ctr"), F.lit(0.0)) +
    F.coalesce(F.col("hrs_cna_ctr"), F.lit(0.0))
)

# 8. Calculate HPPD (Hours Per Patient Day)
print("  Calculating HPPD...")
df = df.withColumn(
    "nursing_hppd",
    F.when(
        F.col("mds_census") > 0,
        F.round(F.col("total_nursing_hours") / F.col("mds_census"), 2)
    ).otherwise(F.lit(None))
)

# 9. Calculate contract ratio
df = df.withColumn(
    "contract_ratio",
    F.when(
        F.col("total_nursing_hours") > 0,
        F.round(F.col("total_contract_hours") / F.col("total_nursing_hours"), 4)
    ).otherwise(F.lit(0.0))
)

# 10. Add metadata columns
print("  Adding metadata...")
df = df.withColumn("processed_at", F.current_timestamp())
df = df.withColumn("source_file", F.input_file_name())

# 11. Filter invalid records
print("  Filtering invalid records...")
invalid_dates = df.filter(F.col("work_date").isNull()).count()
if invalid_dates > 0:
    print(f"  WARNING: Removing {invalid_dates} rows with invalid dates")

df = df.filter(F.col("work_date").isNotNull())

# =============================================================================
# SELECT FINAL COLUMNS
# =============================================================================
print("\n--- Selecting Final Columns ---")

# Build column list
final_columns = [
    # Identifiers
    "provider_id", "provider_name", "city", "state", "county_name", "county_fips",
    # Time
    "cy_qtr", "work_date", "work_year", "work_month", "work_day_of_week",
    # Census
    "mds_census",
]

# Add all hour columns (lowercase)
for col_name in HOUR_COLUMNS:
    lc = col_name.lower()
    if lc in df.columns:
        final_columns.append(lc)

# Add calculated and metadata columns
final_columns.extend([
    "total_nursing_hours", "total_employed_hours", "total_contract_hours",
    "nursing_hppd", "contract_ratio",
    "processed_at", "source_file"
])

df_final = df.select(*[c for c in final_columns if c in df.columns])

# =============================================================================
# DATA QUALITY CHECKS
# =============================================================================
print("\n--- Data Quality Checks ---")

final_count = df_final.count()
print(f"Final row count: {final_count:,}")

# Key column null checks
for col in ['provider_id', 'work_date', 'state', 'mds_census']:
    if col in df_final.columns:
        null_count = df_final.filter(F.col(col).isNull()).count()
        pct = (null_count / final_count) * 100 if final_count > 0 else 0
        status = "✓" if pct == 0 else "⚠"
        print(f"  {status} {col}: {null_count:,} nulls ({pct:.2f}%)")

# Date range
date_stats = df_final.agg(
    F.min("work_date").alias("min_date"),
    F.max("work_date").alias("max_date")
).collect()[0]
print(f"  Date range: {date_stats['min_date']} to {date_stats['max_date']}")

# Provider count
provider_count = df_final.select("provider_id").distinct().count()
print(f"  Unique providers: {provider_count:,}")

# State count
state_count = df_final.select("state").distinct().count()
print(f"  Unique states: {state_count}")

# =============================================================================
# WRITE TO PROCESSED LAYER
# =============================================================================
print("\n--- Writing to Processed Layer ---")
print(f"Output path: {PROCESSED_PATH}")
print("Partitioning by: cy_qtr, state")

df_final.write \
    .mode("overwrite") \
    .partitionBy("cy_qtr", "state") \
    .option("compression", "snappy") \
    .parquet(PROCESSED_PATH)

print("Write complete!")

# =============================================================================
# JOB COMPLETE
# =============================================================================
print("\n" + "="*60)
print("JOB COMPLETED SUCCESSFULLY")
print(f"Rows processed: {final_count:,}")
print(f"Output: {PROCESSED_PATH}")
print("="*60)

job.commit()
