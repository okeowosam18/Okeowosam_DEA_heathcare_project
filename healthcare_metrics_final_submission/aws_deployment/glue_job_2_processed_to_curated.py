"""
Healthcare Metrics Pipeline - AWS Glue Job 2: Processed to Curated
==================================================================
Creates dimensional model from processed data (Silver → Gold)

Deploy to: s3://{bucket}/scripts/glue_job_2_processed_to_curated.py

Arguments:
    --source_bucket: S3 bucket name
    --target_bucket: S3 bucket name
    --database_name: Glue Data Catalog database name

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
from pyspark.sql.types import IntegerType

# =============================================================================
# INITIALIZE GLUE CONTEXT
# =============================================================================
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket', 'database_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
SOURCE_BUCKET = args['source_bucket']
TARGET_BUCKET = args['target_bucket']
DATABASE_NAME = args['database_name']

PROCESSED_PATH = f"s3://{SOURCE_BUCKET}/processed/nursing_staffing/"
CURATED_PATH = f"s3://{TARGET_BUCKET}/curated/"

print("="*60)
print("GLUE JOB 2: PROCESSED TO CURATED (Silver → Gold)")
print(f"Source: {PROCESSED_PATH}")
print(f"Target: {CURATED_PATH}")
print(f"Database: {DATABASE_NAME}")
print("="*60)

# =============================================================================
# READ PROCESSED DATA
# =============================================================================
print("\n--- Reading Processed Data ---")

df = spark.read.parquet(PROCESSED_PATH)
total_rows = df.count()
print(f"Loaded {total_rows:,} rows from processed layer")

# =============================================================================
# BUILD DIMENSION: DIM_DATE
# =============================================================================
print("\n--- Building dim_date ---")

# Get date range from data
date_range = df.agg(
    F.min("work_date").alias("min_date"),
    F.max("work_date").alias("max_date")
).collect()[0]

# Extend range for future use
start_date = "2024-01-01"
end_date = "2025-12-31"

dim_date = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'), 
        to_date('{end_date}'), 
        interval 1 day
    )) as full_date
""")

dim_date = dim_date.select(
    F.date_format("full_date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    F.col("full_date"),
    F.year("full_date").alias("year"),
    F.quarter("full_date").alias("quarter"),
    F.month("full_date").alias("month"),
    F.dayofmonth("full_date").alias("day"),
    F.weekofyear("full_date").alias("week_of_year"),
    F.dayofweek("full_date").alias("day_of_week"),
    F.date_format("full_date", "EEEE").alias("day_name"),
    F.when(F.dayofweek("full_date").isin(1, 7), True).otherwise(False).alias("is_weekend"),
    F.date_format("full_date", "yyyy-MM").alias("year_month"),
    F.concat(F.year("full_date"), F.lit("Q"), F.quarter("full_date")).alias("year_quarter")
)

dim_date_count = dim_date.count()
print(f"  Generated {dim_date_count} date records")

# =============================================================================
# BUILD DIMENSION: DIM_PROVIDER
# =============================================================================
print("\n--- Building dim_provider ---")

dim_provider = df.select(
    "provider_id",
    "provider_name",
    "city",
    "state",
    "county_name",
    "county_fips"
).dropDuplicates(["provider_id"])

# Add surrogate key
dim_provider = dim_provider.withColumn(
    "provider_sk",
    F.monotonically_increasing_id()
)

dim_provider_count = dim_provider.count()
print(f"  Generated {dim_provider_count} provider records")

# =============================================================================
# BUILD DIMENSION: DIM_STATE
# =============================================================================
print("\n--- Building dim_state ---")

dim_state = df.groupBy("state").agg(
    F.countDistinct("provider_id").alias("provider_count"),
    F.countDistinct("county_name").alias("county_count"),
    F.countDistinct("city").alias("city_count"),
    F.sum("mds_census").alias("total_patient_days"),
    F.avg("nursing_hppd").alias("avg_hppd")
)

dim_state = dim_state.withColumn(
    "state_sk",
    F.monotonically_increasing_id()
)

dim_state_count = dim_state.count()
print(f"  Generated {dim_state_count} state records")

# =============================================================================
# BUILD FACT TABLE: FACT_DAILY_STAFFING
# =============================================================================
print("\n--- Building fact_daily_staffing ---")

fact_staffing = df.select(
    # Keys
    F.col("provider_id"),
    F.date_format("work_date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    F.col("state"),
    F.col("cy_qtr"),
    F.col("work_date"),
    
    # Census
    F.col("mds_census").alias("patient_census"),
    
    # RN Hours
    F.col("hrs_rn"),
    F.col("hrs_rn_emp"),
    F.col("hrs_rn_ctr"),
    F.col("hrs_rndon"),
    F.col("hrs_rnadmin"),
    
    # LPN Hours
    F.col("hrs_lpn"),
    F.col("hrs_lpn_emp"),
    F.col("hrs_lpn_ctr"),
    F.col("hrs_lpnadmin"),
    
    # CNA Hours
    F.col("hrs_cna"),
    F.col("hrs_cna_emp"),
    F.col("hrs_cna_ctr"),
    
    # Other
    F.col("hrs_natrn"),
    F.col("hrs_medaide"),
    
    # Calculated metrics
    F.col("total_nursing_hours"),
    F.col("total_employed_hours"),
    F.col("total_contract_hours"),
    F.col("nursing_hppd"),
    F.col("contract_ratio"),
    
    # Metadata
    F.col("processed_at")
)

fact_count = fact_staffing.count()
print(f"  Generated {fact_count:,} fact records")

# =============================================================================
# BUILD AGGREGATE: AGG_PROVIDER_MONTHLY
# =============================================================================
print("\n--- Building agg_provider_monthly ---")

agg_provider = df.groupBy(
    "provider_id",
    "provider_name",
    "state",
    F.year("work_date").alias("year"),
    F.month("work_date").alias("month")
).agg(
    # Census
    F.avg("mds_census").alias("avg_daily_census"),
    F.sum("mds_census").alias("total_patient_days"),
    
    # Hours
    F.sum("hrs_rn").alias("total_rn_hours"),
    F.sum("hrs_lpn").alias("total_lpn_hours"),
    F.sum("hrs_cna").alias("total_cna_hours"),
    F.sum("total_nursing_hours").alias("total_nursing_hours"),
    F.sum("total_employed_hours").alias("total_employed_hours"),
    F.sum("total_contract_hours").alias("total_contract_hours"),
    
    # HPPD
    F.avg("nursing_hppd").alias("avg_nursing_hppd"),
    F.min("nursing_hppd").alias("min_nursing_hppd"),
    F.max("nursing_hppd").alias("max_nursing_hppd"),
    
    # Mix
    F.avg("contract_ratio").alias("avg_contract_ratio"),
    
    # Coverage
    F.count("*").alias("days_reported")
).withColumn(
    "year_month",
    F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("month").cast("string"), 2, "0"))
)

agg_provider_count = agg_provider.count()
print(f"  Generated {agg_provider_count:,} provider-month records")

# =============================================================================
# BUILD AGGREGATE: AGG_STATE_MONTHLY
# =============================================================================
print("\n--- Building agg_state_monthly ---")

agg_state = df.groupBy(
    "state",
    F.year("work_date").alias("year"),
    F.month("work_date").alias("month")
).agg(
    F.countDistinct("provider_id").alias("provider_count"),
    F.sum("mds_census").alias("total_patient_days"),
    F.avg("mds_census").alias("avg_daily_census"),
    F.sum("total_nursing_hours").alias("total_nursing_hours"),
    F.sum("hrs_rn").alias("total_rn_hours"),
    F.sum("hrs_lpn").alias("total_lpn_hours"),
    F.sum("hrs_cna").alias("total_cna_hours"),
    F.avg("nursing_hppd").alias("avg_nursing_hppd"),
    F.percentile_approx("nursing_hppd", 0.5).alias("median_nursing_hppd"),
    F.avg("contract_ratio").alias("avg_contract_ratio"),
    F.count("*").alias("total_records")
).withColumn(
    "year_month",
    F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("month").cast("string"), 2, "0"))
)

agg_state_count = agg_state.count()
print(f"  Generated {agg_state_count} state-month records")

# =============================================================================
# BUILD AGGREGATE: AGG_OVERALL_METRICS
# =============================================================================
print("\n--- Building agg_overall_metrics ---")

agg_overall = df.agg(
    F.countDistinct("provider_id").alias("total_providers"),
    F.countDistinct("state").alias("total_states"),
    F.sum("mds_census").alias("total_patient_days"),
    F.sum("total_nursing_hours").alias("total_nursing_hours"),
    F.avg("nursing_hppd").alias("overall_avg_hppd"),
    F.avg("contract_ratio").alias("overall_contract_ratio"),
    F.min("work_date").alias("data_start_date"),
    F.max("work_date").alias("data_end_date"),
    F.countDistinct("work_date").alias("total_days"),
    F.count("*").alias("total_records")
).withColumn("snapshot_date", F.current_date())

print("  Generated overall metrics")

# =============================================================================
# WRITE TO CURATED LAYER
# =============================================================================
print("\n--- Writing to Curated Layer ---")

def write_table(dataframe, table_name, partition_cols=None):
    """Write a table to the curated layer."""
    path = f"{CURATED_PATH}{table_name}/"
    print(f"  Writing {table_name}...")
    
    writer = dataframe.write.mode("overwrite").option("compression", "snappy")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.parquet(path)
    print(f"    ✓ {table_name} → {path}")

# Write dimensions
write_table(dim_date, "dimensions/dim_date")
write_table(dim_provider, "dimensions/dim_provider")
write_table(dim_state, "dimensions/dim_state")

# Write facts (partitioned by state for query efficiency)
write_table(fact_staffing, "facts/fact_daily_staffing", partition_cols=["state"])

# Write aggregates
write_table(agg_provider, "aggregates/agg_provider_monthly", partition_cols=["state"])
write_table(agg_state, "aggregates/agg_state_monthly")
write_table(agg_overall, "aggregates/agg_overall_metrics")

# =============================================================================
# REGISTER TABLES IN GLUE CATALOG (Optional - Crawler will do this)
# =============================================================================
print("\n--- Summary ---")
print(f"  dim_date:              {dim_date_count:,} rows")
print(f"  dim_provider:          {dim_provider_count:,} rows")
print(f"  dim_state:             {dim_state_count} rows")
print(f"  fact_daily_staffing:   {fact_count:,} rows")
print(f"  agg_provider_monthly:  {agg_provider_count:,} rows")
print(f"  agg_state_monthly:     {agg_state_count} rows")
print(f"  agg_overall_metrics:   1 row")

# =============================================================================
# JOB COMPLETE
# =============================================================================
print("\n" + "="*60)
print("JOB COMPLETED SUCCESSFULLY")
print(f"Output location: {CURATED_PATH}")
print("Run the Glue Crawler to update the Data Catalog")
print("="*60)

job.commit()
