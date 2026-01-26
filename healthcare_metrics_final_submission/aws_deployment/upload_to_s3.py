"""
Healthcare Metrics Pipeline - S3 Upload Script (Simulated Ingestion)
====================================================================
This script uploads local CSV files to S3, simulating the Google Drive
ingestion step. Use this for testing/development.

Usage:
    python upload_to_s3.py --bucket YOUR_BUCKET_NAME --source /path/to/data

Author: Samuel
Version: 1.0
"""

import os
import sys
import argparse
import boto3
from pathlib import Path
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Default paths (update for your environment)
DEFAULT_CONFIG = {
    'bucket_name': 'healthcare-metrics-datalake',
    'region': 'us-east-1',
    
    # Local source paths (Windows)
    'master_file': r'C:\Users\okeow\OneDrive\Documents\data\PBJ_Daily_Nurse_Staffing_Q2_2024.csv',
    'supporting_dir': r'C:\Users\okeow\OneDrive\Documents\data\supporting',
    
    # S3 prefixes
    'raw_prefix': 'raw/',
    'nursing_prefix': 'raw/nursing_staffing/',
    'supporting_prefix': 'raw/supporting/',
}

# File categorization for S3 paths
FILE_CATEGORIES = {
    'PBJ_Daily_Nurse_Staffing': ('nursing_staffing', 'cy_qtr'),
    'NH_ProviderInfo': ('supporting/provider_info', None),
    'NH_QualityMsr_MDS': ('supporting/quality_mds', None),
    'NH_QualityMsr_Claims': ('supporting/quality_claims', None),
    'NH_HealthCitations': ('supporting/health_citations', None),
    'NH_FireSafetyCitations': ('supporting/fire_citations', None),
    'NH_Penalties': ('supporting/penalties', None),
    'NH_Ownership': ('supporting/ownership', None),
    'NH_StateUSAverages': ('supporting/state_averages', None),
    'NH_SurveyInfo': ('supporting/survey_info', None),
    'NH_InfectionControlViol': ('supporting/infection_control', None),
    'NH_CovidVaxProvider': ('supporting/covid_vax', None),
}


# =============================================================================
# S3 FUNCTIONS
# =============================================================================

def get_s3_client(region: str = 'us-east-1'):
    """Create S3 client with error handling."""
    try:
        s3 = boto3.client('s3', region_name=region)
        # Test credentials
        s3.list_buckets()
        logger.info("AWS credentials verified successfully")
        return s3
    except NoCredentialsError:
        logger.error("AWS credentials not found. Run 'aws configure' first.")
        sys.exit(1)
    except ClientError as e:
        logger.error(f"AWS error: {e}")
        sys.exit(1)


def create_bucket_if_not_exists(s3_client, bucket_name: str, region: str):
    """Create S3 bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket exists: {bucket_name}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            logger.info(f"Creating bucket: {bucket_name}")
            if region == 'us-east-1':
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            logger.info(f"Bucket created: {bucket_name}")
        else:
            raise


def create_folder_structure(s3_client, bucket_name: str):
    """Create the medallion architecture folder structure."""
    folders = [
        'raw/nursing_staffing/',
        'raw/supporting/provider_info/',
        'raw/supporting/quality_mds/',
        'raw/supporting/quality_claims/',
        'raw/supporting/health_citations/',
        'raw/supporting/penalties/',
        'raw/supporting/ownership/',
        'raw/supporting/state_averages/',
        'processed/nursing_staffing/',
        'processed/supporting/',
        'curated/dimensions/',
        'curated/facts/',
        'curated/aggregates/',
        'scripts/',
        'logs/',
    ]
    
    logger.info("Creating folder structure...")
    for folder in folders:
        s3_client.put_object(Bucket=bucket_name, Key=folder)
    logger.info(f"Created {len(folders)} folders")


def determine_s3_key(file_name: str, ingest_date: str) -> str:
    """Determine the S3 key based on file name."""
    
    # Check each category
    for pattern, (category, partition_type) in FILE_CATEGORIES.items():
        if pattern in file_name:
            if partition_type == 'cy_qtr':
                # Extract quarter from filename
                if 'Q1_2024' in file_name or 'Q1' in file_name:
                    qtr = '2024Q1'
                elif 'Q2_2024' in file_name or 'Q2' in file_name:
                    qtr = '2024Q2'
                elif 'Q3_2024' in file_name or 'Q3' in file_name:
                    qtr = '2024Q3'
                elif 'Q4_2024' in file_name or 'Q4' in file_name:
                    qtr = '2024Q4'
                else:
                    qtr = '2024Q2'  # Default
                return f"raw/{category}/cy_qtr={qtr}/ingest_date={ingest_date}/{file_name}"
            else:
                return f"raw/{category}/ingest_date={ingest_date}/{file_name}"
    
    # Default: unknown category
    return f"raw/other/ingest_date={ingest_date}/{file_name}"


def upload_file(s3_client, bucket_name: str, local_path: str, s3_key: str) -> bool:
    """Upload a single file to S3."""
    try:
        file_size = os.path.getsize(local_path)
        logger.info(f"Uploading: {os.path.basename(local_path)} ({file_size / 1024 / 1024:.1f} MB)")
        logger.info(f"  → s3://{bucket_name}/{s3_key}")
        
        # Use multipart upload for large files
        config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=50 * 1024 * 1024,  # 50 MB
            max_concurrency=10,
            multipart_chunksize=10 * 1024 * 1024,  # 10 MB
        )
        
        s3_client.upload_file(
            local_path,
            bucket_name,
            s3_key,
            Config=config,
            ExtraArgs={'ContentType': 'text/csv'}
        )
        
        logger.info(f"  ✓ Upload complete")
        return True
        
    except Exception as e:
        logger.error(f"  ✗ Upload failed: {e}")
        return False


# =============================================================================
# MAIN UPLOAD FUNCTION
# =============================================================================

def upload_healthcare_data(
    bucket_name: str,
    master_file: str = None,
    supporting_dir: str = None,
    region: str = 'us-east-1',
    create_bucket: bool = True
):
    """
    Upload healthcare data files to S3.
    
    Args:
        bucket_name: S3 bucket name
        master_file: Path to master nursing staffing CSV
        supporting_dir: Path to directory with supporting files
        region: AWS region
        create_bucket: Create bucket if it doesn't exist
    """
    logger.info("="*60)
    logger.info("HEALTHCARE DATA - S3 UPLOAD")
    logger.info(f"Bucket: {bucket_name}")
    logger.info(f"Region: {region}")
    logger.info("="*60)
    
    # Get S3 client
    s3 = get_s3_client(region)
    
    # Create bucket if needed
    if create_bucket:
        create_bucket_if_not_exists(s3, bucket_name, region)
        create_folder_structure(s3, bucket_name)
    
    # Track results
    results = {
        'uploaded': [],
        'failed': [],
        'total_bytes': 0
    }
    
    ingest_date = datetime.now().strftime('%Y-%m-%d')
    
    # Upload master file
    if master_file and os.path.exists(master_file):
        logger.info("\n--- Uploading Master File ---")
        file_name = os.path.basename(master_file)
        s3_key = determine_s3_key(file_name, ingest_date)
        
        if upload_file(s3, bucket_name, master_file, s3_key):
            results['uploaded'].append(file_name)
            results['total_bytes'] += os.path.getsize(master_file)
        else:
            results['failed'].append(file_name)
    else:
        logger.warning(f"Master file not found: {master_file}")
    
    # Upload supporting files
    if supporting_dir and os.path.exists(supporting_dir):
        logger.info("\n--- Uploading Supporting Files ---")
        
        for file_path in Path(supporting_dir).glob('*.csv'):
            file_name = file_path.name
            s3_key = determine_s3_key(file_name, ingest_date)
            
            if upload_file(s3, bucket_name, str(file_path), s3_key):
                results['uploaded'].append(file_name)
                results['total_bytes'] += file_path.stat().st_size
            else:
                results['failed'].append(file_name)
    else:
        logger.warning(f"Supporting directory not found: {supporting_dir}")
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("UPLOAD SUMMARY")
    logger.info("="*60)
    logger.info(f"Files uploaded: {len(results['uploaded'])}")
    logger.info(f"Files failed:   {len(results['failed'])}")
    logger.info(f"Total size:     {results['total_bytes'] / 1024 / 1024:.1f} MB")
    
    if results['uploaded']:
        logger.info("\nUploaded files:")
        for f in results['uploaded']:
            logger.info(f"  ✓ {f}")
    
    if results['failed']:
        logger.info("\nFailed files:")
        for f in results['failed']:
            logger.info(f"  ✗ {f}")
    
    logger.info("="*60)
    
    return results


# =============================================================================
# VERIFICATION FUNCTION
# =============================================================================

def verify_upload(bucket_name: str, region: str = 'us-east-1'):
    """Verify files were uploaded correctly."""
    logger.info("\n--- Verifying Upload ---")
    
    s3 = get_s3_client(region)
    
    # List objects in raw folder
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix='raw/',
        MaxKeys=100
    )
    
    if 'Contents' not in response:
        logger.warning("No files found in raw/ folder")
        return
    
    logger.info(f"Files in s3://{bucket_name}/raw/:")
    
    total_size = 0
    for obj in response['Contents']:
        key = obj['Key']
        size = obj['Size']
        total_size += size
        if size > 0:  # Skip folder placeholders
            logger.info(f"  {key} ({size / 1024 / 1024:.1f} MB)")
    
    logger.info(f"\nTotal: {total_size / 1024 / 1024:.1f} MB")


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Upload healthcare data to S3 (simulated ingestion)'
    )
    
    parser.add_argument(
        '--bucket', '-b',
        default=DEFAULT_CONFIG['bucket_name'],
        help='S3 bucket name'
    )
    
    parser.add_argument(
        '--region', '-r',
        default=DEFAULT_CONFIG['region'],
        help='AWS region'
    )
    
    parser.add_argument(
        '--master', '-m',
        default=DEFAULT_CONFIG['master_file'],
        help='Path to master nursing staffing CSV'
    )
    
    parser.add_argument(
        '--supporting', '-s',
        default=DEFAULT_CONFIG['supporting_dir'],
        help='Path to supporting files directory'
    )
    
    parser.add_argument(
        '--verify', '-v',
        action='store_true',
        help='Verify upload after completion'
    )
    
    parser.add_argument(
        '--skip-bucket-create',
        action='store_true',
        help='Skip bucket creation (bucket must exist)'
    )
    
    args = parser.parse_args()
    
    # Run upload
    results = upload_healthcare_data(
        bucket_name=args.bucket,
        master_file=args.master,
        supporting_dir=args.supporting,
        region=args.region,
        create_bucket=not args.skip_bucket_create
    )
    
    # Verify if requested
    if args.verify:
        verify_upload(args.bucket, args.region)
    
    # Exit code
    if results['failed']:
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
