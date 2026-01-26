# ============================================================================
# Healthcare Metrics Pipeline - Step-by-Step Deployment (Windows)
# ============================================================================
# Run each section one at a time to make debugging easier
# ============================================================================

$ErrorActionPreference = "Stop"

# =============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# =============================================================================
$PROJECT_NAME = "healthcare-metrics"
$AWS_REGION = "us-east-1"
$REDSHIFT_PASSWORD = "Healthcare123!"

# Your data file paths - UPDATE THESE
$MASTER_FILE = "C:\Users\okeow\OneDrive\Documents\data\PBJ_Daily_Nurse_Staffing_Q2_2024.csv"
$SUPPORTING_DIR = "C:\Users\okeow\OneDrive\Documents\data\supporting"

# Derived values
$ACCOUNT_ID = aws sts get-caller-identity --query Account --output text
$BUCKET_NAME = "$PROJECT_NAME-datalake-$ACCOUNT_ID"
$STACK_NAME = "$PROJECT_NAME-infrastructure"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "CONFIGURATION" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Account ID:    $ACCOUNT_ID"
Write-Host "Bucket Name:   $BUCKET_NAME"
Write-Host "Stack Name:    $STACK_NAME"
Write-Host "Region:        $AWS_REGION"
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# =============================================================================
# STEP 1: DEPLOY CLOUDFORMATION
# =============================================================================
Write-Host "[STEP 1] Deploying CloudFormation stack..." -ForegroundColor Green

aws cloudformation deploy `
    --template-file cloudformation_template.yaml `
    --stack-name $STACK_NAME `
    --parameter-overrides `
        ProjectName=$PROJECT_NAME `
        Environment=dev `
        RedshiftAdminPassword=$REDSHIFT_PASSWORD `
    --capabilities CAPABILITY_NAMED_IAM `
    --region $AWS_REGION

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] CloudFormation deployment failed!" -ForegroundColor Red
    Write-Host "Check the AWS Console -> CloudFormation for error details" -ForegroundColor Yellow
    exit 1
}

Write-Host "[SUCCESS] CloudFormation stack deployed!" -ForegroundColor Green
Write-Host ""

# =============================================================================
# STEP 2: UPLOAD GLUE SCRIPTS
# =============================================================================
Write-Host "[STEP 2] Uploading Glue scripts to S3..." -ForegroundColor Green

aws s3 cp glue_job_1_raw_to_processed.py "s3://$BUCKET_NAME/scripts/" --region $AWS_REGION
aws s3 cp glue_job_2_processed_to_curated.py "s3://$BUCKET_NAME/scripts/" --region $AWS_REGION

Write-Host "[SUCCESS] Glue scripts uploaded!" -ForegroundColor Green
Write-Host ""

# =============================================================================
# STEP 3: CREATE S3 FOLDER STRUCTURE
# =============================================================================
Write-Host "[STEP 3] Creating S3 folder structure..." -ForegroundColor Green

# Create empty files to establish folder structure
$null | aws s3 cp - "s3://$BUCKET_NAME/raw/nursing_staffing/.keep" --region $AWS_REGION
$null | aws s3 cp - "s3://$BUCKET_NAME/raw/supporting/.keep" --region $AWS_REGION
$null | aws s3 cp - "s3://$BUCKET_NAME/processed/nursing_staffing/.keep" --region $AWS_REGION
$null | aws s3 cp - "s3://$BUCKET_NAME/curated/dimensions/.keep" --region $AWS_REGION
$null | aws s3 cp - "s3://$BUCKET_NAME/curated/facts/.keep" --region $AWS_REGION
$null | aws s3 cp - "s3://$BUCKET_NAME/curated/aggregates/.keep" --region $AWS_REGION

Write-Host "[SUCCESS] Folder structure created!" -ForegroundColor Green
Write-Host ""

# =============================================================================
# STEP 4: UPLOAD DATA FILES
# =============================================================================
Write-Host "[STEP 4] Uploading data files to S3..." -ForegroundColor Green

$TODAY = Get-Date -Format "yyyy-MM-dd"

# Upload master file
if (Test-Path $MASTER_FILE) {
    $MASTER_FILENAME = Split-Path $MASTER_FILE -Leaf
    Write-Host "  Uploading master file: $MASTER_FILENAME"
    aws s3 cp $MASTER_FILE "s3://$BUCKET_NAME/raw/nursing_staffing/ingest_date=$TODAY/$MASTER_FILENAME" --region $AWS_REGION
    Write-Host "  [OK] Master file uploaded" -ForegroundColor Green
} else {
    Write-Host "  [WARN] Master file not found: $MASTER_FILE" -ForegroundColor Yellow
}

# Upload supporting files
if (Test-Path $SUPPORTING_DIR) {
    $FILES = Get-ChildItem -Path $SUPPORTING_DIR -Filter "*.csv"
    foreach ($FILE in $FILES) {
        Write-Host "  Uploading: $($FILE.Name)"
        aws s3 cp $FILE.FullName "s3://$BUCKET_NAME/raw/supporting/ingest_date=$TODAY/$($FILE.Name)" --region $AWS_REGION
    }
    Write-Host "  [OK] Supporting files uploaded" -ForegroundColor Green
} else {
    Write-Host "  [WARN] Supporting directory not found: $SUPPORTING_DIR" -ForegroundColor Yellow
}

Write-Host ""

# =============================================================================
# STEP 5: RUN GLUE JOB 1
# =============================================================================
Write-Host "[STEP 5] Starting Glue Job 1 (Raw to Processed)..." -ForegroundColor Green

$JOB1_RUN = aws glue start-job-run `
    --job-name "$PROJECT_NAME-raw-to-processed" `
    --region $AWS_REGION `
    --output json | ConvertFrom-Json

$JOB1_RUN_ID = $JOB1_RUN.JobRunId
Write-Host "  Job Run ID: $JOB1_RUN_ID"
Write-Host "  Waiting for completion (5-10 minutes)..."

do {
    Start-Sleep -Seconds 30
    $STATUS = aws glue get-job-run `
        --job-name "$PROJECT_NAME-raw-to-processed" `
        --run-id $JOB1_RUN_ID `
        --query 'JobRun.JobRunState' `
        --output text `
        --region $AWS_REGION
    Write-Host "  Status: $STATUS"
} while ($STATUS -eq "RUNNING" -or $STATUS -eq "STARTING" -or $STATUS -eq "WAITING")

if ($STATUS -eq "SUCCEEDED") {
    Write-Host "[SUCCESS] Glue Job 1 completed!" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Glue Job 1 failed with status: $STATUS" -ForegroundColor Red
    Write-Host "Check CloudWatch Logs for details" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# =============================================================================
# STEP 6: RUN GLUE JOB 2
# =============================================================================
Write-Host "[STEP 6] Starting Glue Job 2 (Processed to Curated)..." -ForegroundColor Green

$JOB2_RUN = aws glue start-job-run `
    --job-name "$PROJECT_NAME-processed-to-curated" `
    --region $AWS_REGION `
    --output json | ConvertFrom-Json

$JOB2_RUN_ID = $JOB2_RUN.JobRunId
Write-Host "  Job Run ID: $JOB2_RUN_ID"
Write-Host "  Waiting for completion..."

do {
    Start-Sleep -Seconds 30
    $STATUS = aws glue get-job-run `
        --job-name "$PROJECT_NAME-processed-to-curated" `
        --run-id $JOB2_RUN_ID `
        --query 'JobRun.JobRunState' `
        --output text `
        --region $AWS_REGION
    Write-Host "  Status: $STATUS"
} while ($STATUS -eq "RUNNING" -or $STATUS -eq "STARTING" -or $STATUS -eq "WAITING")

if ($STATUS -eq "SUCCEEDED") {
    Write-Host "[SUCCESS] Glue Job 2 completed!" -ForegroundColor Green
} else {
    Write-Host "[ERROR] Glue Job 2 failed with status: $STATUS" -ForegroundColor Red
    exit 1
}
Write-Host ""

# =============================================================================
# STEP 7: RUN CRAWLER
# =============================================================================
Write-Host "[STEP 7] Running Glue Crawler..." -ForegroundColor Green

aws glue start-crawler --name "$PROJECT_NAME-curated-crawler" --region $AWS_REGION

Write-Host "  Waiting for crawler to complete..."

do {
    Start-Sleep -Seconds 15
    $STATUS = aws glue get-crawler `
        --name "$PROJECT_NAME-curated-crawler" `
        --query 'Crawler.State' `
        --output text `
        --region $AWS_REGION
    Write-Host "  Status: $STATUS"
} while ($STATUS -ne "READY")

Write-Host "[SUCCESS] Crawler completed!" -ForegroundColor Green
Write-Host ""

# =============================================================================
# STEP 8: GET REDSHIFT ENDPOINT
# =============================================================================
Write-Host "[STEP 8] Getting Redshift connection info..." -ForegroundColor Green

$REDSHIFT_ENDPOINT = aws cloudformation describe-stacks `
    --stack-name $STACK_NAME `
    --query "Stacks[0].Outputs[?OutputKey=='RedshiftEndpoint'].OutputValue" `
    --output text `
    --region $AWS_REGION

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "DEPLOYMENT COMPLETE!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "S3 Bucket:         $BUCKET_NAME" -ForegroundColor White
Write-Host "Redshift Endpoint: $REDSHIFT_ENDPOINT" -ForegroundColor White
Write-Host "Redshift Database: analytics" -ForegroundColor White
Write-Host "Redshift Username: admin" -ForegroundColor White
Write-Host "Redshift Password: (the one you set above)" -ForegroundColor White
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Go to AWS Console -> Redshift -> Query Editor v2"
Write-Host "2. Connect to $PROJECT_NAME-workgroup"
Write-Host "3. Run the SQL in redshift_setup.sql"
Write-Host "============================================" -ForegroundColor Cyan
