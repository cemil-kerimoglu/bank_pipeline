# Bank Data Pipeline

A PySpark-based ETL pipeline for processing banking data from S3, performing transaction cleaning and loan analysis.

## Overview

The pipeline processes Czech banking data:
- **Transaction Processing**: Cleans transaction data and corrects typos
- **Loan Analysis**: Calculates average loan amounts by district

## Quick Start

### Local Development
```bash
# Setup conda environment
conda env create -f environment.yml
conda activate Scigility

# Configure AWS credentials
cp .envSAMPLE .env  # Edit with your AWS credentials

# Run pipeline
python -m main
```

### Docker
```bash
# Build and run (uses micromamba + conda environment)
docker build -t bank-pipeline .
docker run --env-file .env bank-pipeline
```

## Configuration

### Environment Variables (.env)
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_BUCKET_NAME=your_s3_bucket
AWS_REGION=eu-central-1
```

### Pipeline Config (conf/pipeline.yaml)
- Input/output S3 paths
- Data cleaning rules
- Processing parameters

## Data Flow

```
S3 Raw Data → Transaction Cleaning → Parquet Output
            → Loan Analysis      → Parquet Output
```

**Input Files** (S3):
- `account.csv`, `client.csv`, `district.csv`
- `trans.csv`, `loan.csv`, `card.csv`, `disp.csv`, `order.csv`

**Output Files** (Local):
- `data/processed/cleaned-trans/` - Cleaned transactions
- `data/processed/avg-loans/` - Average loans by district

## Architecture

```
main.py                 # Pipeline orchestrator
├── src/jobs/
│   ├── trans_processing.py  # Transaction cleaning
│   └── loan_analysis.py     # Loan calculations
├── src/utils/
│   ├── spark_utils.py       # Spark session management
│   └── data_utils.py        # S3 data operations
└── conf/
    ├── pipeline.yaml        # Pipeline configuration
    └── spark-defaults.conf  # Spark settings
```

## Development

### Testing
```bash
# Activate environment first
conda activate Scigility

# Run tests
pytest tests/

# With coverage
pytest --cov=src tests/
```

### Environment Management
- **Conda Environment**: `environment.yml` defines exact package versions
- **Container Base**: `mambaorg/micromamba` for fast dependency resolution
- **Java Runtime**: OpenJDK 17 (Spark/Hadoop compatible LTS version)

## CI/CD

GitHub Actions workflow (``.github/workflows/ci.yml``):
- **Test Stage**: Automated testing with micromamba environment
- **Build Stage**: Docker image creation using conda environment  
- **Deploy Stage**: Container registry publishing to ghcr.io
- **Environment**: Uses same `environment.yml` for consistency


