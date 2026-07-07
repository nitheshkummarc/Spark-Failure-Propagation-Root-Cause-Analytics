#!/bin/bash
# ============================================================================
# HDFS Ingestion Script
# Uploads TPC-H raw data to HDFS
# ============================================================================

set -e

# Configuration
LOCAL_DATA_DIR="${1:-./tpch_data}"
HDFS_RAW_PATH="${2:-/project/tpch/raw}"
HDFS_PARQUET_PATH="${3:-/project/tpch/parquet}"

echo "=============================================="
echo "HDFS Data Ingestion"
echo "=============================================="
echo "Local Source: ${LOCAL_DATA_DIR}"
echo "HDFS Target: ${HDFS_RAW_PATH}"
echo "=============================================="

# Check if local data exists
if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo "ERROR: Local data directory not found: $LOCAL_DATA_DIR"
    echo "Run generate_tpch_data.sh first"
    exit 1
fi

# Check if HDFS is accessible
if ! hdfs dfs -ls / > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to HDFS"
    echo "Make sure Hadoop/HDFS is running"
    exit 1
fi

# Create HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p "$HDFS_RAW_PATH"
hdfs dfs -mkdir -p "$HDFS_PARQUET_PATH"
hdfs dfs -mkdir -p "/project/spark-logs"
hdfs dfs -mkdir -p "/project/models"
hdfs dfs -mkdir -p "/project/features"
hdfs dfs -mkdir -p "/project/preprocess"
hdfs dfs -mkdir -p "/project/lib"

# Upload compiled JAR if it exists locally
echo "Uploading Spark RCA Assembly JAR to HDFS..."
if [ -f "target/scala-2.12/spark-rca-assembly.jar" ]; then
    hdfs dfs -put -f "target/scala-2.12/spark-rca-assembly.jar" "/project/lib/spark-rca-assembly.jar"
    echo "  Uploaded from target/scala-2.12/"
elif [ -f "/opt/spark/jars/spark-rca-assembly.jar" ]; then
    hdfs dfs -put -f "/opt/spark/jars/spark-rca-assembly.jar" "/project/lib/spark-rca-assembly.jar"
    echo "  Uploaded from /opt/spark/jars/"
else
    echo "  WARNING: spark-rca-assembly.jar not found! You must build it and upload it to /project/lib/spark-rca-assembly.jar in HDFS manually."
fi

# Upload data files
echo "Uploading TPC-H data to HDFS..."
for file in "$LOCAL_DATA_DIR"/*.tbl; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        echo "  Uploading: $filename"
        hdfs dfs -put -f "$file" "$HDFS_RAW_PATH/$filename"
    fi
done

# Verify upload
echo ""
echo "Uploaded files:"
hdfs dfs -ls -h "$HDFS_RAW_PATH"

# Show total size
echo ""
TOTAL_SIZE=$(hdfs dfs -du -s -h "$HDFS_RAW_PATH" | awk '{print $1}')
echo "=============================================="
echo "Ingestion complete!"
echo "Total size in HDFS: $TOTAL_SIZE"
echo "=============================================="
