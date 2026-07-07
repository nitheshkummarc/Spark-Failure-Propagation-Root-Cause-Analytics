#!/bin/bash
# ============================================================================
# Spark Job Submission Script
# Submits the RCA application to YARN cluster
# ============================================================================

set -e

# Configuration
JAR_PATH="${JAR_PATH:-target/scala-2.12/spark-rca-assembly.jar}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
MASTER="${MASTER:-yarn}"
DEPLOY_MODE="${DEPLOY_MODE:-cluster}"

# Resource Configuration
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-4g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
NUM_EXECUTORS="${NUM_EXECUTORS:-4}"

# Application Arguments
APP_COMMAND="${1:-info}"
shift || true
APP_ARGS="$@"

echo "=============================================="
echo "Spark Job Submission"
echo "=============================================="
echo "JAR: ${JAR_PATH}"
echo "Master: ${MASTER}"
echo "Deploy Mode: ${DEPLOY_MODE}"
echo "Command: ${APP_COMMAND}"
echo "=============================================="
echo "Resources:"
echo "  Driver Memory: ${DRIVER_MEMORY}"
echo "  Executor Memory: ${EXECUTOR_MEMORY}"
echo "  Executor Cores: ${EXECUTOR_CORES}"
echo "  Num Executors: ${NUM_EXECUTORS}"
echo "=============================================="

# Check if JAR exists
if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR file not found: $JAR_PATH"
    echo "Build the project first: sbt assembly"
    exit 1
fi

# Submit job
$SPARK_HOME/bin/spark-submit \
    --class com.sparkrca.Main \
    --master "$MASTER" \
    --deploy-mode "$DEPLOY_MODE" \
    --driver-memory "$DRIVER_MEMORY" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --executor-cores "$EXECUTOR_CORES" \
    --num-executors "$NUM_EXECUTORS" \
    --conf "spark.eventLog.enabled=true" \
    --conf "spark.eventLog.dir=hdfs:///project/spark-logs" \
    --conf "spark.yarn.submit.waitAppCompletion=true" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.dynamicAllocation.enabled=false" \
    "$JAR_PATH" \
    "$APP_COMMAND" $APP_ARGS

echo ""
echo "=============================================="
echo "Job submitted successfully!"
echo "=============================================="
