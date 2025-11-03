cd /home/avoutas/boutasThesis

cat > run-benchmark.sh << 'SCRIPT'
#!/bin/bash

################################################################################
# ETL Flink Benchmark Script - All-in-One Version
################################################################################
# Usage: ./run-benchmark.sh <parallelism> [options]
#
# Examples:
#   ./run-benchmark.sh 8                    # Run with parallelism=8
#   ./run-benchmark.sh 8 --no-cleanup       # Skip checkpoint cleanup
#   ./run-benchmark.sh 8 --show-config      # Show Flink config and exit
#
# This script handles:
#   1. Job cancellation
#   2. Checkpoint/savepoint cleanup (HDFS or local)
#   3. Kafka consumer group reset
#   4. Fresh job submission
#   5. Complete monitoring with verification
################################################################################

# Configuration
KAFKA_BROKER="clu02.softnet.tuc.gr:6667"
CONSUMER_GROUP="etl-flink-consumer"
FLINK_CONF="/usr/local/flink/conf/flink-conf.yaml"
MAX_SAFE_PARALLELISM=16

# Parse arguments
PARALLELISM=$1
SKIP_CLEANUP=false
SHOW_CONFIG=false

shift
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cleanup)
            SKIP_CLEANUP=true
            shift
            ;;
        --show-config)
            SHOW_CONFIG=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

################################################################################
# SHOW CONFIG MODE
################################################################################
if [ "$SHOW_CONFIG" = true ]; then
    echo "=========================================="
    echo "Flink Configuration"
    echo "=========================================="
    if [ -f "$FLINK_CONF" ]; then
        echo ""
        echo "Checkpoint directory:"
        grep "state.checkpoints.dir" $FLINK_CONF || echo "  (not configured)"
        echo ""
        echo "Savepoint directory:"
        grep "state.savepoints.dir" $FLINK_CONF || echo "  (not configured)"
        echo ""
        echo "Full configuration:"
        cat $FLINK_CONF
    else
        echo "ERROR: Config file not found at $FLINK_CONF"
    fi
    exit 0
fi

################################################################################
# VALIDATION
################################################################################
if [ "$#" -lt 0 ] || [ -z "$PARALLELISM" ]; then
    echo "Usage: $0 <parallelism> [options]"
    echo ""
    echo "Options:"
    echo "  --no-cleanup     Skip checkpoint cleanup (faster, but may cause issues)"
    echo "  --show-config    Show Flink configuration and exit"
    echo ""
    echo "Examples:"
    echo "  $0 4             Run with parallelism=4"
    echo "  $0 8             Run with parallelism=8"
    echo "  $0 11            Run with parallelism=11"
    echo ""
    echo "Safe parallelism range: 1-${MAX_SAFE_PARALLELISM}"
    exit 1
fi

# Validate parallelism
if ! [[ "$PARALLELISM" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Parallelism must be a number"
    exit 1
fi

if [ "$PARALLELISM" -gt "$MAX_SAFE_PARALLELISM" ]; then
    echo "=========================================="
    echo "⚠️  WARNING: High Parallelism Detected"
    echo "=========================================="
    echo "Requested parallelism: $PARALLELISM"
    echo "Maximum safe value:    $MAX_SAFE_PARALLELISM"
    echo ""
    echo "Your cluster has only 33 task slots (11 workers × 3 slots)."
    echo "Parallelism > $MAX_SAFE_PARALLELISM may cause the job to stay in"
    echo "CREATED state indefinitely (insufficient resources)."
    echo ""
    read -p "Continue anyway? (yes/no): " CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 1
    fi
    echo ""
fi

################################################################################
# START BENCHMARK
################################################################################
echo "=========================================="
echo "ETL Flink Benchmark"
echo "=========================================="
echo "Parallelism:       $PARALLELISM"
echo "Skip cleanup:      $SKIP_CLEANUP"
echo "Kafka broker:      $KAFKA_BROKER"
echo "Consumer group:    $CONSUMER_GROUP"
echo "=========================================="
echo ""

################################################################################
# STEP 1: STOP EXISTING JOBS
################################################################################
echo "[1/7] Stopping any existing Flink jobs..."
cd /usr/local/flink

JOBS=$(./bin/flink list -r 2>/dev/null | grep -v "No running jobs" | awk 'NR>3 {print $4}')
if [ -z "$JOBS" ]; then
    echo "  ✓ No running jobs found"
else
    for job in $JOBS; do
        echo "  Cancelling job: $job"
        ./bin/flink cancel $job 2>/dev/null || true
    done

    # CRITICAL: Wait for jobs to actually stop and release consumer groups
    echo "  Waiting for jobs to fully terminate..."
    MAX_WAIT=30
    for i in $(seq 1 $MAX_WAIT); do
        RUNNING_JOBS=$(./bin/flink list -r 2>/dev/null | grep -v "No running jobs" | wc -l)
        if [ "$RUNNING_JOBS" -le 3 ]; then
            echo "  ✓ All jobs stopped after ${i}s"
            break
        fi
        if [ $((i % 5)) -eq 0 ]; then
            echo "    Still waiting... (${i}/${MAX_WAIT}s)"
        fi
        sleep 1
    done

    # Extra wait for consumer group cleanup in Kafka
    echo "  Waiting for Kafka consumer group to release..."
    sleep 10
    echo "  ✓ Jobs stopped"
fi
echo ""

################################################################################
# STEP 2: CLEAN FLINK CHECKPOINTS/SAVEPOINTS
################################################################################
if [ "$SKIP_CLEANUP" = true ]; then
    echo "[2/7] Skipping checkpoint cleanup (--no-cleanup flag)"
    echo "  ⚠️  WARNING: Job may resume from previous state!"
    echo ""
else
    echo "[2/7] Cleaning Flink checkpoints and savepoints..."

    # Try multiple cleanup strategies
    CLEANED=false

    # Strategy 1: Read from flink-conf.yaml
    if [ -f "$FLINK_CONF" ]; then
        # Handle both "key: value" and "key:value" formats, remove comments
        CHECKPOINT_DIR=$(grep "^state.checkpoints.dir" $FLINK_CONF 2>/dev/null | sed 's/#.*//' | awk -F: '{for(i=2;i<=NF;i++) printf "%s%s", $i, (i<NF?" ":""); print ""}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | tr -d '"' || echo "")
        SAVEPOINT_DIR=$(grep "^state.savepoints.dir" $FLINK_CONF 2>/dev/null | sed 's/#.*//' | awk -F: '{for(i=2;i<=NF;i++) printf "%s%s", $i, (i<NF?" ":""); print ""}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | tr -d '"' || echo "")

        # Clean checkpoints from config
        if [ ! -z "$CHECKPOINT_DIR" ] && [ "$CHECKPOINT_DIR" != "state.checkpoints.dir" ]; then
            echo "  Checkpoint dir (from config): $CHECKPOINT_DIR"
            if [[ "$CHECKPOINT_DIR" == hdfs://* ]]; then
                hdfs dfs -rm -r -f "$CHECKPOINT_DIR"/* 2>/dev/null && echo "    ✓ Cleaned HDFS checkpoints" && CLEANED=true || echo "    ⚠️  Could not clean"
            elif [[ "$CHECKPOINT_DIR" == file://* ]]; then
                LOCAL_DIR="${CHECKPOINT_DIR#file://}"
                rm -rf "$LOCAL_DIR"/* 2>/dev/null && echo "    ✓ Cleaned local checkpoints" && CLEANED=true || echo "    ⚠️  Could not clean"
            else
                # Try as local path
                rm -rf "$CHECKPOINT_DIR"/* 2>/dev/null && echo "    ✓ Cleaned checkpoints" && CLEANED=true || echo "    ⚠️  Could not clean"
            fi
        fi

        # Clean savepoints from config
        if [ ! -z "$SAVEPOINT_DIR" ] && [ "$SAVEPOINT_DIR" != "state.savepoints.dir" ]; then
            echo "  Savepoint dir (from config): $SAVEPOINT_DIR"
            if [[ "$SAVEPOINT_DIR" == hdfs://* ]]; then
                hdfs dfs -rm -r -f "$SAVEPOINT_DIR"/* 2>/dev/null && echo "    ✓ Cleaned HDFS savepoints" || echo "    ⚠️  Could not clean"
            elif [[ "$SAVEPOINT_DIR" == file://* ]]; then
                LOCAL_DIR="${SAVEPOINT_DIR#file://}"
                rm -rf "$LOCAL_DIR"/* 2>/dev/null && echo "    ✓ Cleaned local savepoints" || echo "    ⚠️  Could not clean"
            else
                rm -rf "$SAVEPOINT_DIR"/* 2>/dev/null && echo "    ✓ Cleaned savepoints" || echo "    ⚠️  Could not clean"
            fi
        fi
    fi

    # Strategy 2: Try common default locations
    if [ "$CLEANED" = false ]; then
        echo "  Trying default checkpoint locations..."

        # Common local paths
        for DIR in "/tmp/flink-checkpoints" "/var/tmp/flink-checkpoints" "$HOME/flink-checkpoints" "/usr/local/flink/checkpoints"; do
            if [ -d "$DIR" ]; then
                rm -rf "$DIR"/* 2>/dev/null && echo "    ✓ Cleaned $DIR" && CLEANED=true
            fi
        done

        # Try HDFS default
        hdfs dfs -rm -r -f /flink/checkpoints/* 2>/dev/null && echo "    ✓ Cleaned HDFS /flink/checkpoints" && CLEANED=true || true
        hdfs dfs -rm -r -f /user/$(whoami)/flink/checkpoints/* 2>/dev/null && echo "    ✓ Cleaned HDFS /user/$(whoami)/flink/checkpoints" && CLEANED=true || true
    fi

    if [ "$CLEANED" = false ]; then
        echo "  ⚠️  WARNING: Could not locate or clean checkpoints!"
        echo "  Job may resume from previous state."
        echo "  To manually clean, run: ./run-benchmark.sh 8 --show-config"
    fi

    echo "  Waiting for cleanup to propagate..."
    sleep 3
    echo "  ✓ Cleanup attempt complete"
    echo ""
fi

################################################################################
# STEP 3: RESET KAFKA CONSUMER GROUP
################################################################################
echo "[3/7] Resetting Kafka consumer group..."
/usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh \
    --bootstrap-server $KAFKA_BROKER \
    --group $CONSUMER_GROUP \
    --delete 2>&1 | grep -v "^$" || true

echo "  Waiting for deletion to propagate across cluster..."
sleep 15

# Verify deletion
GROUP_CHECK=$(/usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh \
    --bootstrap-server $KAFKA_BROKER \
    --group $CONSUMER_GROUP \
    --describe 2>&1)

if echo "$GROUP_CHECK" | grep -q "does not exist"; then
    echo "  ✓ Consumer group successfully deleted"
else
    echo "  ⚠️  Consumer group still exists, waiting additional 10 seconds..."
    sleep 10
fi
echo "  ✓ Consumer group reset"
echo ""

################################################################################
# STEP 4: VERIFY KAFKA TOPICS
################################################################################
echo "[4/7] Verifying Kafka topics..."
cd /usr/hdp/current/kafka-broker

# Get total records in input topic
TOTAL_RECORDS=$(bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER \
    --topic etl.input.v1 \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')

if [ -z "$TOTAL_RECORDS" ] || [ "$TOTAL_RECORDS" = "0" ]; then
    echo "  ⚠️  WARNING: etl.input.v1 appears empty!"
    echo "  Total records: ${TOTAL_RECORDS:-0}"
    echo ""
    read -p "  Continue anyway? (yes/no): " CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        echo "Aborted."
        exit 1
    fi
else
    echo "  ✓ etl.input.v1: $TOTAL_RECORDS records"
fi
echo ""

################################################################################
# STEP 5: SUBMIT FLINK JOB
################################################################################
echo "[5/7] Submitting Flink job..."
START_TIME=$(date +%s)
echo "  Start time: $(date -d @$START_TIME)"

cd /usr/local/flink
# Submit job (Flink will auto-restore from checkpoints if found)
# Note: Checkpoint cleanup above should prevent restoration
./bin/flink run -p $PARALLELISM -d /home/avoutas/boutasThesis/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1 2>&1 | tee /tmp/flink_submit_$$.log

# Check if submission succeeded
if grep -q "Job has been submitted with JobID" /tmp/flink_submit_$$.log; then
    JOB_ID=$(grep "Job has been submitted with JobID" /tmp/flink_submit_$$.log | awk '{print $NF}')
    echo "  ✓ Job submitted: $JOB_ID"
else
    echo "  ✗ Job submission failed!"
    cat /tmp/flink_submit_$$.log
    exit 1
fi
rm -f /tmp/flink_submit_$$.log
echo ""

################################################################################
# STEP 6: WAIT FOR JOB INITIALIZATION
################################################################################
echo "[6/7] Waiting for job initialization..."
# Dynamic wait based on parallelism: 10s base + 1.5s per parallelism
INIT_WAIT=$((10 + PARALLELISM * 3 / 2))
echo "  Wait time: ${INIT_WAIT}s (based on parallelism=$PARALLELISM)"

for ((i=1; i<=INIT_WAIT; i++)); do
    if [ $((i % 5)) -eq 0 ]; then
        echo "  Progress: $i/${INIT_WAIT}s"
    fi
    sleep 1
done

# Verify consumer group is initialized
echo "  Verifying consumer group initialization..."
cd /usr/hdp/current/kafka-broker

RETRY_COUNT=0
CONSUMER_INITIALIZED=0

while [ $RETRY_COUNT -lt 30 ]; do
    CURRENT_OFFSET=$(bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $CONSUMER_GROUP \
        --describe 2>/dev/null | \
        grep etl.input.v1 | \
        awk '{sum += $3} END {print sum}')

    if [ ! -z "$CURRENT_OFFSET" ] && [ "$CURRENT_OFFSET" != "0" ]; then
        echo "  ✓ Consumer group active, starting from offset: $CURRENT_OFFSET"

        # Warn if not starting from 0
        if [ "$CURRENT_OFFSET" != "0" ] && [ "$CURRENT_OFFSET" -gt 100 ]; then
            echo ""
            echo "  ⚠️  WARNING: Not starting from offset 0!"
            echo "  This suggests checkpoint restoration is active."
            echo "  Results may be incomplete."
            echo "  Flink dashboard: http://clu01.softnet.tuc.gr:8081"
            echo ""
        fi

        CONSUMER_INITIALIZED=1
        break
    fi

    if [ $((RETRY_COUNT % 5)) -eq 0 ] && [ $RETRY_COUNT -gt 0 ]; then
        echo "  Waiting for consumer to start... ($RETRY_COUNT/30s)"
    fi
    sleep 1
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $CONSUMER_INITIALIZED -eq 0 ]; then
    echo ""
    echo "  ✗ ERROR: Consumer group did not initialize after 30 seconds!"
    echo ""
    echo "  Possible causes:"
    echo "  - Job failed to start (check Flink dashboard)"
    echo "  - Insufficient task slots (parallelism=$PARALLELISM too high)"
    echo "  - Kafka connection issues"
    echo ""
    echo "  Flink dashboard: http://clu01.softnet.tuc.gr:8081"
    echo "  Check job status: cd /usr/local/flink && ./bin/flink list"
    exit 1
fi
echo ""

################################################################################
# STEP 7: MONITOR CONSUMPTION
################################################################################
echo "[7/7] Monitoring consumption progress..."
echo "=========================================="

PREV_OFFSET=0
STABLE_COUNT=0
COMPLETION_TIME=0
LAST_PROGRESS_TIME=$(date +%s)

while true; do
    # Get current offset
    CURRENT_OFFSET=$(bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $CONSUMER_GROUP \
        --describe 2>/dev/null | \
        grep etl.input.v1 | \
        awk '{sum += $3} END {print sum}')

    # Skip if not available
    if [ -z "$CURRENT_OFFSET" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S'): Consumer group unavailable, retrying..."
        sleep 1
        continue
    fi

    # Get lag
    TOTAL_LAG=$(bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $CONSUMER_GROUP \
        --describe 2>/dev/null | \
        grep etl.input.v1 | \
        awk '{sum += $5} END {print sum}')

    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    # Calculate throughput
    if [ $ELAPSED -gt 0 ]; then
        THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $CURRENT_OFFSET/$ELAPSED}")
    else
        THROUGHPUT="0.00"
    fi

    # Calculate percentage
    if [ ! -z "$TOTAL_RECORDS" ] && [ "$TOTAL_RECORDS" != "0" ]; then
        PERCENT=$(awk "BEGIN {printf \"%.1f\", ($CURRENT_OFFSET/$TOTAL_RECORDS)*100}")
    else
        PERCENT="0.0"
    fi

    echo "$(date '+%H:%M:%S'): Offset=$CURRENT_OFFSET | Lag=$TOTAL_LAG | Progress=${PERCENT}% | Throughput=${THROUGHPUT} rec/s | Elapsed=${ELAPSED}s"

    # Check for progress
    if [ "$CURRENT_OFFSET" != "$PREV_OFFSET" ]; then
        LAST_PROGRESS_TIME=$CURRENT_TIME
        STABLE_COUNT=0
    else
        # No progress detected
        if [ "$CURRENT_OFFSET" != "0" ]; then
            STABLE_COUNT=$((STABLE_COUNT + 1))

            # Record completion time on first detection
            if [ $STABLE_COUNT -eq 1 ]; then
                COMPLETION_TIME=$CURRENT_TIME
            fi

            # Confirm stable for 5 seconds
            if [ $STABLE_COUNT -ge 5 ]; then
                DURATION=$((COMPLETION_TIME - START_TIME))
                FINAL_THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $CURRENT_OFFSET/$DURATION}")

                echo ""
                echo "=========================================="
                echo "✅ CONSUMPTION COMPLETED"
                echo "=========================================="
                echo "Total records in Kafka:  $TOTAL_RECORDS"
                echo "Total records processed: $CURRENT_OFFSET"
                echo "Duration:                ${DURATION}s"
                echo "Parallelism:             $PARALLELISM"
                echo "Throughput:              ${FINAL_THROUGHPUT} rec/sec"
                echo ""

                # Verify completeness
                if [ "$CURRENT_OFFSET" -eq "$TOTAL_RECORDS" ]; then
                    echo "Status:                  ✅ COMPLETE (100%)"
                    echo "=========================================="
                    echo ""
                    echo "🎯 Perfect benchmark! All records consumed."
                else
                    MISSING=$((TOTAL_RECORDS - CURRENT_OFFSET))
                    PERCENT=$(awk "BEGIN {printf \"%.2f\", ($CURRENT_OFFSET/$TOTAL_RECORDS)*100}")
                    echo "Status:                  ⚠️  INCOMPLETE (${PERCENT}%)"
                    echo "Missing records:         $MISSING"
                    echo "=========================================="
                    echo ""
                    echo "⚠️  Possible causes of incomplete consumption:"
                    echo "  1. Checkpoint restoration (job resumed from old state)"
                    echo "  2. Kafka topic was not clean before starting"
                    echo "  3. Consumer group was not fully reset"
                    echo ""
                    echo "Solutions:"
                    echo "  - Run: ./clean-flink-state.sh (manual cleanup)"
                    echo "  - Or rebuild job with checkpointing disabled"
                    echo "  - Check: http://clu01.softnet.tuc.gr:8081"
                fi
                echo ""
                echo "Flink dashboard: http://clu01.softnet.tuc.gr:8081/jobs/$JOB_ID"
                echo ""
                break
            fi
        fi
    fi

    # Timeout detection (no progress for 60 seconds)
    TIME_SINCE_PROGRESS=$((CURRENT_TIME - LAST_PROGRESS_TIME))
    if [ $TIME_SINCE_PROGRESS -gt 60 ] && [ "$CURRENT_OFFSET" != "0" ]; then
        echo ""
        echo "=========================================="
        echo "⚠️  TIMEOUT: No progress for 60 seconds"
        echo "=========================================="
        echo "Last offset:             $CURRENT_OFFSET"
        echo "Total in Kafka:          $TOTAL_RECORDS"
        echo "Remaining:               $((TOTAL_RECORDS - CURRENT_OFFSET))"
        echo ""
        echo "Possible issues:"
        echo "  - Flink job failed (check dashboard)"
        echo "  - Processing bottleneck"
        echo "  - Kafka consumer lag stuck"
        echo ""
        echo "Flink dashboard: http://clu01.softnet.tuc.gr:8081/jobs/$JOB_ID"
        echo "=========================================="
        break
    fi

    PREV_OFFSET=$CURRENT_OFFSET
    sleep 1
done
SCRIPT

chmod +x run-benchmark.sh