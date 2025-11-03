 cat > monitor-kafka.sh << 'SCRIPT'
  #!/bin/bash

  KAFKA_DIR="/usr/hdp/current/kafka-broker"
  BROKER="clu02.softnet.tuc.gr:6667"

  while true; do
      clear
      echo "=========================================="
      echo "  KAFKA TOPICS MONITORING"
      echo "  $(date)"
      echo "=========================================="
      echo ""

      # Record counts
      echo "📊 RECORD COUNTS:"
      INPUT_COUNT=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
          --broker-list $BROKER --topic etl.input.v1 --time -1 2>/dev/null | \
          awk -F: '{sum += $3} END {print sum}')

      CONFIG_COUNT=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
          --broker-list $BROKER --topic etl.config.v1 --time -1 2>/dev/null | \
          awk -F: '{sum += $3} END {print sum}')

      OUTPUT_COUNT=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
          --broker-list $BROKER --topic etl.output.v1 --time -1 2>/dev/null | \
          awk -F: '{sum += $3} END {print sum}')

      echo "  etl.input.v1:  $INPUT_COUNT records"
      echo "  etl.config.v1: $CONFIG_COUNT records"
      echo "  etl.output.v1: $OUTPUT_COUNT records"
      echo ""

      # Consumer lag
      echo "📈 CONSUMER LAG (etl-flink-consumer):"
      $KAFKA_DIR/bin/kafka-consumer-groups.sh \
          --bootstrap-server $BROKER \
          --group etl-flink-consumer \
          --describe 2>/dev/null | grep etl.input | \
          awk '{printf "  Partition %s: Offset=%s Lag=%s\n", $2, $3, $5}'

      TOTAL_LAG=$($KAFKA_DIR/bin/kafka-consumer-groups.sh \
          --bootstrap-server $BROKER \
          --group etl-flink-consumer \
          --describe 2>/dev/null | grep etl.input | \
          awk '{sum += $5} END {print sum}')

      echo ""
      echo "  Total Lag: $TOTAL_LAG records remaining"

      # Progress
      if [ ! -z "$INPUT_COUNT" ] && [ "$INPUT_COUNT" != "0" ]; then
          PROCESSED=$((INPUT_COUNT - TOTAL_LAG))
          PROGRESS=$(awk "BEGIN {printf \"%.2f\", ($PROCESSED/$INPUT_COUNT)*100}")
          echo "  Progress: $PROGRESS%"
      fi

      echo ""
      echo "=========================================="
      echo "Press Ctrl+C to stop monitoring"

      sleep 2
  done
  SCRIPT

  chmod +x 
  