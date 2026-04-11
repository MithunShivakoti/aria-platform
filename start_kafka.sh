#!/bin/bash
KAFKA_DIR="$(dirname "$0")/kafka_2.13-3.7.0"

if [ ! -d "$KAFKA_DIR" ]; then
  echo "ERROR: Kafka not found at $KAFKA_DIR"
  exit 1
fi

cd "$KAFKA_DIR"

echo "Starting Zookeeper..."
bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
ZK_PID=$!
echo "Zookeeper PID: $ZK_PID"
sleep 6

echo "Starting Kafka broker..."
bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &
KAFKA_PID=$!
echo "Kafka PID: $KAFKA_PID"
sleep 6

echo "Creating topic: sensor-readings..."
bin/kafka-topics.sh --create \
  --topic sensor-readings \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists 2>&1

echo ""
echo "Kafka ready on localhost:9092"
echo "To check topic: bin/kafka-topics.sh --list --bootstrap-server localhost:9092"
echo "To stop: kill $ZK_PID $KAFKA_PID"
