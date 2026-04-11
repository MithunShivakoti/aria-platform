#!/bin/bash
DIR="$(dirname "$0")"
cd "$DIR"

echo "============================================"
echo " ARIA v2 — Full Stack Startup"
echo "============================================"

# 1. Start Kafka + Zookeeper
echo ""
echo "[1/4] Starting Kafka..."
bash start_kafka.sh
sleep 8

# 2. Start Kafka producer
echo ""
echo "[2/4] Starting Kafka producer..."
python kafka_producer.py &
PRODUCER_PID=$!
echo "Producer PID: $PRODUCER_PID"
sleep 2

# 3. Start WebSocket bridge
echo ""
echo "[3/4] Starting WebSocket bridge..."
python kafka_bridge.py &
BRIDGE_PID=$!
echo "Bridge PID: $BRIDGE_PID"
sleep 1

# 4. Start Node server
echo ""
echo "[4/4] Starting ARIA Node server..."
node server.js &
NODE_PID=$!
sleep 1

echo ""
echo "============================================"
echo " All systems running!"
echo "============================================"
echo " Dashboard  : http://localhost:3000/ARIA_v2.html"
echo " Kafka       : localhost:9092"
echo " WS Bridge  : ws://localhost:8765"
echo ""
echo " Open the dashboard and click  📡 GO LIVE"
echo ""
echo " To stop everything:"
echo "   kill $PRODUCER_PID $BRIDGE_PID $NODE_PID"
echo "   kill \$(lsof -t -i:9092) \$(lsof -t -i:8765) 2>/dev/null"
echo "============================================"
