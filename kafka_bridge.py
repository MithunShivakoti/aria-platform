import asyncio
import json
import threading
import websockets
from kafka import KafkaConsumer

# Shared async queue — Kafka thread puts messages here, WebSocket coroutine reads them
message_queue = asyncio.Queue()

# All connected browser clients
connected_clients = set()

async def ws_handler(websocket, path):
    connected_clients.add(websocket)
    print(f'Browser connected. Total clients: {len(connected_clients)}')
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.discard(websocket)
        print(f'Browser disconnected. Total clients: {len(connected_clients)}')

async def broadcast_loop():
    """Reads from the queue and broadcasts to all connected WebSocket clients."""
    global connected_clients
    while True:
        data = await message_queue.get()
        if connected_clients:
            disconnected = set()
            for client in list(connected_clients):
                try:
                    await client.send(json.dumps(data))
                except Exception:
                    disconnected.add(client)
            connected_clients.difference_update(disconnected)

def kafka_consumer_thread(loop):
    """Runs in a background thread. Puts Kafka messages onto the asyncio queue."""
    consumer = KafkaConsumer(
        'sensor-readings',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='aria-dashboard'
    )
    print('Kafka consumer started, waiting for messages...')
    for message in consumer:
        asyncio.run_coroutine_threadsafe(
            message_queue.put(message.value),
            loop
        )

async def main():
    loop = asyncio.get_event_loop()

    # Start Kafka consumer in a background thread
    t = threading.Thread(target=kafka_consumer_thread, args=(loop,), daemon=True)
    t.start()

    print('Starting WebSocket bridge on ws://localhost:8765')
    async with websockets.serve(ws_handler, 'localhost', 8765):
        await broadcast_loop()  # runs forever

asyncio.run(main())
