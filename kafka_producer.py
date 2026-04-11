import pandas as pd
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv('timeseries.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp')

# Group by timestamp so all sensors at same timestamp go together
groups = df.groupby('timestamp')
timestamps = sorted(groups.groups.keys())

print(f'Streaming {len(timestamps)} timestamps, {len(df)} total readings')
print('Producer started. Press Ctrl+C to stop.')

# Replay speed — 0.5 seconds per timestamp batch = 2 batches per second
REPLAY_DELAY = 0.5

while True:  # loop forever for continuous demo
    for ts in timestamps:
        batch = groups.get_group(ts)
        readings = []
        for _, row in batch.iterrows():
            readings.append({
                'timestamp': str(row['timestamp']),
                'sensor_id': row['sensor_id'],
                'asset_id': row['asset_id'],
                'sensor_type': row['sensor_type'],
                'value': round(float(row['value']), 4),
                'unit': row['unit'],
                'quality_flag': row['quality_flag']
            })
        # Send entire timestamp batch as one Kafka message
        producer.send('sensor-readings', value={
            'timestamp': str(ts),
            'readings': readings
        })
        producer.flush()
        time.sleep(REPLAY_DELAY)
    print('Reached end of dataset, restarting loop...')
