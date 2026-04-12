import pandas as pd
import json
import time
import argparse
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description='ARIA Kafka producer — replays timeseries.csv')
parser.add_argument('--start-hour', type=float, default=0,
    help='Simulation hour to start from (e.g. 790 starts just before P-101 failure). Default: 0')
parser.add_argument('--delay', type=float, default=0.5,
    help='Seconds between timestamp batches (default 0.5). Lower = faster replay.')
args = parser.parse_args()

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

# Apply --start-hour offset
BASE_TIME = pd.Timestamp('2025-10-04 07:00:00', tz='UTC')
if args.start_hour > 0:
    start_ts = BASE_TIME + pd.Timedelta(hours=args.start_hour)
    timestamps = [ts for ts in timestamps if ts >= start_ts]
    print(f'Starting from hour {args.start_hour} ({start_ts})')

print(f'Streaming {len(timestamps)} timestamps, {len(df)} total readings')
print(f'Replay delay: {args.delay}s per batch')
print('Producer started. Press Ctrl+C to stop.')

REPLAY_DELAY = args.delay

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
    # On loop restart, go back to start_hour if specified
    timestamps = sorted(groups.groups.keys())
    if args.start_hour > 0:
        start_ts = BASE_TIME + pd.Timedelta(hours=args.start_hour)
        timestamps = [ts for ts in timestamps if ts >= start_ts]
