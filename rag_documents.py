import pandas as pd, json

df = pd.read_csv('failure_events.csv')
result = {}

for _, row in df.iterrows():
    asset_id = row['asset_id']
    if asset_id not in result:
        result[asset_id] = []
    result[asset_id].append({
        'event_id':          row['failure_event_id'],
        'scenario_id':       row['scenario_id'],
        'wo':                row['scenario_id'],
        'date':              str(row['event_timestamp'])[:10],
        'severity':          row['severity'],
        'safety_impact':     row['safety_impact'],
        'detected_by':       row['detected_by'],
        'failure_mode':      row['failure_mode'],
        'root_cause':        row['root_cause'],
        'mechanism':         row['failure_mechanism'],
        'immediate_action':  row['immediate_action'],
        'corrective_action': row['corrective_action'],
        'production_loss':   int(row['production_loss_bbl']),
        'downtime':          float(row['downtime_hours'])
    })

json.dump(result, open('failure_events.json', 'w'), indent=2)
print('Done:', len(df), 'events across', len(result), 'assets')
for asset_id, events in result.items():
    print(f'  {asset_id}: {len(events)} event(s)')