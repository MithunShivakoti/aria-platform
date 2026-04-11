import pandas as pd
import json

docs = pd.read_csv(r"C:\\Users\\mithu\\OneDrive\\Desktop\\data\\documents.csv")

target_docs = [
    'SOP-MAINT-001',  # pump lubrication
    'MAN-MECH-001',   # bearing specs
    'SOP-OPS-010',    # compressor restart
    'SOP-MAINT-010',  # heat exchanger clean
    'MAN-INST-001',   # pressure transmitter
    'SOP-SAFE-001',   # ESD response
    'SOP-OPS-001',    # V-101 startup
]

rag = {}
for doc_id in target_docs:
    row = docs[docs['doc_id'] == doc_id]
    if len(row) > 0:
        rag[doc_id] = {
            'title':   row['title'].values[0],
            'content': row['content'].values[0]
        }
        print(f"✅ {doc_id}: {len(row['content'].values[0])} chars")
    else:
        print(f"❌ {doc_id}: NOT FOUND")

with open(r"C:\\Users\\mithu\\OneDrive\\Desktop\\data\\rag_documents.json", "w") as f:
    json.dump(rag, f)

print("\n✅ rag_documents.json saved")