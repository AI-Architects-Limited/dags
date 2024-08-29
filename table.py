import prestodb
import pandas as pd
import numpy as np
from faker import Faker

# Initialize Faker for generating random IP addresses
fake = Faker()

# Establish a connection to Presto
conn = prestodb.dbapi.connect(
    host='localhost',
    port=8080,
    user='krishnakumarisingh',  # Replace with your username
    catalog='memory',  # Using 'memory' as a temporary catalog; replace with your catalog if different
    schema='default'
)

# Create a cursor object to execute SQL commands
cur = conn.cursor()

# Step 3: Create the table in Presto
create_table_query = """
CREATE TABLE dim_user_events (
    user_id bigint,
    timestamp bigint,
    event_type varchar,
    device_type varchar,
    device_id bigint,
    ip_address varchar
)
"""

cur.execute(create_table_query)
print("Table 'dim_user_events' created successfully.")

# Step 4: Generate synthetic data
num_rows = 10000
user_ids = np.random.randint(1e8, 1e9, size=num_rows)
timestamps = np.random.randint(
    pd.Timestamp('now').timestamp() - 10 * 24 * 60 * 60,  # 10 days ago
    pd.Timestamp('now').timestamp(),  # Now
    size=num_rows
).astype(int)
event_types = np.random.choice(['click', 'view', 'share', 'like', 'comment'], size=num_rows)
device_types = np.random.choice(['web', 'mobile'], size=num_rows)
device_ids = np.random.randint(1e8, 1e9, size=num_rows)
ip_addresses = [fake.ipv4() for _ in range(num_rows)]

# Convert to a DataFrame
data = pd.DataFrame({
    'user_id': user_ids,
    'timestamp': timestamps,
    'event_type': event_types,
    'device_type': device_types,
    'device_id': device_ids,
    'ip_address': ip_addresses
})

# Step 5: Insert synthetic data into Presto
insert_query_template = """
INSERT INTO dim_user_events (user_id, timestamp, event_type, device_type, device_id, ip_address)
VALUES
"""

# Batch size for insertion
batch_size = 1000

for start in range(0, num_rows, batch_size):
    end = start + batch_size
    batch = data.iloc[start:end]

    # Generate value strings for batch insertion
    values = ", ".join(
        f"({row['user_id']}, {row['timestamp']}, '{row['event_type']}', '{row['device_type']}', {row['device_id']}, '{row['ip_address']}')"
        for _, row in batch.iterrows()
    )

    insert_query = insert_query_template + values

    # Execute the insert query
    cur.execute(insert_query)
    print(f"Inserted rows {start + 1} to {end} into 'dim_user_events'.")

# Close the cursor and connection
cur.close()
conn.close()
print("Data insertion complete.")
