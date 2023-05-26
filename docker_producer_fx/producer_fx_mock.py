import json
import random
import time
from kafka import KafkaProducer


# Define Kafka producer
producer = KafkaProducer(bootstrap_servers=['broker:29092'])

# Generate and send FX conversion rates every 3 seconds
while True:
    # Generate random FX conversion rate between USD and INR
    fx_rate = random.uniform(75, 100)
    
    # Create JSON object with FX rate and timestamp
    fx_rate_json = {'timez': int(time.time()*1000), 'fx_rate': fx_rate}
    
    # Serialize JSON object to string
    fx_rate_str = json.dumps(fx_rate_json)

    print(fx_rate_str)
    
    # Send JSON object to Kafka
    producer.send('stream2fx', fx_rate_str.encode('utf-8'))
    
    # Wait for 3 seconds
    time.sleep(random.uniform(1,3))