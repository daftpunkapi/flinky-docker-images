from confluent_kafka import Producer
import websocket
import json

# Kafka configuration
kafka_bootstrap_servers = 'broker:29092'
kafka_topic = 'stream1ws'

# Create Kafka producer
producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

# Define callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def on_message(ws, message):
    data = json.loads(message)
    data = data['data']
    filtered_data = [{'price':d['p'], 'timez':d['t']} for d in data]
    
    # print(type(filtered_data))
    for record in filtered_data:
        # Produce message to Kafka topic
        record = json.dumps(record)
        # print(record)
        # print(type(record))
        producer.produce(kafka_topic, value=record, callback=delivery_report)


def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=ch5m4qpr01qjg0av1hegch5m4qpr01qjg0av1hf0",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()