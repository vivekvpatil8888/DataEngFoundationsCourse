from confluent_kafka import Consumer, TopicPartition

consumer = Consumer(
    {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest',
    }
)

# indefinite loop to pull data from brokers
def consume(consumer, timeout):
    while True:
        message = consumer.poll(timeout)
        if message is None:
            continue
        if message.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        yield message
    consumer.close()

consumer.subscribe(['topic1'])
for msg in consume(consumer, 1.0):
    print(msg)