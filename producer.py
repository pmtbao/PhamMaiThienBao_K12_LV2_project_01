from confluent_kafka import Producer
from confluent_kafka import Consumer
import  configparser

# Config by code
conf_pro_local={
    'bootstrap.servers':'localhost:9094, localhost:9194, localhost:9294',
    'sasl.username':'admin',
    'sasl.password':'Unigap@2024',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
}
conf_pro_docker = {
    'bootstrap.servers' : 'kafka-0:29092, kafka-1:29092, kafka-2:29092',}
conf_con = {
    'bootstrap.servers': '113.160.15.232:9094, 113.160.15.232:9194, 113.160.15.232:9294',
    'group.id':'group1',
    'auto.offset.reset':'earliest',
    'sasl.username' : 'kafka',
    'sasl.password' : 'UnigapKafka@2024',
    'security.protocol' : 'SASL_PLAINTEXT',
    'sasl.mechanism' : 'PLAIN',
}

#Config by file config.ini
def producer_config():
    conf_dict={}
    config=configparser.ConfigParser()
    config.read('config.ini')
    conf_dict['bootstrap.servers']=config['producer']['bootstrap.servers']
    conf_dict['sasl.username'] = config['producer']['sasl.username']
    conf_dict['sasl.password'] = config['producer']['sasl.password']
    conf_dict['security.protocol'] = config['producer']['security.protocol']
    conf_dict['sasl.mechanism'] = config['producer']['sasl.mechanism']
    return conf_dict

def acked(err,msg):
    if err is not None:
        print(f'Failed to delivered message {str(msg)}: {str(err)}')
    else:
        print(f"Message produced: {str(msg)}")

if __name__ == "__main__":
    conf_pro = producer_config()
    producer=Producer(conf_pro)
    consumer=Consumer(conf_con)
    consumer.subscribe(['product_view'])
    running=True
    while running:
       try:
           msg = consumer.poll(1.0)
           if msg is None: print('Waiting...')
           else:
               producer.produce('product-view',value=msg.value(), callback=acked)
               producer.poll(0)
       except KeyboardInterrupt:
           running=False
       # finally:
       #     producer.flush()
       #     consumer.close()

