import pymongo
from confluent_kafka import Consumer
import configparser
from io import StringIO
import json

# Config by code
conf_local={
    'bootstrap.servers':'localhost:9094, localhost:9194, localhost:9294',
    'group.id':'group1',
    'auto.offset.reset':'earliest',
    'sasl.username':'admin',
    'sasl.password':'Unigap@2024',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
}

# Config by file
def consumer_config():
    conf_dict={}
    config=configparser.ConfigParser()
    config.read('config.ini')
    conf_dict['bootstrap.servers']=config['consumer']['bootstrap.servers']
    conf_dict['group.id'] = config['consumer']['group.id']
    conf_dict['auto.offset.reset'] = config['consumer']['auto.offset.reset']
    conf_dict['sasl.username'] = config['consumer']['sasl.username']
    conf_dict['sasl.password'] = config['consumer']['sasl.password']
    conf_dict['security.protocol'] = config['consumer']['security.protocol']
    conf_dict['sasl.mechanism'] = config['consumer']['sasl.mechanism']
    return conf_dict
def mongo_config():
    conf_dict = {}
    config = configparser.ConfigParser()
    config.read('config.ini')
    conf_dict['domain'] = config['mongodb']['domain']
    conf_dict['port'] = config['mongodb']['port']
    return f"{conf_dict['domain']}:{conf_dict['port']}"

if __name__=="__main__":
    conf=consumer_config()

    #Config theo mongodb container
    client=pymongo.MongoClient(mongo_config())
    db=client['kafka-project']
    col=db['product-view']

    consumer=Consumer(conf)
    consumer.subscribe(['product-view'])
    running=True
    while running:
        try:
            msg=consumer.poll(1.0)
            if msg is None: pass
            else:
                if msg.error() is not None:
                    print(msg.error())
                    '''loging'''
                doc=json.load(StringIO(msg.value().decode("utf-8")))
                col.insert_one(doc)
        except Exception as e:
            print(e)
            '''loging'''

        # finally:
        #     consumer.close()
        #     running = False