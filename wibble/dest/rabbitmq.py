import time
import pika

import settings
from wibble import Operation

class RabbitMQ(object):
    
    def __init__(self):
        self.credentials = pika.PlainCredentials(settings.DEST['USER'], settings.DEST['PASSWORD'])
        self.exchange_name = settings.DEST['EXCHANGE_PREFIX'] + settings.name
        self.connection = None
        self.channel = None


    def send(self, table_name, operation, data):
        headers_dict = {'TABLE_NAME':table_name, 'OPERATION': operation}
        if data == None:
            print("No body for MQ send")
            return

        if settings.DEST['HOST'] != '':
            retry = 5
            done = False
            while not done:
                if self.channel == None:
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.DEST['HOST'],credentials=self.credentials))
                    self.channel = self.connection.channel()
                    self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='headers')

                try:
                    self.channel.basic_publish(  exchange=self.exchange_name,
                                        routing_key='cdc',
                                        body=data,
                                        properties = pika.BasicProperties(headers=headers_dict))
                    done = True
                except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed) as e:
                    time.sleep(1)
                    self.channel = None
                    retry -= 1
                    if retry == 0:
                        done = True
                        print( "ERROR! Sending payload to {}:{}".format(self.exchange_name, data) )
        else:
            print('{\n   "HEADERS":%s,\n    "BODY":%s\n}'%(headers_dict,data))