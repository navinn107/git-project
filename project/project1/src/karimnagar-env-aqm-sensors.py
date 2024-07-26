import requests
import json
import pika
import logging as log
import ssl
import configparser
import dateutil.parser as dp
import os
config = configparser.ConfigParser()
config.read('rabbitmq_config.ini')

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

time_formatter = "%d-%m-%YT%H:%M:%S+05:30"
time_formatter_new = "%Y-%m-%dT%H:%M:%S+05:30"

class KARIMNAGAR_AQM_INFO:
    def __init__(self, rabbitmq_user, rabbitmq_password, rabbitmq_broker_id, region, rabbitmq_port, cipher_text, exchange_name, exchange_type_name, routing_key_name, queue_name, timeout, base_url):
        self.rabbitmq_user = rabbitmq_user
        self.rabbitmq_password = rabbitmq_password
        self.rabbitmq_broker_id = rabbitmq_broker_id
        self.region = region
        self.rabbitmq_port = rabbitmq_port
        self.cipher_text = cipher_text

        self.exchange = exchange_name
        self.exchange_type = exchange_type_name
        self.routing_key = routing_key_name
        self.queue = queue_name
        self.timeout = timeout  

        self.connection = None
        self.channel = None

        self.base_url = base_url

    def connect(self):
        """Establishes connection to RabbitMQ and sets up channel, exchange, and queue."""
        self.response_json = None
        self.correlation_id = None

        # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
        # ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        # ssl_context.set_ciphers(self.cipher_text)

        # url = f"amqps://{self.rabbitmq_user}:{self.rabbitmq_password}@{self.rabbitmq_broker_id}.mq.{self.region}.amazonaws.com:{self.rabbitmq_port}"
        # parameters = pika.URLParameters(url)
        # parameters.ssl_options = pika.SSLOptions(context=ssl_context)

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))

        # self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
        self.channel.queue_declare(queue=self.queue)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)

        log.info(".......SERVER INITIALIZATION DONE.......")

    def get_data(self):
        """Extracts AQM locations from the source API."""
        url = self.base_url

        try:
            response = requests.get(url, verify=False)

            if response.status_code == 200:
                json_array = response.json()
                self.transform_publish(json_array)
            else:
                log.error(f"Error: {response.status_code}")

        except requests.exceptions.HTTPError as errh:
            log.error("An HTTP Error occurred:", errh)
        except requests.exceptions.ConnectionError as errc:
            log.error("An Error Connecting to the API occurred:", errc)
        except requests.exceptions.Timeout as errt:
            log.error("A Timeout Error occurred:", errt)
        except requests.exceptions.RequestException as err:
            log.error("An Unknown Error occurred", err)
        except Exception as oe:
            log.error('Other errors:', oe)

    def transform_publish(self, list_of_packets):
        """Transforms the data as per IUDX vocab and publishes them."""
        transformed_data = []

        for packet in list_of_packets:
            transformed_packet = self.transform_packet(packet)
            transformed_data.append(transformed_packet)
            self.publish_data(transformed_packet)

    def transform_packet(self, packet):
        """Transforms a single packet to the desired format."""
        vv = packet["observationDateTime"]
        observationDateTime =  dp.parse(vv).strftime(time_formatter_new)

        transformed = {
            "id": packet["id"],
            "airTemperature": packet["airTemperature"]["instValue"],
            "ambientNoise": packet["ambientNoise"]["instValue"],
            "co2": packet["co2"]["instValue"],
            "o3": packet["o3"]["instValue"],
            "pm10": packet["pm10"]["instValue"],
            "observationDateTime": observationDateTime

        }
        return transformed
    

    def deduplication(self, current_list_of_packets):

        """
        Removes duplicates from current_list of packets for each cycle &
        Stores list of packets seen in each cycle as json dump.
        :param current_list_of_packets: contains packets obtained at each cycle
        :type current_list_of_packets: List
        """
        if not(os.path.exists('karimnagar.json')):
            with open('karimnagar.json', 'w') as fp:
                self.publish_data(current_list_of_packets)
                cache_packet = {packet["id"] : packet for packet in current_list_of_packets}
                json.dump(cache_packet, fp, indent=6)
        else:
            with open('karimnagar.json', 'r+') as fp:
                json_str = fp.read()
                if json_str!="":
                    cache_list = json.loads(json_str)
                    fp.seek(0)
                    fp.truncate(0)
                    diff_cache_packet = {}
                    diff_packet = []
                    
                    for packet in current_list_of_packets:
    
                        if str(packet["id"]) not in cache_list.keys():
                            diff_cache_packet[packet["id"]] = packet
                            diff_packet.append(packet)

                        elif (str(packet["id"]) in cache_list.keys() and packet is not cache_list[str(packet["id"])]):
                            if packet["observationDateTime"]:
                                if (dp.parse(packet["observationDateTime"]) > dp.parse(cache_list[str(packet["id"])]["observationDateTime"])):
                                    cache_list[str(packet["id"])] = packet
                                    diff_packet.append(packet)
                                
                            else:
                                cache_list[str(packet["id"])] = packet
                                
                            
                    json.dump({**cache_list, **diff_cache_packet}, fp, indent=7)
                    self.publish_data(diff_packet)
                    
                else:
                    with open('karimnagar.json', 'w') as fp:
                        self.publish_data(current_list_of_packets)
                        cache_packet = {packet["id"] : packet for packet in current_list_of_packets}
                        json.dump(cache_packet, fp, indent=6)

    


    def publish_data(self, data):
        """Publishes the transformed data to RabbitMQ."""
        message = json.dumps(data, indent=4)
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=message,
            properties=pika.BasicProperties(content_type="application/json", delivery_mode=1),
            mandatory=True
        )
        log.info(" [x] Sent data to RabbitMQ")

# Example usage
if __name__ == "__main__":
    rabbitmq_config = config['rabbitmq']
    base_url = "http://3.110.255.79/api/env/get-sensor-info"

    karimnagar_aqm = KARIMNAGAR_AQM_INFO(
        rabbitmq_config['rabbitmq_user'],
        rabbitmq_config['rabbitmq_password'],
        rabbitmq_config['rabbitmq_broker_id'],
        rabbitmq_config['region'],
        rabbitmq_config['rabbitmq_port'],
        rabbitmq_config['cipher_text'],
        rabbitmq_config['exchange_name'],
        rabbitmq_config['exchange_type_name'],
        rabbitmq_config['routing_key_name'],
        rabbitmq_config['queue_name'],
        int(rabbitmq_config['timeout']),
        base_url
    )

    karimnagar_aqm.connect()
    karimnagar_aqm.get_data()
