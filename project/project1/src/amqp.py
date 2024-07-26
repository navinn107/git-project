import pika
import logging as log
import ssl
import configparser

config = configparser.ConfigParser()
config.read('rabbitmq_config.ini')

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RabbitMQ:
    def __init__(self, rabbitmq_user, rabbitmq_password, rabbitmq_broker_id, region, rabbitmq_port, cipher_text, exchange_name, exchange_type_name, routing_key_name, queue_name, timeout):
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

    def connect(self):
        """Establishes connection to RabbitMQ and sets up channel, exchange, and queue."""
        self.response_json = None
        self.correlation_id = None

        # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers(self.cipher_text)

        url = f"amqps://{self.rabbitmq_user}:{self.rabbitmq_password}@{self.rabbitmq_broker_id}.mq.{self.region}.amazonaws.com:{self.rabbitmq_port}"
        parameters = pika.URLParameters(url)
        parameters.ssl_options = pika.SSLOptions(context=ssl_context)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
        self.channel.queue_declare(queue=self.queue)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)

        log.info(".......SERVER INITIALIZATION DONE.......")

    def publish_message(self, message):
        """Publishes a message to the RabbitMQ queue."""
        self.channel.basic_publish(exchange=self.exchange, routing_key=self.routing_key, body=message)
        log.info(f" [x] Sent '{message}'")

timeout = int(config['default']['timeout'])
rabbitmq_user = config['rabbitmq']['user']
rabbitmq_password = config['rabbitmq']['password']
rabbitmq_broker_id = config['rabbitmq']['broker_id']
rabbitmq_port = int(config['rabbitmq']['port'])
rabbitmq_region = config['rabbitmq']['region']
cipher_text = config['rabbitmq']['cipher_text']
exchange_name = config['exchange']['name']
exchange_type_name = config['exchange']['type']
routing_key_name = config['routing']['key']
queue_name = config['queue']['name']

message = 'Hello World!'
rabbitmq.publish_message(message)
print(" [x] Sent 'Hello World!'")
