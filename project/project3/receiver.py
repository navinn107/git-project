import pika
import json
import logging as log
import redshift_connector

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RabbitmqServer:

    def __init__(self, host_name, queue_name, redshift_host, redshift_dbname, redshift_user, redshift_password, redshift_port):
        
        self.host = host_name
        self.queue = queue_name
        self.redshift_host = redshift_host
        self.redshift_dbname = redshift_dbname
        self.redshift_user = redshift_user
        self.redshift_password = redshift_password
        self.redshift_port = redshift_port

        self.connection = None
        self.channel = None
        self.cursor = None

        self.connect()
        self.purge_queue()
        self.connect_db()

    def connect(self):
        """Establishes connection to RabbitMQ and sets up channel, exchange, and queue."""
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue)
            log.info(".......RABBITMQ CONNECTED.......")
        
        except pika.exceptions.AMQPConnectionError as e:
            log.error(f".......FAILED TO CONNECT TO RABBITMQ: {e}.......")
        
        except Exception as e:
            log.error(f".......UNEXPECTED ERROR DURING RABBITMQ CONNECTION: {e}.......")

    def purge_queue(self):
        
        """Purges the messages in the queue."""
        
        try:
            self.channel.queue_purge(queue=self.queue)
            log.info(".......QUEUE PURGED.......")
        
        except Exception as e:
            log.error(f".......FAILED TO PURGE QUEUE: {e}.......")

    def connect_db(self):
        """Establishes connection to the Redshift database."""
        
        try:
            self.conn_params = {
                'host': self.redshift_host,
                'database': self.redshift_dbname,
                'user': self.redshift_user,
                'password': self.redshift_password,
                'port': self.redshift_port
            }
            self.conn = redshift_connector.connect(**self.conn_params)
            self.cursor = self.conn.cursor()
            log.info(".......REDSHIFT DATABASE CONNECTED.......")
        
        except redshift_connector.errors.RedshiftConnectorError as e:
            log.error(f".......FAILED TO CONNECT TO REDSHIFT: {e}.......")
        
        except Exception as e:
            log.error(f".......UNEXPECTED ERROR DURING REDSHIFT CONNECTION: {e}.......")

    def start_consume(self):
        """Starts consuming messages from RabbitMQ."""
        if self.connection.is_closed or self.channel.is_closed:
            log.error(".......RABBITMQ CONNECTION IS NOT AVAILABLE.......")
            self.connect()

        log.info(".......AWAITING RPC REQUESTS.......")

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.process_request)
        self.channel.start_consuming()

    def publish(self, message, reply_to, corr_id):
        """Publishes a message to RabbitMQ."""
        
        self.channel.basic_publish(exchange='', routing_key=reply_to,
                                   properties=pika.BasicProperties(
                                       correlation_id=corr_id
                                   ),
                                   body=json.dumps(message))
        log.info(f".......PUBLISHED MESSAGE: {message}.......")

    def process_request(self, ch, method, properties, body):
        """Processes incoming request messages."""
        
        try:
        
            log.info(".......RECEIVED REQUEST.......")
            request_json = json.loads(body)
            msisdn_value = request_json.get("msisdn")

            if msisdn_value:
                response = self.fetch_data(msisdn_value)
                self.publish(response, properties.reply_to, properties.correlation_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                response = {"statusCode": 400, "message": "MSISDN value is missing"}
                self.publish(response, properties.reply_to, properties.correlation_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
        
            log.error(f".......ERROR PROCESSING REQUEST: {e}.......")
            response = {"statusCode": 500, "message": "INTERNAL SERVER ERROR"}
            self.publish(response, properties.reply_to, properties.correlation_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def fetch_data(self, msisdn_value):
        """Fetches data from Redshift based on MSISDN value."""
        
        try:
        
            query = f'''
                SELECT 
                    SUBSCRIBER_PERSONAL_ID, 
                    SUBSCRIBER_FIRST_NAME, 
                    SUBSCRIBER_LAST_NAME, 
                    SUBSCRIBER_DOB,
                    DATE_OF_REGISTRATION,
                    MSISDN
                FROM 
                    public.customers 
                WHERE 
                    MSISDN = '{msisdn_value}';
            '''

            self.cursor.execute(query)
            results = self.cursor.fetchall()
            dictionary = self.fetch_response(results)

        except Exception as e:
            log.error(f".......ERROR FETCHING DATA: {e}.......")
            dictionary = {"statusCode": 500, "message": "Error fetching data"}

        return dictionary

    def fetch_response(self, results):
        """Formats fetched data into response format."""
        dictionary = {}
        if results:
            data = {
                "SUBSCRIBER_PERSONAL_ID": results[0][0],
                "SUBSCRIBER_FIRST_NAME": results[0][1],
                "SUBSCRIBER_LAST_NAME": results[0][2],
                "SUBSCRIBER_DOB": results[0][3].isoformat(),
                "DATE_OF_REGISTRATION": results[0][4].isoformat(),
                "MSISDN": results[0][5]
            }
            dictionary['statusCode'] = 200
            dictionary["results"] = data
        else:
            dictionary['statusCode'] = 204
            dictionary["message"] = "No content"

        return dictionary


if __name__ == '__main__':
    # RabbitMQ and Redshift connection parameters
    host_name = 'localhost'
    queue_name = 'ndx'
    redshift_host = 'redshift-cluster-1.cxp6lw5ihkz8.ap-south-1.redshift.amazonaws.com'
    redshift_dbname = 'dev'
    redshift_user = 'awsuser'
    redshift_password = 'E|Y[Wn!ylv?B$F?6'
    redshift_port = 5439

    # Create and start the RabbitmqServer instance
    server = RabbitmqServer(host_name, queue_name, redshift_host, redshift_dbname, redshift_user, redshift_password, redshift_port)
    server.start_consume()
