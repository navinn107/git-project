from flask import Flask, jsonify, request
import pika
import uuid
import logging as log
import time
import json

log.basicConfig(level=log.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class RestAPI:

    def __init__(self, host_name, exchange_name, exchange_type_name, routing_key_name, queue_name, timeout):
        
    
        self.host = host_name
        self.exchange = exchange_name
        self.exchange_type = exchange_type_name
        self.routing_key = routing_key_name
        self.queue = queue_name
        self.timeout = timeout  
                
        # self.connection = None
        # self.channel = None
        # self.reply_queue = None
        
        self.connect()

        self.app = Flask(__name__)
        self.setup_routes()    

    def connect(self):

        """ESTABLISHES CONNECTION TO RABBITMQ AND SETS UP CHANNEL, EXCHANGE, AND QUEUE."""
        
        self.response_json = None
        self.correlation_id = None
        
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
        self.channel.queue_declare(queue=self.queue)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)
        
        self.reply_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        
        log.info(".......SERVER INITIALIZATION DONE.......")
        
        self.channel.basic_consume(queue=self.reply_queue, on_message_callback=self.on_reply_message_received, auto_ack=True)

    def on_reply_message_received(self, ch, method, properties, body):
        
        if self.correlation_id == properties.correlation_id:
            self.response_json = json.loads(body)

    def publish(self, message):
        
        """PUBLISHES A MESSAGE TO THE RABBITMQ QUEUE AND WAITS FOR A RESPONSE."""
        
        self.response_json = None
        self.correlation_id = str(uuid.uuid4())

        # if self.connection.is_closed or not self.channel or self.channel.is_closed:
        #     log.info(".......CONNECTION OR CHANNEL IS CLOSED, RECONNECTING.......")
        #     self.connect()

        self.channel.basic_publish(exchange=self.exchange, routing_key=self.routing_key,
            properties=pika.BasicProperties(
                reply_to=self.reply_queue,
                correlation_id=self.correlation_id,
            ),
            body=message
        )

        try:
            self.connection.process_data_events(time_limit=self.timeout)
        except pika.exceptions.AMQPTimeoutError:
            return None
        
        return self.response_json
    
    def setup_routes(self):
        
        """SETS UP THE FLASK ROUTE FOR THE API."""
        
        @self.app.route('/send_request', methods=['GET'])
        def get_info():
            
            msisdn_val = request.args.get('msisdn')            
            if not msisdn_val:
                return jsonify({"statusCode": 400, "message": "MSISDN VALUE IS MISSING"}), 400
            
            message = json.dumps({'msisdn': msisdn_val})
            
            try:
                response = self.publish(message)
                if response is None:
                    return jsonify({"statusCode": 500, "message": "NO RESPONSE FROM SERVER"}), 500
                return jsonify(response)
            except Exception as e:
                return jsonify({"statusCode": 500, "detail": str(e)}), 500
            # finally:
            #     if not self.connection.is_closed:
            #         self.connection.close()

    def run(self, port=5000):
        
        """RUNS THE FLASK APPLICATION."""
        self.app.run(host='127.0.0.1', port=port, debug=True)

if __name__ == '__main__':
    
    # Define RabbitMQ connection parameters
    host_name = 'localhost'
    exchange_name = 'rpc_ndx_exchange'
    exchange_type_name = 'direct'
    routing_key_name = 'rpc_routing_key'
    queue_name = 'ndx'
    timeout = 10

    # Create and run the RestAPI instance
    fetch_ndx_data = RestAPI(host_name, exchange_name, exchange_type_name, routing_key_name, queue_name, timeout)
    fetch_ndx_data.run()
