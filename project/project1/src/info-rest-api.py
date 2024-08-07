from flask import Flask, jsonify
import json
import random
from datetime import datetime

class RestAPI:

    def __init__(self):
        self.app = Flask(__name__)
        self.setup_routes()
        self.locations = self.load_locations()

    def load_locations(self):
        with open("../misc/env_sensor_location.json", "r") as file:
            return json.load(file)

    def generate_sensor_data(self, location_id):
        return {
            "id": location_id,
            "observationDateTime": datetime.now().isoformat(),
            "airTemperature": {
                "instValue": random.uniform(25.0, 35.0)
            },
            "o3": {
                "instValue": random.uniform(10.0, 100.0)
            },
            "co2": {
                "instValue": random.uniform(300.0, 500.0)
            },
            "pm10": {
                "instValue": random.uniform(50.0, 150.0)
            },
            "ambientNoise": {
                "instValue": random.uniform(40.0, 90.0)
            }
        }

    def setup_routes(self):
        @self.app.route('/api/env/get-sensor-info', methods=['GET'])
        def get_sensor_info():
            sensor_data_list = [
                self.generate_sensor_data(location["id"])
                for location in self.locations
            ]
            return jsonify(sensor_data_list), 200
        
        @self.app.route('/api/env/get-locations', methods=['GET'])
        def get_locations():
            return jsonify(self.locations), 200
        
        @self.app.errorhandler(405)
        def method_not_allowed(e):
            return jsonify({'error': 'Method Not Allowed', 'message': 'The method is not allowed for the requested URL.'}), 405

        @self.app.errorhandler(404)
        def page_not_found(e):
            return jsonify({'error': 'Invalid URL', 'message': 'The requested URL is not found on the server. It is to fetch the env data'}), 404

    def run(self, port=5000):
        self.app.run(host='127.0.0.1', port=port, debug=True)

my_app = RestAPI()
app = my_app.app
