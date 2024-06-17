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
        with open("../env_sensor_location.json", "r") as file:
            return json.load(file)

    def generate_sensor_data(self, location_id):
        return {
            "id": location_id,
            "observationDateTime": datetime.now().isoformat(),
            "airTemperature": {
                "instValue": random.randrange(30, 40, 3)
            },
            "o3": {
                "instValue": random.randrange(30, 40, 3)
            },
            "co2": {
                "instValue": random.randrange(30, 40, 3)
            },
            "pm10": {
                "instValue": random.randrange(100, 130, 3)
            },
            "ambientNoise": {
                "instValue": random.randrange(50, 60, 3)
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

    def run(self, port=5000):
        self.app.run(host='127.0.0.1', port=port, debug=True)

if __name__ == '__main__':
    my_app = RestAPI()
    my_app.run()
