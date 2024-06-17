from flask import Flask, jsonify
import uuid
import json
class RestAPI:

    def __init__(self):
        self.app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):

        @self.app.route('/api/env/get-locations', methods=['GET'])
        def get_locations():
            # Sample data: List of 10 environmental sensor locations with UUIDs
            with open("../env_sensor_location.json", "r+") as f:
                locations = json.loads(f.read())

            return jsonify(locations), 200

    def run(self, port=5000):
        self.app.run(host='127.0.0.1', port=port, debug=True)

if __name__ == '__main__':
    my_app = RestAPI()
    my_app.run()
