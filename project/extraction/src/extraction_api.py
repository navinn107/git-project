from flask import Flask, jsonify, request
from datetime import datetime
import jwt
import json
import os
import boto3
import csv
import sqlite3

class RestAPI:

    def __init__(self):

        self.app = Flask(__name__)
        self.setup_routes()


    def fetch_client(self, client_id ):

        conn = sqlite3.connect('../../db/users.db')
        cursor = conn.cursor()

        cursor.execute("Select * from users where clientID=?", (client_id,))

        existing_user = cursor.fetchone()

        return True if existing_user else False
        
    def get_data(self):

        ACCESS_KEY = 'AKIA6KFM5H3WOEHMPLG2'
        SECRET_KEY = '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'
        
        BUCKET_NAME = 'file-bucket107'
        OBJECT_KEY = 'public_access/bengaluru-yulu-zone-location-static-file.json'
        
        session = boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        s3 = session.client('s3')
        response = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)

        json_str = response['Body'].read().decode('utf-8')
        json_array = json.loads(json_str)

        station_code = request.args.get("station_code")
        if station_code:

            filter_data = []
            for json_val in json_array:
                if json_val["station_code"] ==  int(station_code):
                    filter_data.append(json_val)

            return jsonify({"results": filter_data})
                
        else:
            return jsonify({"results":json_array})

    def authenticate(self, token):
        
        decoded_payload = jwt.decode(token, options={"verify_signature": False}, algorithms=['HS256'])
        client_id = decoded_payload["sub"]

        if self.fetch_client(client_id):
            
            current_time = datetime.now()
            token_expiration_time = datetime.fromtimestamp(decoded_payload['exp'])

            if current_time > token_expiration_time:

                return jsonify({'message': 'Token expired'}), 401
            else:
                return self.get_data()
            
        else:
            return jsonify({'message': 'No authorization'}), 401

    def setup_routes(self):

        @self.app.route('/api/ndx/get-data', methods=['POST'])
        def get_data():
            authorization_token = request.headers.get("Authorization")
            if authorization_token:
                return self.authenticate(authorization_token)
            else:
                return jsonify({'message': '[Invalid] URL'}), 404

        @self.app.route('/', defaults={'path': ''})
        @self.app.route('/<path:path>')
        def catch_all(path):
            return jsonify({'message': 'Invalid URL'}), 404
            
    def run(self, port=8000):
            self.app.run(host='192.168.1.6', port=port, debug=True)

if __name__ == '__main__':
    RestAPI().run()

