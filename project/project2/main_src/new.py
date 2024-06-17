from flask import Flask, jsonify, request
from datetime import datetime, timedelta
import jwt
import json
import sqlite3
import hashlib
import boto3

class RestAPI:

    def __init__(self):
        self.app = Flask(__name__)
        self.SECRET_KEY = '86ddb5f6-ff02-461d-9ac2-c6ad8e95812b'
        self.setup_routes()
    
    def register_user(self, username, password):
        conn = sqlite3.connect("../../db/users.db")
        cursor = conn.cursor()
        
        hashed_username = hashlib.sha256(username.encode()).hexdigest()
        cursor.execute("SELECT * FROM users WHERE clientID=?", (hashed_username,))
        existing_user = cursor.fetchone()
        
        if existing_user:
            conn.close()
            return jsonify({'message': "Username already exists"}), 400
        
        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        cursor.execute("INSERT INTO users (clientID, clientSecretKey) VALUES (?, ?)", (hashed_username, hashed_password))
        conn.commit()
        conn.close()
        
        return jsonify({
            "message": "Registration successful",  
            "results": [
                {
                    "clientID": hashed_username, 
                    "clientSecretKey": hashed_password
                }
            ]
        })
    
    def fetch_client(self, client_id, client_secretkey=None):
        conn = sqlite3.connect('../../db/users.db')
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM users WHERE clientID=?", (client_id,))
        existing_user = cursor.fetchone()
        conn.close()

        if not existing_user:
            return None

        if client_secretkey:
            hashed_client_secretkey = hashlib.sha256(client_secretkey.encode()).hexdigest()
            if hashed_client_secretkey == existing_user[1]:
                return existing_user
            else:
                return None
        else:
            return existing_user

    def generate_token(self, client_id):
        current_time = datetime.now()
        expiration_time = current_time + timedelta(minutes=180)

        payload = {
            'sub': client_id,
            'iss': 'data.ndx.org.in',
            'aud': 'rs.data.ndx.org.in',
            'exp': expiration_time.timestamp(),
            'iat': current_time.timestamp(),
            'jti': f"rs:{client_id}",
            'role': 'developer',
            'cons': {},
            'user': '9ca1732c-232e-4686-b21e-a5a355c3e843',
            'drl': 'provider'
        }
        token = jwt.encode(payload, self.SECRET_KEY, algorithm='HS256')
        return token

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
            filter_data = [json_val for json_val in json_array if json_val["station_code"] == int(station_code)]
            return jsonify({"results": filter_data})
        else:
            return jsonify({"results": json_array})

    def authenticate(self, token):
        try:
            decoded_payload = jwt.decode(token, self.SECRET_KEY, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401

        client_id = decoded_payload["sub"]
        if self.fetch_client(client_id):
            return self.get_data()
        else:
            return jsonify({'message': 'No authorization'}), 401

    def setup_routes(self):

        @self.app.route("/api/register", methods=['POST'])
        def register():
            if request.headers.get('Content-Type') != 'application/json':
                return jsonify({'error': 'Unsupported Media Type', 'message': 'Request must be in JSON format'}), 415
            try:
                data = request.json
                username = data.get('username')
                password = data.get('password')
                if not username or not password:
                    return jsonify({"message": "Both username and password are required."}), 400
                return self.register_user(username, password)
            except Exception as e:
                return jsonify({'error': 'Bad Request', 'message': 'Request body must be in JSON format'}), 400

        @self.app.route('/api/ndx/get-token', methods=['POST'])
        def get_token():
            if request.headers.get('Content-Type') != 'application/json':
                return jsonify({'error': 'Unsupported Media Type', 'message': 'Request must be in JSON format'}), 415
            try:
                data = request.json
                client_id = data.get('clientID')
                client_secret = data.get('clientSecretKey')
                user = self.fetch_client(client_id, client_secret)
                if user:
                    token = self.generate_token(client_id)
                    return jsonify({'token': token}), 200
                else:
                    return jsonify({'message': "ClientId or ClientSecretKey is incorrect or user not registered"}), 200
            except Exception as e:
                return jsonify({'error': 'Bad Request', 'message': 'Request body must be in JSON format'}), 400

        @self.app.route('/api/ndx/get-data', methods=['POST'])
        def get_data():
            authorization_token = request.headers.get("Authorization")
            if authorization_token:
                return self.authenticate(authorization_token)
            else:
                return jsonify({'message': 'Authorization token is missing'}), 401

        @self.app.route('/', defaults={'path': ''})
        @self.app.route('/<path:path>')
        def catch_all(path):
            return jsonify({'message': 'Invalid URL'}), 404

    def run(self, port=5000):
        self.app.run(host='127.0.0.1', port=port, debug=True)

if __name__ == "__main__":
    RestAPI().run()
