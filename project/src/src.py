from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import sqlite3
import hashlib
import jwt
import json
import boto3


class RestAPI:

    def __init__(self):
        self.app = Flask(__name__)
        self.SECRET_KEY = '86ddb5f6-ff02-461d-9ac2-c6ad8e95812b'
        self.setup_routes()
    
    def fetch_clients(self, client_id, client_secretkey=None, password=None,  register=False):

        conn = sqlite3.connect('../db/users.db')
        cursor = conn.cursor()

        cursor.execute("Select * from users where clientID=?", (client_id,))

        existing_user = cursor.fetchone()
        
        if register:

            if existing_user:
                return jsonify({'message': "username already exists"}), 400

            client_secret = hashlib.sha256(password.encode()).hexdigest()
            cursor.execute("INSERT INTO users (clientID, clientSecretKey) VALUES (?, ?)", (client_id, client_secret))
            conn.commit()
        
            return jsonify({
                "message": "Registration successfull",  
                "results":
                [
                    {
                        "clientID": client_id, 
                        "clientSecretKey": client_secret
                    }
                ]})

        return existing_user
    
    def generate_token(self, client_id):
        
        current_time = datetime.now()
        expiration_time = current_time + timedelta(minutes=180)

        payload = {
            'sub': client_id,
            'iss': 'data.ndx.org.in',
            'aud': 'rs.data.ndx.org.in',
            'exp': expiration_time.timestamp(),  # Token expires in 30 minutes
            'iat': current_time.timestamp(),
            'jti': f"rs:{client_id}",
            'role': 'developer',
            'cons': {},
            'user': '9ca1732c-232e-4686-b21e-a5a355c3e843',
            'drl': 'provider'
        }
        token = jwt.encode(payload, self.SECRET_KEY, algorithm='HS256')
        return token

    def authenticate(self, token):
        
        decoded_payload = jwt.decode(token, options={"verify_signature": False}, algorithms=['HS256'])
        client_id = decoded_payload["sub"]

        if self.fetch_clients(client_id):
            
            current_time = datetime.now()
            token_expiration_time = datetime.fromtimestamp(decoded_payload['exp'])

            if current_time > token_expiration_time:

                return jsonify({'message': 'Token expired'}), 401
            else:
                return self.get_data()
            
        else:
            return jsonify({'message': 'No authorization'}), 401

    def register_user(self, username, password):
          
        client_id = hashlib.sha256(username.encode()).hexdigest()
        return self.fetch_clients(client_id, password=password, register=True)

    def get_token(self, client_id, client_secret):

        existing_user = self.fetch_clients(client_id)
        
        if existing_user:

            if client_secret==existing_user[2]:
                token = self.generate_token(client_id)
                return jsonify({'token': token}), 200
            
            else:
                return jsonify({'message': "ClientId or  ClientSecretKey is incorrect"}), 200

        else:
            return jsonify({'message': "User is not registered"}), 200

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


        if request.args:
            station_code = request.args.get("station_code")
            if station_code:

                filter_data = []
                for json_val in json_array:
                    if json_val["station_code"] ==  int(station_code):
                        filter_data.append(json_val)

                return jsonify({"results": filter_data})
                    
            else:
                return jsonify({'error': 'Bad Request', 'message': 'station_code is the only filter'}), 400
            
        else:
            return jsonify({"results": json_array})



    def setup_routes(self):

        @self.app.route("/api/register", methods=['POST'])
        def register():

            if request.headers.get('Content-Type') != 'application/json':
                return jsonify({'error': 'Unsupported Media Type', 'message': 'Request must be in JSON format'}), 415
            try:
                data = request.json
            except Exception as e:
                return jsonify({'error': 'Bad Request', 'message': 'Request body must be in JSON format'}), 400

            if data:
                
                data = request.get_json()
                
                username = data.get('username')
                password = data.get('password')

                if not username or not password:
                    return jsonify({"message": "Both  username and password are required."}), 400

            return self.register_user(username, password)

        @self.app.route('/api/ndx/get-token', methods=['POST'])
        def get_token():
  
            data = request.json
            client_id = data.get('clientID')
            client_secret = data.get('clientSecretKey')

            return self.get_token(client_id, client_secret)

        @self.app.route('/api/ndx/get-data', methods=['POST'])
        def get_data():
            authorization_token = request.headers.get("token")
            if authorization_token:
                return self.authenticate(authorization_token)
            else:
                return jsonify({'message': '[Invalid] URL'}), 404

        @self.app.errorhandler(405)
        def method_not_allowed(e):
            return jsonify({'error': 'Method Not Allowed', 'message': 'The method is not allowed for the requested URL.'}), 405

        @self.app.errorhandler(404)
        def page_not_found(e):
            return jsonify({'error': 'Invalid URL', 'message': 'The requested URL was not found on the server.'}), 404

    def run(self, port=5000):
        self.app.run(host='127.0.0.1', port=port, debug=True)


if __name__ == "__main__":
    app = TokenGenerator().app
    app.run(host="0.0.0.0",port=5000, debug=True)   



        



    
