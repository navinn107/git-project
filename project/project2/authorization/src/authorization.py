from flask import Flask, jsonify, request
from datetime import datetime, timedelta
import jwt
import json
import os
import sqlite3

class TokenGenerator:

    def __init__(self):

        self.app = Flask(__name__)
        self.SECRET_KEY = '86ddb5f6-ff02-461d-9ac2-c6ad8e95812b'
        self.setup_routes()

    def fetch_clients(self, client_id, client_secretkey ):

        conn = sqlite3.connect('../../db/users.db')
        cursor = conn.cursor()

        cursor.execute("Select * from users where clientID=?", (client_id,))

        existing_user = cursor.fetchone()

        if existing_user:
            
            if client_secretkey==existing_user[2]:
                token = self.generate_token(client_id)
                return jsonify({'token': token}), 200
            
            else:
                return jsonify({'message': "ClientId or  ClientSecretKey is incorrect"}), 200

        else:
            return jsonify({'message': "User is not registered"}), 200

    def setup_routes(self):
        
        @self.app.route('/api/ndx/get-token', methods=['POST'])
        def get_token():
            
            data = request.json
            client_id = data.get('clientID')
            client_secret = data.get('clientSecretKey')

            return self.fetch_clients(client_id, client_secret)

        @self.app.route('/', defaults={'path': ''})
        @self.app.route('/<path:path>')
        def catch_all(path):
            
            return jsonify({'message': 'Invalid URL'}), 404

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

if __name__ == "__main__":
    app = TokenGenerator().app
    app.run(host="0.0.0.0",port=5000, debug=True)    


        


        



    
