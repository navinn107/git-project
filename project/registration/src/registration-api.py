from flask import Flask, request, jsonify
import sqlite3
import hashlib

class Registration:

    def __init__(self):

        self.app = Flask(__name__)
        self.SECRET_KEY = '86ddb5f6-ff02-461d-9ac2-c6ad8e95812b'

        self.route()
    
    def register_user(self, username, password):
        
        conn = sqlite3.connect("../../db/users.db")
        cursor = conn.cursor()
        
        hashed_username = hashlib.sha256(username.encode()).hexdigest()

        cursor.execute("Select * from users where clientID=?", (hashed_username,))
        existing_user = cursor.fetchone()
        
        if existing_user:
            return jsonify({'message': "username already exists"}), 400
        
        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        cursor.execute("INSERT INTO users (clientID, clientSecretKey) VALUES (?, ?)", (hashed_username, hashed_password))
        conn.commit()
        
        return jsonify({
            "message": "Registration successfull",  
            "results":
            [
                {
                    "clientID": hashed_username, 
                    "clientSecretKey": hashed_password
                }
            ]})
    
    def route(self):

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
        

        @self.app.route('/', defaults={'path': ''}, methods=['POST', 'GET'])
        @self.app.route('/<path:path>', methods=['POST', 'GET'])
        def catch_all(path):
            
            return jsonify({'message': 'Invalid URL'}), 404
          
    def run(self, port=5000):
        self.app.run(host='127.0.0.1', port=port, debug=True)


if __name__ == "__main__":
    Registration().run()