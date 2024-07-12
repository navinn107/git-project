import requests

response = requests.get("http://127.0.0.1:5000/send_request?msisdn=248410141")
print(response.text )
print(response.status_code)



