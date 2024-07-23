import requests
from msal import ConfidentialClientApplication

client_id = '9b7eb88a-c89f-4f9c-9801-230fd859e855'
client_secret = 'SLS8Q~1esi9PzdHNg3sCu.r6~IAK6pqyEh1y4bye'
tenant_id = 'e16ca6c1-813e-4ec3-a7b2-4f6ad40991ab'

authority_url = f"https://login.microsoftonline.com/{tenant_id}"
scope = ["https://analysis.windows.net/powerbi/api/.default"]

app = ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
token_response = app.acquire_token_for_client(scopes=scope)
access_token = token_response['access_token']

url = "https://api.powerbi.com/v1.0/myorg/reports"

headers = {
    'Authorization': f'Bearer {access_token}'
}

response = requests.get(url, headers=headers)
print(response.json())
