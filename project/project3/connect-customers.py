




import boto3
import json
import redshift_connector
import os



# def get_secret(secret_name, region_name):
#     # Create a Secrets Manager client
#     client = boto3.client('secretsmanager', region_name=region_name)

    
#     get_secret_value_response = client.get_secret_value(SecretId=secret_name)
 

#     # Decrypts secret using the associated KMS key
#     secret = get_secret_value_response['SecretString']
#     return json.loads(secret)

# # Replace these with your actual values
# secret_name = "redshift!redshift-cluster-1-awsuser"
# region_name = "ap-south-1"

# # # Fetch the secret
# secret = get_secret(secret_name, region_name)

# print(secret)
# Extract the necessary details
host = 'redshift-cluster-1.cxp6lw5ihkz8.ap-south-1.redshift.amazonaws.com'  
dbname = 'dev'
# user = secret['username']
# password = secret['password']
# port = 5439  # Default port for Redshift


try:
    conn = redshift_connector.connect(
        host="redshift-cluster-1.cxp6lw5ihkz8.ap-south-1.redshift.amazonaws.com",
        database="dev",
        user="awsuser",
        password="E|Y[Wn!ylv?B$F?6",
        port=5439
    )
    cursor = conn.cursor()

    cursor.execute("SELECT * from public.customers LIMIT 10;")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    cursor.close()
    conn.close()

except Exception as e:
    print(f"An error occurred: {e}")
