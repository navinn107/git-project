import boto3

# Initialize a session using Amazon STS
sts_client = boto3.client('sts')

# Assume the role specified by the ARN
role_arn = "arn:aws:iam::983910137580:role/RedshiftAccessRole"
role_session_name = "my-session"

assumed_role_object = sts_client.assume_role(
    RoleArn=role_arn,
    RoleSessionName=role_session_name
)

# Extract the temporary credentials
credentials = assumed_role_object['Credentials']

# Use the temporary credentials to create a Redshift client
redshift_client = boto3.client(
    'redshift',
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)

# Now you can use the redshift_client to interact with your Redshift instance
# For example, describe clusters
response = redshift_client.describe_clusters()
print(response)
