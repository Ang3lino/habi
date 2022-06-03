from google.cloud import secretmanager
import json


def read_secret(project_id,name_secret):
    project_id = project_id  # GCP project in which to store secrets in Secret Manager.
    secret_id = f"projects/372328156728/secrets/{name_secret}/versions/latest"  # ID of the secret to create.
    client = secretmanager.SecretManagerServiceClient()  # Create the Secret Manager client.
    response = client.access_secret_version(request={"name": secret_id})  # Access the secret version.
    payload = response.payload.data.decode("UTF-8")
    return payload 
    
    # #Save in dictionary
    # cred = json.loads(payload)
    # return cred
