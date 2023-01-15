from calendar import c
import json
import platform
from google.cloud import storage
from google.oauth2 import service_account
import pickle
bucketName = "***"


my_os = platform.system()
cred_json = json.loads(GoogleBaseHook(gcp_conn_id="***")._get_field('keyfile_dict'))
    
storage_client = storage.Client(
        project = cred_json["project_id"],
        credentials = service_account.Credentials.from_service_account_info(cred_json))

bucket = storage_client.get_bucket(bucketName)


def load_pickle_from_cloud_storage(cloud_file_name):
    blob = bucket.blob(cloud_file_name)
    pickle_in = blob.download_as_string()
    my_dictionary = pickle.loads(pickle_in)
    return my_dictionary

def download_file_from_cloud_storage(cloud_file_name, local_file_name):
    try:
        blob = bucket.blob(cloud_file_name)
        with open(local_file_name, 'wb') as f:
            storage_client.download_blob_to_file(blob, f)
        return True
    except Exception as e:
        print(e)
        return False

def upload_file_to_cloud_storage(cloud_file_name, local_file_name):
    try:
        blob = bucket.blob(cloud_file_name)
        blob.upload_from_filename(local_file_name)
        return True
    except Exception as e:
        print(e)
        return False


def write_df_as_cloud_csv(cloud_file_name, df_string):
    try:
        blob = bucket.blob(cloud_file_name)
        blob.upload_from_string(df_string, 'text/csv')
        return True
    except Exception as e:
        print(e)
        return False    


