import requests
from requests.structures import CaseInsensitiveDict
import pprint
from pprint import pprint
import json
import subprocess
import os
import argparse
import re
import sys
from datetime import datetime as dt
import time
from time import sleep
import random
import boto3
from botocore.exceptions import ClientError
##########################
import bssh_utils
from datetime import datetime as dt
from datetime import timedelta
#####################

##ICA_BASE_URL = "https://ica.illumina.com/ica"
def logging_statement(string_to_print):
    date_time_obj = dt.now()
    timestamp_str = date_time_obj.strftime("%Y/%b/%d %H:%M:%S:%f")
    #############
    final_str = f"[ {timestamp_str} ] {string_to_print}"
    return print(f"{final_str}")
    
### obtain temporary AWS credentials
def get_temporary_credentials(api_key,project_id,data_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/data/{data_id}:createTemporaryCredentials"
    full_url = api_base_url + endpoint
    ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    payload = {}
    payload['credentialsFormat'] = "RCLONE"
    ########
    response = requests.post(full_url, headers=headers, data=json.dumps(payload))
    if response.status_code != 200:
        pprint(json.dumps(response.json()),indent=4)
        raise ValueError(f"Could not get temporary credentials for {data_id}")
    return response.json()

def set_temp_credentials(credential_json):
    CREDS = credential_json
    os.environ['AWS_ACCESS_KEY_ID'] = CREDS['rcloneTempCredentials']['config']['access_key_id']
    os.environ['AWS_SESSION_TOKEN'] = CREDS['rcloneTempCredentials']['config']['session_token']
    os.environ['AWS_SECRET_ACCESS_KEY'] = CREDS['rcloneTempCredentials']['config']['secret_access_key']
    return print("Setting temp credentials for upload/download")


def create_aws_service_object(aws_service_name,credential_json):
   required_aws_obj = boto3.client(
       aws_service_name,
       aws_access_key_id=credential_json['rcloneTempCredentials']['config']['access_key_id'],
       aws_secret_access_key=credential_json['rcloneTempCredentials']['config']['secret_access_key'],
       aws_session_token=credential_json['rcloneTempCredentials']['config']['session_token'],
       region_name = credential_json['rcloneTempCredentials']['config']['region']
   )
   return required_aws_obj

def upload_file(filename,credential_json):
    try:
        s3 = create_aws_service_object('s3',credential_json)
        s3_uri_split = credential_json['rcloneTempCredentials']['filePathPrefix'].split('/')
        bucket = s3_uri_split[0]
        object_name = "/".join(s3_uri_split[1:(len(s3_uri_split))])
        response = s3.upload_file(filename, bucket, object_name)
    except ClientError as e:
        logging.error(e)
    return logging_statement(f"Uploaded {filename}")

def download_file(filename,credential_json):
    try:
        s3 = create_aws_service_object('s3',credential_json)
        s3_uri_split = credential_json['rcloneTempCredentials']['filePathPrefix'].split('/')
        bucket = s3_uri_split[0]
        object_name = "/".join(s3_uri_split[1:(len(s3_uri_split))])
        response = s3.download_file( bucket, object_name,filename)
    except ClientError as e:
        logging.error(e)
    return logging_statement(f"Downloaded {filename}")

def delete_data_ica_managed(filename,credential_json):
    try:
        s3 = create_aws_service_object('s3',credential_json)
        s3_uri_split = credential_json['rcloneTempCredentials']['filePathPrefix'].split('/')
        bucket = s3_uri_split[0]
        object_name = "/".join(s3_uri_split[1:(len(s3_uri_split))])
        response = s3.delete_object( bucket, object_name)
    except ClientError as e:
        logging.error(e)
    return logging_statement(f"Deleted {filename}")


def unarchive_data_ica_managed(api_key,data_id,project_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/data/{data_id}:unarchive"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        response = requests.post(full_url, headers=headers)
    except:
        logging_statement(f"ERROR: Cannot archive data {data_id} not found in project {project_id}")
        return None
    if response.status_code == 204:
        return True
    else:
        logging_statement(f"ERROR: archive data {data_id} not found in project {project_id}")
        return False


def archive_data_ica_managed(api_key,data_id,project_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/data/{data_id}:archive"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        response = requests.post(full_url, headers=headers)
    except:
        logging_statement(f"ERROR: Cannot archive data {data_id} not found in project {project_id}")
        return None
    if response.status_code == 204:
        return True
    else:
        logging_statement(f"ERROR: Cannot archive data {data_id} not found in project {project_id}")
        return False

def delete_data_ica_managed_v2(api_key,data_id,project_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/data/{data_id}:delete"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        response = requests.post(full_url, headers=headers)
    except:
        logging_statement(f"ERROR: Cannot delete data {data_id} not found in project {project_id}")
        return None
    if response.status_code == 204:
        return True
    else:
        logging_statement(f"ERROR: Cannot delete data {data_id} not found in project {project_id}")
        return False    

def unarchive_data_basespace_managed(credentials,**kwargs):
    basespace_url = f"https://api.basespace.illumina.com/v2/restore"
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    data = dict()
    ############
    format_string2  = "%Y-%m-%dT%H:%M:%S.%fZ"
    format_string1  = "%Y-%m-%dT%H:%M:%SZ"
    format_string3  = "%Y-%m-%dT%H:%M:%S"
    timestamp_obj = dt.strptime(kwargs['timestamp'],format_string1)
    ################
    for key, value in kwargs.items():
        if key == "dataset_ids":
            data['DatasetIds'] = value
        elif key == "run_ids":
            data['RunIds'] = value
        elif key == "project_ids":
            ## for each project, identify datasets associated to that project and check the creation date
            ### inorder to mark it for archive/unarchive
            projects = value 
            data['DatasetIds'] = []
            for project_id in projects:  
                dataset_dict = bssh_utils.get_datasets(project_id,credentials)
                if len(list(dataset_dict.keys())) > 0:
                    for dataset_id in list(list(dataset_dict.keys())):
                        dataset_creation_date = dataset_dict[dataset_id]['DateCreated']
                        dataset_creation_date = dataset_creation_date.split(".")[0]
                        dataset_creation_time = None
                        try:
                            dataset_creation_time  = dt.strptime(dataset_creation_date,format_string3)
                        except:
                            dataset_creation_time  = dt.strptime(dataset_creation_date,format_string1)
                        if dataset_creation_time is not None:
                            if data_creation_time < timestamp_obj:
                                data['DatasetIds'].append(dataset_id)
            data['DatasetIds'] = list(set(data['DatasetIds']))
    if len(list(data.keys())) > 0:
        try:
            platform_response = requests.post(basespace_url, headers=headers,data=json.dumps(data))
            platform_response_json = platform_response.json() 
        except:
            platform_response = requests.post(basespace_url, headers=headers)
            pprint(platform_response,indent=4)
            logging_statement(f"ERROR: Could not unarchive for the following items: {data}")  
            return False
    else:
        logging_statment(f"ERROR: Please provide items (dataset_ids,run_ids,project_ids) to unarchive")
        return None
    return True

def delete_data_basespace_managed(credentials,**kwargs):
    basespace_url = f"https://api.basespace.illumina.com/v2/archive"
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    data = dict()
    for key, value in kwargs.items():
        if key == "datasets":
            data['DatasetIds'] = value
            basespace_url = f"https://api.basespace.illumina.com/v2/{key}/{value}"
        elif key == "runs":
            data['RunIds'] = value
            basespace_url = f"https://api.basespace.illumina.com/v2/{key}/{value}"
        elif key == "projects":
            ## for each project, identify datasets associated to that project and check the creation date
            ### inorder to mark it for archive/unarchive
            projects = value 
            data['DatasetIds'] = []
            for project_id in projects:  
                dataset_dict = bssh_utils.get_datasets(project_id,credentials)
                if len(list(dataset_dict.keys())) > 0:
                    for dataset_id in list(list(dataset_dict.keys())):
                        dataset_creation_date = dataset_dict[dataset_id]['DateCreated']
                        dataset_creation_date = dataset_creation_date.split(".")[0]
                        dataset_creation_time = None
                        try:
                            dataset_creation_time  = dt.strptime(dataset_creation_date,format_string3)
                        except:
                            dataset_creation_time  = dt.strptime(dataset_creation_date,format_string1)
                        if dataset_creation_time is not None:
                            if data_creation_time < timestamp_obj:
                                data['DatasetIds'].append(dataset_id)
            data['DatasetIds'] = list(set(data['DatasetIds']))
            basespace_url = f"https://api.basespace.illumina.com/v2/{key}/datasets"
    if len(list(data.keys())) > 0:
        try:
            platform_response = requests.delete(basespace_url, headers=headers)
            platform_response_json = platform_response.json() 
        except:
            platform_response = requests.delete(basespace_url, headers=headers)
            pprint(platform_response,indent=4)
            logging_statement(f"ERROR: Could not delete the following items: {data}")  
            return False
    else:
        logging_statment(f"ERROR: Please provide items (datasets,runs,projects) to delete")
        return None            
    return True

def archive_data_basespace_managed(credentials,**kwargs):
    basespace_url = f"https://api.basespace.illumina.com/v2/archive"
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    data = dict()
    ############
    format_string2  = "%Y-%m-%dT%H:%M:%S.%fZ"
    format_string1  = "%Y-%m-%dT%H:%M:%SZ"
    format_string3  = "%Y-%m-%dT%H:%M:%S"
    timestamp_obj = dt.strptime(kwargs['timestamp'],format_string1)
    ################
    for key, value in kwargs.items():
        if key == "dataset_ids":
            data['DatasetIds'] = value
        elif key == "run_ids":
            data['RunIds'] = value
        elif key == "project_ids":
            ## for each project, identify datasets associated to that project and check the creation date
            ### inorder to mark it for archive/unarchive
            projects = value 
            data['DatasetIds'] = []
            for project_id in projects:  
                dataset_dict = bssh_utils.get_datasets(project_id,credentials)
                if len(list(dataset_dict.keys())) > 0:
                    for dataset_id in list(list(dataset_dict.keys())):
                        dataset_creation_date = dataset_dict[dataset_id]['DateCreated']
                        dataset_creation_date = dataset_creation_date.split(".")[0]
                        dataset_creation_time = None
                        try:
                            dataset_creation_time  = dt.strptime(dataset_creation_date,format_string3)
                        except:
                            dataset_creation_time  = dt.strptime(dataset_creation_date,format_string1)
                        if dataset_creation_time is not None:
                            if data_creation_time < timestamp_obj:
                                data['DatasetIds'].append(dataset_id)
            data['DatasetIds'] = list(set(data['DatasetIds']))
    if len(list(data.keys())) > 0:
        try:
            platform_response = requests.post(basespace_url, headers=headers,data=json.dumps(data))
            platform_response_json = platform_response.json() 
        except:
            platform_response = requests.post(basespace_url, headers=headers)
            pprint(platform_response,indent=4)
            logging_statement(f"ERROR: Could not archive for the following items: {data}")  
            return False
    else:
        logging_statment(f"ERROR: Please provide items (dataset_ids,run_ids,project_ids) to archive")
        return None
    return True

### for a given basespace project, return datasets to take action (i.e. archive/unarchive and delete) on
def find_basespace_datasets(credentials,**kwargs):
    datasets_collected_dict = dict()
    datasets_available = []
    datasets_archived = []
    format_string1  = "%Y-%m-%dT%H:%M:%SZ"
    format_string3  = "%Y-%m-%dT%H:%M:%S"
    if 'mode' in kwargs.keys():
        mode = kwargs['mode']
    else:
        mode = 'default'
    if 'project_id' in kwargs.keys():
        project_id = kwargs['project_id']
    else:
        project_id = None
    if 'timestamp' in kwargs.keys():

        timestamp_obj = dt.strptime(kwargs['timestamp'],format_string1)
    else:
        timestamp_obj = None
    if 'timestamp2' in kwargs.keys():
        timestamp2_obj = dt.strptime(kwargs['timestamp2'],format_string1)
    else:
        timestamp2_obj = None
    #######################    
    dataset_dict = bssh_utils.get_datasets(project_id,credentials)
    for dataset_id in list(dataset_dict.keys()):
        dataset_creation_date = dataset_dict[dataset_id]['DateCreated']
        dataset_creation_date = dataset_creation_date.split(".")[0]
        dataset_creation_time = None
        try:
            dataset_creation_time  = dt.strptime(dataset_creation_date,format_string3)
        except:
            dataset_creation_time  = dt.strptime(dataset_creation_date,format_string1)
        if mode == "delete":
            if dataset_dict[dataset_id]['IsArchived'] is False:
                if dataset_creation_time is not None:
                    if dataset_creation_time < timestamp_obj:
                        if timestamp2_obj is None:
                            datasets_available.append(dataset_id)
                        elif dataset_creation_time >= timestamp2_obj:
                            datasets_available.append(dataset_id)
                else:
                    logging_statement(f"WARNING: Cannot determine dataset creation date for {dataset_id}")
            elif dataset_dict[dataset_id]['IsArchived'] is True:
                if dataset_creation_time is not None:
                    if dataset_creation_time < timestamp_obj:
                        if timestamp2_obj is None:
                            datasets_archived.append(dataset_id)
                        elif dataset_creation_time >= timestamp2_obj:
                            datasets_archived.append(dataset_id)
                else:
                   logging_statement(f"WARNING: Cannot determine dataset creation date for {dataset_id}")
        elif mode == "archive":
            if dataset_dict[dataset_id]['IsArchived'] is False:
                if dataset_creation_time is not None:
                    if dataset_creation_time < timestamp_obj:
                        if timestamp2_obj is None:
                            datasets_available.append(dataset_id)
                        elif dataset_creation_time >= timestamp2_obj:
                            datasets_available.append(dataset_id)
                else:
                    logging_statement(f"WARNING: Cannot determine dataset creation date for {dataset_id}")
            elif dataset_dict[dataset_id]['IsArchived'] is True:
                if dataset_creation_time is not None:
                    if dataset_creation_time < timestamp_obj:
                        if timestamp2_obj is None:
                            datasets_archived.append(dataset_id)
                        elif dataset_creation_time >= timestamp2_obj:
                            datasets_archived.append(dataset_id)
                else:
                    logging_statement(f"WARNING: Cannot determine dataset creation date for {dataset_id}")
        else:
            if dataset_creation_time is not None:
                if dataset_creation_time < timestamp_obj:
                    if timestamp2_obj is None:
                        datasets_available.append(dataset_id)
                    elif dataset_creation_time >= timestamp2_obj:
                        datasets_available.append(dataset_id)
            else:
                logging_statement(f"WARNING: Cannot determine dataset creation date for {dataset_id}")
    #######################################
    #print(f"datasets_archived {datasets_archived}")
    #print(f"datasets_available {datasets_available}")
    datasets_collected_dict['datasets_archived'] = datasets_archived
    datasets_collected_dict['datasets_available'] = datasets_available
    return datasets_collected_dict

### for a given basespace account, return runs to take action (i.e. archive/unarchive and delete) on
def find_basespace_runs(credentials,**kwargs):
    basespace_runs_to_check = bssh_utils.list_runs(credentials)
    runs_collected_dict = dict()
    runs_available = []
    runs_archived = []
    format_string1  = "%Y-%m-%dT%H:%M:%SZ"
    format_string3  = "%Y-%m-%dT%H:%M:%S"
    if 'mode' in kwargs.keys():
        mode = kwargs['mode']
    else:
        mode = 'default'

    if 'timestamp' in kwargs.keys():
        timestamp_obj = dt.strptime(kwargs['timestamp'],format_string1)
    else:
        timestamp_obj = None
    if 'timestamp2' in kwargs.keys():
        timestamp2_obj = dt.strptime(kwargs['timestamp2'],format_string1)
    else:
        timestamp2_obj = None
    #########################
    runs_dict = bssh_utils.list_runs(credentials)
    for run_id in list(runs_dict.keys()):
        run_creation_date = runs_dict[run_id]['DateCreated']
        run_creation_date = run_creation_date.split(".")[0]
        run_creation_time = None
        try:
            run_creation_time  = dt.strptime(run_creation_date,format_string3)
        except:
            run_creation_time  = dt.strptime(run_creation_date,format_string1)
        if mode == "delete":
            if runs_dict[run_id]['IsArchived'] is False:
                if run_creation_time is not None:
                    if run_creation_time < timestamp_obj:
                        if timestamp2_obj is None:
                            runs_available.append(run_id)
                        elif run_creation_time >= timestamp2_obj:
                            runs_available.append(run_id)
                else:
                    logging_statement(f"WARNING: Cannot determine run creation date for {run_id}")
            elif runs_dict[run_id]['IsArchived'] is True:
                if run_creation_time is not None:
                    if run_creation_time < timestamp_obj:
                        if timestamp2_obj is None:
                            runs_archived.append(run_id)
                        elif run_creation_time >= timestamp2_obj:
                            runs_archived.append(run_id)
                else:
                    logging_statement(f"WARNING: Cannot determine run creation date for {run_id}")
        elif mode == "archive":
            if runs_dict[run_id]['IsArchived'] is False:
                if run_creation_time is not None:
                    if run_creation_time < timestamp_obj:
                        if timestamp2_obj is None:
                            runs_available.append(run_id)
                        elif run_creation_time >= timestamp2_obj:
                            runs_available.append(run_id)
                else:
                    logging_statement(f"WARNING: Cannot determine run creation date for {run_id}")
            elif runs_dict[run_id]['IsArchived'] is True:
                if run_creation_time is not None:
                    if run_creation_time < timestamp_obj:
                        if timestamp2_obj is None:
                            runs_archived.append(run_id)
                        elif run_creation_time >= timestamp2_obj:
                            runs_archived.append(run_id)
                else:
                    logging_statement(f"WARNING: Cannot determine run creation date for {run_id}")
        else:
            if run_creation_time is not None:
                if run_creation_time < timestamp_obj:
                    if timestamp2_obj is None:
                        runs_available.append(run_id)
                    elif run_creation_time >= timestamp2_obj:
                        runs_available.append(run_id)
            else:
                logging_statement(f"WARNING: Cannot determine run creation date for {run_id}")
    #######################
    runs_collected_dict['runs_archived'] = runs_archived
    runs_collected_dict['runs_available'] = runs_available
    return runs_collected_dict