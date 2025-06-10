from pprint import pprint
import requests
from requests.structures import CaseInsensitiveDict
import json
import sys
def who_am_i(credentials):
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    basespace_url = "https://api.basespace.illumina.com/v1pre3/users/current"
    my_id = None
    try:
        platform_response = requests.get(basespace_url, headers=headers)
        platform_response_json = platform_response.json()
        my_id = platform_response.json()['Response']['Id']
    except:
        platform_response = requests.get(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find id for user for the following URL: {basespace_url}")
    return my_id


# STEP 1: list projects for current BaseSpace user
def list_basespace_projects(credentials):
    page_size = 200
    offset = 0
    page_number = 0
    total_items_returned = 0
    basespace_url = f"https://api.basespace.illumina.com/v1pre3/users/current/projects?Limit={page_size}&Offset={offset}"
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    my_projects = []
    try:
        platform_response = requests.get(basespace_url, headers=headers)
        platform_response_json = platform_response.json()
        #print(f"{list(platform_response.json()['Response'].keys())}")
        if 'Response' not in list(platform_response_json.keys()):
            pprint(json.dumps(platform_response_json),indent=4)
        else:
            total_items = platform_response_json['Response']['TotalCount']
            items_returned = platform_response_json['Response']['DisplayedCount']
            if total_items <= page_size:
                for project_idx,project in enumerate(platform_response.json()['Response']['Items']):
                    my_projects.append(project['Id'])
            else:
                total_items_returned += items_returned
                while total_items_returned < total_items:
                    offset += page_size
                    page_number += 1
                    basespace_url = f"https://api.basespace.illumina.com/v1pre3/users/current/projects?Limit={page_size}&Offset={offset}"
                    platform_response = requests.get(basespace_url, headers=headers)
                    platform_response_json = platform_response.json()
                    items_returned = platform_response.json()['Response']['DisplayedCount']
                    for project_idx,project in enumerate(platform_response.json()['Response']['Items']):
                        my_projects.append(project['Id'])
    except:
        platform_response = requests.get(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find projects for the following URL: {basespace_url}")
    return my_projects  

# STEP 2: get datasets for each user
def get_datasets(project_id,credentials,owning_id=None):
    page_size = 200
    offset = 0
    page_number = 0
    total_items_returned = 0
    basespace_url = f"https://api.basespace.illumina.com/v2/projects/{project_id}/datasets?Limit={page_size}&Offset={offset}"
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    my_datasets = []
    ### check for 'DateCreated'
    try:
        platform_response = requests.get(basespace_url, headers=headers)
        platform_response_json = platform_response.json()
        if 'Items' not in list(platform_response_json.keys()):
            print(list(platform_response_json.keys()))
            #pprint(json.dumps(platform_response_json),indent=4)
        else:
            total_items = platform_response_json['Paging']['TotalCount']
            items_returned = platform_response_json['Paging']['DisplayedCount']
            if total_items <= page_size:
                for dataset_idx,dataset in enumerate(platform_response.json()['Items']):
                    if owning_id is None:
                        my_datasets.append(dataset['Id'])
                    else:
                        if dataset['Project']['UserOwnedBy']['Id'] == owning_id:
                            my_datasets.append(dataset['Id'])
            else:
                total_items_returned += items_returned
                while total_items_returned < total_items:
                    offset += page_size
                    page_number += 1
                    basespace_url = f"https://api.basespace.illumina.com/v2/projects/{project_id}/datasets?Limit={page_size}&Offset={offset}"
                    platform_response = requests.get(basespace_url, headers=headers)
                    platform_response_json = platform_response.json()
                    items_returned = platform_response_json['Paging']['DisplayedCount']
                    for dataset_idx,dataset in enumerate(platform_response.json()['Items']):
                        if owning_id is None:
                            my_datasets.append(dataset['Id'])
                        else:
                            if dataset['Project']['UserOwnedBy']['Id'] == owning_id:
                                my_datasets.append(dataset['Id'])

    except:
        platform_response = requests.get(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find datasets for the following URL: {basespace_url}")
    return my_datasets  
#curl -X 'GET' -H 'Authorization: Bearer abf7d1f438144408b3532541268daa7c' -H 'Content-Type: application/json' -H 'User-Agent: BaseSpaceGOSDK/0.13.10 BaseSpaceCLI/1.6.2' 'https://api.basespace.illumina.com/v1pre3/users/current/runs?Limit=200&Offset=0'
# STEP 2: get datasets for each user
def list_runs(credentials,owning_id=None):
    page_size = 200
    offset = 0
    page_number = 0
    total_items_returned = 0
    basespace_url = f"https://api.basespace.illumina.com/v1pre3/users/current/runs?Limit={page_size}&Offset={offset}"
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    my_runs = []

    ### check for 'DateCreated'
    try:
        platform_response = requests.get(basespace_url, headers=headers)
        platform_response_json = platform_response.json()
        if 'Response' not in list(platform_response_json.keys()):
            pprint(json.dumps(platform_response_json),indent=4)
        else:
            total_items = platform_response_json['Response']['TotalCount']
            items_returned = platform_response_json['Response']['DisplayedCount']
            if total_items <= page_size:
                for run_idx,run in enumerate(platform_response.json()['Response']['Items']):
                    my_runs.append(run['Id'])
            else:
                total_items_returned += items_returned
                while total_items_returned < total_items:
                    offset += page_size
                    page_number += 1
                    basespace_url = f"https://api.basespace.illumina.com/v1pre3/users/current/runs?Limit={page_size}&Offset={offset}"
                    platform_response = requests.get(basespace_url, headers=headers)
                    platform_response_json = platform_response.json()
                    items_returned = platform_response_json['Response']['DisplayedCount']
                    for run_idx,run in enumerate(platform_response.json()['Response']['Items']):
                        my_runs.append(run['Id'])

    except:
        platform_response = requests.get(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find runs for the following URL: {basespace_url}")
    return my_runs  
#curl -X 'POST' -d '{"DatasetIds":["ds.b06fb5b8b42746c59b54ae7bf45e55b6"]}' -H 'Authorization: Bearer myTOKEN' -H 'Content-Type: application/json' -H 'User-Agent: BaseSpaceGOSDK/0.13.10 BaseSpaceCLI/1.6.2' 'https://api.basespace.illumina.com/v2/archive'
#curl -X 'POST' -d '{"DatasetIds":["ds.b06fb5b8b42746c59b54ae7bf45e55b6"]}' -H 'Authorization: Bearer myTOKEN' -H 'Content-Type: application/json' -H 'User-Agent: BaseSpaceGOSDK/0.13.10 BaseSpaceCLI/1.6.2' 'https://api.basespace.illumina.com/v2/restore'

bearer_token = "mY_TOKEN"
my_id = who_am_i(bearer_token)
print(f"{my_id}")
my_basespace_projects = list_basespace_projects(bearer_token)
print(f"{my_basespace_projects}")

my_datasets = get_datasets(my_basespace_projects[0],bearer_token)
print(f"{my_datasets}")

my_basespace_runs = list_runs(bearer_token)
print(f"{my_basespace_runs}")
