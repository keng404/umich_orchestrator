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
    my_response = None
    try:
        platform_response = requests.get(basespace_url, headers=headers)
        platform_response_json = platform_response.json()
        my_response = platform_response.json()['Response']
    except:
        platform_response = requests.get(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find id for user for the following URL: {basespace_url}")
    return my_response

def get_scopes(credentials):
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    basespace_url = "https://api.basespace.illumina.com/v1pre3/oauthv2/token/current"
    my_response = None
    try:
        platform_response = requests.get(basespace_url, headers=headers)
        platform_response_json = platform_response.json()
        my_response = platform_response.json()['Response']
    except:
        platform_response = requests.get(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find id for user for the following URL: {basespace_url}")
    return my_response


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
    my_projects = dict()
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
                    my_projects[project['Name']] = project
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
                        my_projects[project['Name']] = project
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
    my_datasets = dict()
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
                        my_datasets[dataset['Id']] = dataset
                    else:
                        if dataset['Project']['UserOwnedBy']['Id'] == owning_id:
                            my_datasets[dataset['Id']] = dataset
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
                            my_datasets[dataset['Id']] = dataset
                        else:
                            if dataset['Project']['UserOwnedBy']['Id'] == owning_id:
                                my_datasets[dataset['Id']] = dataset

    except:
        platform_response = requests.get(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find datasets for the following URL: {basespace_url}")
    return my_datasets  

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
    my_runs = dict()

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
                    my_runs[run['Id']] = run
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
                        my_runs[run['Id']] = run

    except:
        platform_response = requests.get(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find runs for the following URL: {basespace_url}")
    return my_runs  


def get_projects_from_basespace_json(input_json):
    with open(input_json) as f:
        d = json.load(f)

    basespace_projects = []
    for i,k in enumerate(d['Projects']):
        basespace_projects.append(d['Projects'][k]['Name'])
    return basespace_projects

def empty_trash(credentials):
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {credentials}"
    headers['Content-Type'] = "application/json"
    basespace_url = "https://api.basespace.illumina.com/v2/trash"
    my_response = None
    try:
        platform_response = requests.delete(basespace_url, headers=headers)
        platform_response_json = platform_response.json()
        my_response = platform_response.json()
    except:
        platform_response = requests.delete(basespace_url, headers=headers)
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find id for user for the following URL: {basespace_url}")
    return my_response