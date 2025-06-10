#!/usr/bin/env python
# every 5/10 minutes or so --- this will be controlled outside of this script?
# lookup project by id or name
# given a project, look up all the analyses associated with it
# subset the analyses to analyses by a particular pipeline
# decide on whether to write which ids to identify which to keep track of
#    - if file exists, check analysis submitted table to see which analyses have been submitted and remove id(s) from our list to keep track of if they have been submitted
# if analyses id SUCCEEDED, run downstream pipeline
#    - create sample manifest file
#    - grab output folder from completed analyses
#    - load in (API) template or craft our own
#    - identify which inputs are hard-coded or not
#    - log in table which analysis id, project id, time submitted for downstream pipeline
##### import helper modules
import ica_analysis_monitor
import ica_analysis_launch
import samplesheet_utils
import ica_data_transfer
import ica_analysis_outputs
######## import python modules
import argparse
import time
import os
import sys
import requests
from requests.structures import CaseInsensitiveDict
import pprint
from pprint import pprint
import json
from datetime import datetime as dt
from datetime import timedelta
import re
import csv
import boto3
from botocore.exceptions import ClientError
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='orchestrator.log', encoding='utf-8', level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())
### helper functions
def logging_statement(string_to_print):
    date_time_obj = dt.now()
    timestamp_str = date_time_obj.strftime("%Y/%b/%d %H:%M:%S:%f")
    #############
    final_str = f"[ {timestamp_str} ] {string_to_print}"
    return logger.info(f"{final_str}")

def get_timestamp_previous_days(days_ago):
    # Get today's date
    today = dt.today()

    # Calculate the date 60 days ago
    days_ago_obj = today - timedelta(days=days_ago)

    # Convert the date to a datetime object (midnight of that day)
    datetime_days_ago = dt.combine(days_ago_obj, dt.min.time())
    return(datetime_days_ago)

def metadata_with_tag(data):
    found_tags = False
    for idx,val in enumerate(data['tags']):
        if len(data['tags'][val]) > 0:
            found_tags = True
    return found_tags
# based off of file of interest, return based on nomenclature if file is MGI or not
def mgi_or_not(foi):
    is_mgi = False
    name_simplified = os.path.basename(foi)
    name_prefix = name_simplified.split('.')[0]
    name_prefix_split = name_prefix.split('_')
    if len(name_prefix_split) >= 3:
       if re.search("M$",name_prefix_split[len(name_prefix_split)-3]) is not None:
        is_mgi = True
    return is_mgi
### This function returns data found within first-level of analysis output
#### to avoid redundant copy jobs
def get_analysis_output_to_copy(analysis_output,analysis_metadata):
    ##pprint(analysis_output,indent=4)
    output_folder_path = None
    logging_statement(f"analysis_metadata['reference']: {analysis_metadata['reference']}")
    for output in analysis_output:
        path_normalized = output['path'].strip("/$")
        ###logging_statement(f"path_normalized: {path_normalized}")
        if os.path.basename(path_normalized) == analysis_metadata['reference']:
            output_folder_path = output['path']
    #### edge case --- output_folder_path is not listed in the analysis_output object
    if output_folder_path is None:
        output_folder_path = "/" + analysis_metadata['reference'] 

    #### copy data to new folder --- only want files + folders in parent directory
    data_to_copy = []
    if output_folder_path is not None:
        for output in analysis_output:
            path_normalized = output['path'].strip("/$")
            path_normalized = path_normalized.strip("^/+")
            #path_remainder = re.sub(output_folder_path,"",path_normalized)
            path_remainder = path_normalized.replace(analysis_metadata['reference'],"")
            path_remainder = path_remainder.strip("^/+")
            #logging_statement(f"path_normalized: {path_normalized}")
            #logging_statement(f"path_remainder: {path_remainder}")
            if path_remainder != "":
                path_remainder_split = path_remainder.split('/')
                if len(path_remainder_split) == 1:
                    #logging_statement(f"path_remainder: {path_remainder}")
                    #logging_statement(f"data_to_copy_path: {output['path']}")
                    data_to_copy.append(output['id'])
    return data_to_copy

def get_data(api_key,data_id,project_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/data/{data_id}"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        response = requests.get(full_url, headers=headers)
    except:
        logging_statement(f"data {data_id} not found in project {project_id}")
        return None
    data_metadata = response.json()
    #pprint(data_metadata,indent=4)
    if 'data' in data_metadata.keys():
        return data_metadata['data']['details']['status']
    else:
        return None
    return logging_statement(f"Data lookup for {data_id} and {project_id} completed")

def get_data_metadata(api_key,data_id,project_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/data/{data_id}"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        response = requests.get(full_url, headers=headers)
    except:
        logging_statement(f"data {data_id} not found in project {project_id}")
        return None
    data_metadata = response.json()
    #pprint(data_metadata,indent=4)
    if 'data' in data_metadata.keys():
        return data_metadata['data']
    else:
        return None
    return logging_statement(f"Data lookup for {data_id} and {project_id} completed")

def craft_data_batch(data_ids):
    batch_object = []
    for data_id in data_ids:
        datum_dict = {}
        datum_dict["dataId"] = data_id
        batch_object.append(datum_dict)
    return batch_object


def create_data(api_key,project_name, filename, data_type, folder_id=None, format_code=None,filepath=None,project_id=None):
    if project_id is None:
        project_id = get_project_id(api_key, project_name)
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/data"
    full_url = api_base_url + endpoint
    ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    #headers['User-Agent'] = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36"
    ########
    payload = {}
    payload['name'] = filename
    if filepath is not None:
        filepath_split = filepath.split('/')
        if len(filepath_split) > 1:
            payload['folderPath'] = filepath
    if folder_id is not None:
        payload['folderId'] = folder_id
    if data_type not in ["FILE", "FOLDER"]:
        raise ValueError("Please enter a correct data type to create. It can be FILE or FOLDER.Exiting\n")
    payload['dataType'] = data_type
    if format_code is not None:
        payload['formatCode'] = format_code
    request_params = {"method": "post", "url": full_url, "headers": headers, "data": json.dumps(payload)}
    response = requests.post(full_url, headers=headers, data=json.dumps(payload))
    data_metadata = response.json()
    if response.status_code not in [201,409]:
        pprint(json.dumps(response.json()),indent=4)
        raise ValueError(f"Could not create data {filename}")
    #### handle case where data (placeholder) exists
    if response.status_code in [409]:
        data_metadata = ica_analysis_launch.list_data(api_key,filename,project_id)
        if len(data_metadata) > 0:
            data_metadata = data_metadata[0]
        else:
            raise ValueError(f"Cannot find data {filename} in the project {project_id}")
    #if 'data' not in keys(response.json()):
    ##pprint(json.dumps(response.json()),indent = 4)
    #raise ValueError(f"Could not obtain data id for {filename}")
    ##pprint(data_metadata,indent=4)
    if 'data' not in data_metadata.keys():
        data_id = data_metadata['id']
    else:
        data_id = data_metadata['data']['id']
    return data_id

def copy_data(api_key,data_to_copy_batch,folder_id,destination_project):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{destination_project}/dataCopyBatch"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    collected_data = {}
    collected_data['items'] = data_to_copy_batch
    collected_data['destinationFolderId'] =  folder_id
    collected_data['copyUserTags'] = True
    collected_data['copyTechnicalTags'] = True
    collected_data['copyInstrumentInfo'] = True
    collected_data['actionOnExist'] = 'OVERWRITE'
    data_to_copy = data_to_copy_batch
    try:
        response = requests.post(full_url, headers = headers,data = json.dumps(collected_data))
    except:
        raise ValueError(f"Could not copy data: {data_to_copy} to {destination_project}")
    if response.status_code >= 400:
        pprint(json.dumps(response.json()), indent=4)
        raise ValueError(f"Could not copy data: {data_to_copy} to {destination_project}")
    copy_details = response.json()
    #pprint(copy_details, indent=4)
    return copy_details

def copy_batch_status(api_key,batch_id,destination_project):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{destination_project}/dataCopyBatch/{batch_id}"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    ##headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        response = requests.get(full_url, headers = headers)
    except:
        raise ValueError(f"Could not get copy batch status : {batch_id} to {destination_project}")
    batch_details = response.json()
    #pprint(batch_details, indent=4)
    return batch_details

def link_data(api_key,data_to_link_batch,destination_project):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{destination_project}/dataLinkingBatch"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v4+json'
    headers['Content-Type'] = 'application/vnd.illumina.v4+json'
    headers['X-API-Key'] = api_key
    collected_data = {}
    collected_data['items'] = data_to_link_batch
    #data = f'{"items":[{"dataId": "{data_to_link}"}]}'
    data_to_link = data_to_link_batch
    try:
        #response = requests.post(full_url, headers = headers,data = data)
        response = requests.post(full_url, headers = headers,data = json.dumps(collected_data))
        #response = requests.post(full_url, headers = headers,json = collected_data)
    except:
        raise ValueError(f"Could not link data: {data_to_link} to {destination_project}")
    if response.status_code >= 400:
        pprint(json.dumps(response.json()), indent=4)
        raise ValueError(f"Could not link data: {data_to_link} to {destination_project}")
    link_details = response.json()
    return link_details

def link_batch_status(api_key,batch_id,project_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/dataLinkingBatch/{batch_id}"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    ##headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        response = requests.get(full_url, headers = headers)
    except:
        raise ValueError(f"Could not get link batch status : {batch_id} to {project_id}")
    batch_details = response.json()
    ###pprint(batch_details, indent=4)
    return batch_details
######
def get_data_id(api_key,file_path,project_id):
    filename = os.path.basename(file_path)
    datum_metadata = ica_analysis_launch.list_data(api_key,filename,project_id)
    data_id = None
    if len(datum_metadata) > 0:
        for idx,data_metadata in enumerate(datum_metadata):
            if 'data' not in data_metadata.keys():
                if data_metadata['path'] == file_path:
                    data_id = data_metadata['id']
            else:
                if data_metadata['data']['path'] == file_path:
                    data_id = data_metadata['data']['id']
    else:
        raise ValueError(f"Cannot find data {filename} in the project {project_id}")
    if data_id is None:
        raise ValueError(f"Cannot find data {file_path} in the project {project_id}")
    return data_id
                    
###################################################
### Here SOURCE and DESTINATION project refer to a BSSH-managed project in ICA and downstream project
################
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_project_id',default=None, type=str, help="SOURCE ICA project id")
    parser.add_argument('--source_project_name',default=None, type=str, help="SOURCE ICA project name")
    parser.add_argument('--destination_project_id',default=None, type=str, help="DESTINATION ICA project id [OPTIONAL]")
    parser.add_argument('--destination_project_name',default=None, type=str, help="DESTINATION ICA project name")
    parser.add_argument('--analyses_monitored_file', default='analyses_monitored_file.txt', type=str, help="Table containing ICA analyses of interest")
    parser.add_argument('--analyses_managed_table', default='analyses_managed_table.txt', type=str, help="Table recording ICA analyses that have been managed")
    parser.add_argument('--api_key_file', default=None, type=str, help="file that contains API-Key")
    parser.add_argument('--server_url', default='https://ica.illumina.com', type=str, help="ICA base URL")
    parser.add_argument('--days_to_archive',default=60, type=int, help="Number of days from current date to archive data from source project")
    parser.add_argument('--days_to_delete',default=90, type=int, help="Number of days from current date to delete data from source project")
    parser.add_argument('--dry_run', action="store_true",default=False, help="run script in dry run mode --- no analyses will be launched, but full script will be run")
    args, extras = parser.parse_known_args()
    #############
    source_project_id = args.source_project_id
    source_project_name = args.source_project_name
    destination_project_id = args.destination_project_id
    destination_project_name = args.destination_project_name
    analyses_monitored_file = args.analyses_monitored_file
    analyses_managed_table = args.analyses_managed_table
    os.environ['ICA_BASE_URL'] = args.server_url
    ###### read in api key file
    my_api_key = None
    logging_statement("Grabbing API Key")
    if args.api_key_file is not None:
        if os.path.isfile(args.api_key_file) is True:
            with open(args.api_key_file, 'r') as f:
                my_api_key = str(f.read().strip("\n"))
    if my_api_key is None:
        raise ValueError("Need API key")

    #### get the project identifiers we need
    if source_project_id is None and source_project_name is not None:
        logging_statement("Grabbing ICA SOURCE PROJECT ID")
        source_project_id = ica_analysis_launch.get_project_id(my_api_key,source_project_name)

    if destination_project_id is None and destination_project_name is None:
        logging_statement("Grabbing ICA DESTINATION PROJECT ID")
        destination_project_name = source_project_name
        destination_project_id = source_project_id
    elif destination_project_id is None and destination_project_name is not None:
        logging_statement("Grabbing ICA DESTINATION PROJECT ID")
        destination_project_id = ica_analysis_launch.get_project_id(my_api_key,destination_project_name)
    if source_project_id is None:
        raise ValueError("Need to provide project name or project id")
    if source_project_id == destination_project_id:
        raise ValueError("Need to provide different project name/id for destination and source project")

    ####### now let's set up pipeline analysis by updating the template
    
    ### STEP1: grab analyses of interest to monitor and analysis_ids_of_interest to manage
    logging_statement("STEP1: grab analyses of interest to monitor and analysis_ids_of_interest to manage")
    analysis_ids_of_interest = []
    analysis_ids_to_manage = []

    logging_statement(f"Grabbing analyses for {source_project_id}")
    analyses_list = ica_analysis_monitor.list_project_analyses(my_api_key,source_project_id)

    ### only keeping track of analyses in these status, any analyses aborted or failed will not be monitored
    ### focus only on BCLConvert analyses
    logging_statement(f"Subsetting analyses_to_monitor and analyses_to_manage for {source_project_id}")
    desired_analyses_status_to_monitor = ["REQUESTED","INTIALIZED","INPROGRESS",'QUEUED', 'INITIALIZING', 'PREPARING_INPUTS', 'GENERATING_OUTPUTS']
    desired_analyses_status_to_manage = ["SUCCEEDED"]
    for aidx,project_analysis in enumerate(analyses_list):
        if re.match('BCLConvert',project_analysis['pipeline']['code'],re.IGNORECASE) is not None and project_analysis['status'] in desired_analyses_status_to_monitor:
            analysis_ids_of_interest.append(project_analysis['id'])
        elif re.match('BCLConvert',project_analysis['pipeline']['code'],re.IGNORECASE) is not None and project_analysis['status'] in desired_analyses_status_to_manage:
            analysis_ids_to_manage.append(project_analysis['id'])

    ###  STEP2: finialize analyses ids to monitor
    logging_statement(f"STEP2: finialize analyses ids to monitor")
    ### look at analyses ids previously monitored and written to a text file
    analysis_ids_previously_considered = []
    if os.path.exists(analyses_monitored_file) is True:
        logging_statement(f"Reading in previous analyses monitored from {analyses_monitored_file}")
        with open(analyses_monitored_file, 'r') as f:
            analysis_ids_previously_considered = str(f.read().split("\n"))

    ### if there are any analyses ids previously monitored and check  that they are not now ready to manage
    for analysis_id in analysis_ids_previously_considered:
        if analysis_id not in analysis_ids_to_trigger and analysis_id not in analysis_ids_of_interest:
            analysis_ids_of_interest.append(analysis_id)
    
    if len(analysis_ids_of_interest) > 0:
        logging_statement(f"Writing in analyses monitored from {analyses_monitored_file}")
        with open(analyses_monitored_file, 'w') as f:
            for analysis_id in analysis_ids_of_interest:
                f.write(analysis_id + "\n")    
    else:
        logging_statement(f"No analyses to monitor")

    ###  STEP3: Focus on analyses ids to manage (link + record , archive,  delete)
    if len(analysis_ids_to_manage) > 0:
        logging_statement(f"STEP3: Focus on analyses ids to manage")
        analysis_ids_previously_managed = {}
        analyses_managed_table_data = []
        if os.path.exists(analyses_managed_table) is True:
            logging_statement(f"Reading in previous analyses monitored from {analyses_managed_table}")
            #### format is analysis_id_managed,run_id
            with open(analyses_managed_table) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                    analyses_managed_table_data.append(row)
        if len(analyses_managed_table_data) > 0:
            for line in analyses_managed_table_data:
                if line[0] != "analysis_id_managed":
                    analysis_ids_previously_managed[line[0]] = line[1]

         ### if analysis id is already associated with a 'downstream' analysis (after looking it up in the analyses_launched_table), don't run trigger the downsream analysis
        logging_statement(f"Finalize list of analyses to manage")
        analysis_ids_to_manage_final = []
        if len(list(analysis_ids_previously_managed.keys())) > 0:
            for id in analysis_ids_to_manage:
                if id not in list(analysis_ids_previously_managed.keys()):
                    analysis_ids_to_manage_final.append(id)
        else:
            analysis_ids_to_manage_final = analysis_ids_to_manage

        metadata_to_write = {}
        if len(analysis_ids_to_manage_final) > 0:
            analysis_ids_to_manage_str = ", ".join(analysis_ids_to_manage_final)
            logging_statement(f"analyses_to_manage: {analysis_ids_to_manage_str}")
            for id in analysis_ids_to_manage_final: 
                ### otherwise do manage the downstream analysis, and write pipeline launch metadata to analyses_launched_table
                ### first identify data id of output folder
                logging_statement(f"first identify data id of output folder for the analysis id: {id}")
                analysis_metadata = ica_analysis_launch.get_project_analysis(my_api_key,source_project_id,id)
                #### adding in re-implementation of analysis output
                analysis_output = ica_analysis_outputs.get_full_analysis_output(my_api_key,source_project_id,id)
                #print(f"analysis_output: {analysis_output}")
                #### folder path
                ##data_to_copy = get_analysis_output_to_copy(analysis_output,analysis_metadata)
                #logging_statement(f"Data to copy: {data_to_copy}")
                ########################
                data_to_link = {}
                output_folder_id = None
                output_folder_path = None
                run_id = None
                #### we only want FASTQ files, no undetermined FASTQ files
                for output in analysis_output:
                    path_normalized = output['path'].strip("/$")
                    path_normalized = path_normalized.strip("^/+") 
                    if os.path.basename(path_normalized) == analysis_metadata['reference']:
                        output_folder_id = output['id']
                        output_folder_path = output['path']
                        run_id = analysis_metadata['reference']
                    if re.search(".fastq.gz$",os.path.basename(path_normalized)) is not None and re.match("Undetermined",os.path.basename(path_normalized)) is None:
                        fastq_of_interest = mgi_or_not(output['path'])
                        if fastq_of_interest is True:
                            data_information = {}
                            data_information['path'] =  output['path']
                            data_information['name'] = output['name']
                            data_to_link[output['id']] = data_information

                ##### raise error condition if we can't find FASTQs of interest        
                if len(list(data_to_link.keys())) < 1:
                    logging_statement(f"Could not find FASTQ files for {analysis_metadata['reference']}")
                ### link the output data
                if args.dry_run is False and len(list(data_to_link.keys())) > 0:
                    data_link_batch = craft_data_batch(list(data_to_link.keys()))
                    print(f"data_link_batch {data_link_batch}")
                    link_batch = link_data(my_api_key,data_link_batch,destination_project_id)
                    link_batch_id = link_batch['id']
                    batch_status = link_batch_status(my_api_key,link_batch_id,destination_project_id)
                    while batch_status['job']['status'] != "SUCCEEDED":
                        logging_statement(f"Checking on linking batch job {link_batch_id}")
                        batch_status = link_batch_status(my_api_key,link_batch_id,destination_project_id)
                        time.sleep(5)
                    logging_statement(f"Linking completed for {run_id}")
                else:
                    logging_statement(f"Linking data from {source_project_id} to {destination_project_id}")
                    logging_statement(f"{data_to_link}")

                ### Create FASTQ manifest file for MGI and upload to ICA
                ### output file will be based on run_id
                if args.dry_run is False:
                    fastq_manifest_file = f"{run_id}.download_manifest.csv"
                    logging_statement(f"Creating FASTQ download manifest {fastq_manifest_file}")
                    with open(fastq_manifest_file, 'w') as f:
                        line_arr = ["file_name","file_path","data_id","project_id"]
                        new_str = ",".join(line_arr)
                        f.write(new_str + "\n")
                        ### name, path, data_id, project_id
                        for idl,dtl in enumerate(data_to_link):
                            new_line = [data_to_link[dtl]['name'],data_to_link[dtl]['path'],dtl,destination_project_id]
                            new_line_str = ",".join(new_line)
                            f.write(new_line_str + "\n")
                    ######## upload manifest file to ICA
                    logging_statement(f"Upload FASTQ download manifest {fastq_manifest_file} to ICA")
                    manifest_file_id = create_data(my_api_key,destination_project_name, os.path.basename(fastq_manifest_file), "FILE",filepath="/fastq_manifests/",project_id=destination_project_id)
                    ### perform actual upload
                    creds = ica_data_transfer.get_temporary_credentials(my_api_key,destination_project_id, manifest_file_id)
                    ica_data_transfer.set_temp_credentials(creds)
                    ica_data_transfer.upload_file(fastq_manifest_file,creds)

                if args.dry_run is False:
                    if os.path.exists(analyses_managed_table) is True:
                        logging_statement(f"Adding analysis managed from {analyses_managed_table}")
                        #### format is analysis_id_managed,run_id
                        with open(analyses_managed_table, 'a+') as f:
                            line_arr = [id,run_id]
                            new_str = ",".join(line_arr)
                            f.write(new_str + "\n")
                    else:
                        logging_statement(f"Creating {analyses_managed_table}")
                        logging_statement(f"Adding analysis managed from {analyses_managed_table}")
                        #### format is analysis_id_managed,run_id
                        with open(analyses_managed_table, 'w') as f:
                            line_arr = ["analysis_id_managed","run_id"]
                            new_str = ",".join(line_arr)
                            f.write(new_str + "\n")
                            
                            line_arr = [id,run_id]
                            new_str = ",".join(line_arr)
                            f.write(new_str + "\n")

    #### folders in BSSH-managed projects we want to keep                        
    folders_whitelist = ['/datasets/','/ilmn-runs/','/ilmn-analyses/','/scratch/']
    ### Check on data in source_project to delete
    delete_timestamp = get_timestamp_previous_days(args.days_to_delete)
    delete_timestamp = re.sub("\\s+","T",str(delete_timestamp))
    delete_timestamp +=  "Z" 
    delete_data_candidates = ica_analysis_launch.list_project_data_by_time(my_api_key,source_project_id,delete_timestamp)
    for folder_idx,folder_metadata in enumerate(delete_data_candidates):
        if folder_metadata['data']['details']['path'] not in folders_whitelist:
            if re.search("BaseSpace",folder_metadata['data']['details']['application']['name']) is not None:
                if folder_metadata['data']['details']['status'] == "AVAILABLE" :
                    logging_statement(f"BaseSpace-managed data: {folder_metadata}")
                    ica_data_transfer.delete_data_basespace_managed()
                elif folder_metadata['data']['details']['status'] == "ARCHIVED" :
                    logging_statement(f"BaseSpace-managed data: {folder_metadata}")
                    ica_data_transfer.unarchive_data_basespace_managed()
                    ica_data_transfer.delete_data_basespace_managed()
            elif re.search("ICA",folder_metadata['data']['details']['application']['name']) is not None:
                if folder_metadata['data']['details']['status'] == "AVAILABLE" :
                    logging_statement(f"ICA-managed data: {folder_metadata}")
                    ica_data_transfer.delete_data_ica_managed()
                elif folder_metadata['data']['details']['status'] == "ARCHIVED" :
                    logging_statement(f"ICA-managed data: {folder_metadata}")
                    ica_data_transfer.unarchive_data_ica_managed()
                    ica_data_transfer.delete_data_ica_managed()
    logging_statement(f"Deleting data in {source_project_id} created {delete_timestamp} or earlier")
    
    ### Check on data in source_project to archive
    archive_timestamp = get_timestamp_previous_days(args.days_to_archive)
    archive_timestamp = re.sub("\\s+","T",str(archive_timestamp))
    archive_timestamp +=  "Z" 
    archive_data_candidates = ica_analysis_launch.list_project_data_by_time(my_api_key,source_project_id,archive_timestamp)
    for folder_idx,folder_metadata in enumerate(archive_data_candidates):
        if folder_metadata['data']['details']['path'] not in folders_whitelist:
            folder_of_interest = metadata_with_tag(folder_metadata['data']['details'])
            logging_statement(f"{folder_of_interest}")
            if re.search("BaseSpace",folder_metadata['data']['details']['application']['name']) is not None:
                if folder_metadata['data']['details']['status'] == "AVAILABLE" :
                    if folder_of_interest:
                        logging_statement(f"BaseSpace-managed data: {folder_metadata}")
                        ica_data_transfer.archive_data_basespace_managed()
                elif folder_metadata['data']['details']['status'] == "ARCHIVED" :
                    if folder_of_interest:
                        logging_statement(f"No need to touch BaseSpace-managed archived data: {folder_metadata}")

            elif re.search("ICA",folder_metadata['data']['details']['application']['name']) is not None:
                if folder_metadata['data']['details']['status'] == "AVAILABLE" :
                    if folder_of_interest:
                        logging_statement(f"ICA-managed data: {folder_metadata}")
                        ica_data_transfer.archive_data_ica_managed()
                elif folder_metadata['data']['details']['status'] == "ARCHIVED" :
                    if folder_of_interest:
                        logging_statement(f"No need to touch ICA-managed archived data: {folder_metadata}")

    logging_statement(f"Archiving data in {source_project_id} created {archive_timestamp} or earlier")



if __name__ == "__main__":
    main()