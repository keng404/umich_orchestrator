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
import bssh_utils
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
    name_prefix_split = name_prefix.split('-')
    ### matches on M-123-345*.fastq.gz
    ### will also match on M_123_4325*.fastq.gz
    # check if the FASTQ starts with an  'M'
    if name_prefix_split[0] == "M" or re.search("^M",name_prefix[0]) is not None:
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
################    
def check_basespace_project(credentials,project_list):
    id_list = []
    ### keys of this dict are basespace ids, we will interate through the dict
    ### and match on project name
    basespace_project_dict = bssh_utils.list_basespace_projects(credentials)
    
    for project_name in project_list:
        if project_name in list(basespace_project_dict.keys()):
            id_list.append(basespace_project_dict[project_name]['Id'])
        else:
            logging_statement(f"WARNING: could not find BaseSpace project named {project_name}")
    return id_list

def file_extensions_for_emedgene(allowlist):
    file_extensions_to_whitelist = []
    with open(allowlist,'r') as f:
        for line in f.readlines():
            line = line.strip("\n")
            file_extensions_to_whitelist.append(line)
    return file_extensions_to_whitelist
###################################################
### Here SOURCE and DESTINATION project refer to a BSSH-managed project in ICA and downstream project
################
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_project_id',default=None, type=str, help="SOURCE ICA project id")
    parser.add_argument('--source_project_name',default=None, type=str, help="SOURCE ICA project name")
    parser.add_argument('--analyses_files_of_interest', default='analyses_files_of_interest.txt', type=str, help="Allowlist of file extensions to pass on to Emedgene")
    parser.add_argument('--analyses_monitored_file', default='analyses_monitored_file.txt', type=str, help="Table containing ICA analyses of interest")
    parser.add_argument('--analyses_managed_table', default='analyses_managed_table.txt', type=str, help="Table recording ICA analyses that have been managed")
    parser.add_argument('--api_key_file', default=None, type=str, help="file that contains API-Key")
    parser.add_argument('--server_url', default='https://ica.illumina.com', type=str, help="ICA base URL")
    parser.add_argument('--verbose', action="store_true",default=False, help="run script in verbose mode --- print out files we find")
    parser.add_argument('--dry_run', action="store_true",default=False, help="run script in dry run mode --- no analyses will be launched, but full script will be run")
    args, extras = parser.parse_known_args()
    #############
    source_project_id = args.source_project_id
    source_project_name = args.source_project_name
    analyses_monitored_file = args.analyses_monitored_file
    analyses_managed_table = args.analyses_managed_table
    ########### identify file extensions of interest
    analyses_files_of_interest = args.analyses_files_of_interest
    emedgene_file_exts_of_interest = []
    ##############
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

    if os.path.isfile(analyses_files_of_interest) is True:
        emedgene_file_exts_of_interest = file_extensions_for_emedgene(analyses_files_of_interest)
        emedgene_file_exts_of_interest_str = ", ".join(emedgene_file_exts_of_interest)
        if args.verbose:
            print(f"emedgene_file_exts_of_interest: {emedgene_file_exts_of_interest_str}")
    else:
        logging_statement(f"WARNING: Could not find file extensions list --- will output paths for all analysis files")

    #### get the project identifiers we need
    if source_project_id is None and source_project_name is not None:
        logging_statement("Grabbing ICA SOURCE PROJECT ID")
        source_project_id = ica_analysis_launch.get_project_id(my_api_key,source_project_name)
    if source_project_id is None:
        raise ValueError("Need to provide project name or project id")

    ####### now let's set up pipeline analysis by updating the template
    
    ### STEP1: grab analyses of interest to monitor and analysis_ids_of_interest to manage
    logging_statement("STEP1: grab analyses of interest to monitor and analysis_ids_of_interest to manage")
    analysis_ids_of_interest = []
    analysis_ids_to_manage = []

    logging_statement(f"Grabbing analyses for {source_project_id}")
    analyses_list = ica_analysis_monitor.list_project_analyses(my_api_key,source_project_id)

    ### only keeping track of analyses in these status, any analyses aborted or failed will not be monitored
    ### focus only on DRAGEN Germline analyses
    logging_statement(f"Subsetting analyses_to_monitor and analyses_to_manage for {source_project_id}")
    desired_analyses_status_to_monitor = ["REQUESTED","INTIALIZED","INPROGRESS",'QUEUED', 'INITIALIZING', 'PREPARING_INPUTS', 'GENERATING_OUTPUTS']
    desired_analyses_status_to_manage = ["SUCCEEDED"]

    for aidx,project_analysis in enumerate(analyses_list):
        if (re.search('DRAGEN',project_analysis['pipeline']['code'],re.IGNORECASE) is not None and re.search('Germline',project_analysis['pipeline']['code'],re.IGNORECASE) is not None) and project_analysis['status'] in desired_analyses_status_to_monitor:
            analysis_ids_of_interest.append(project_analysis['id'])
        elif (re.search('DRAGEN',project_analysis['pipeline']['code'],re.IGNORECASE) is not None and re.search('Germline',project_analysis['pipeline']['code'],re.IGNORECASE) is not None) and project_analysis['status'] in desired_analyses_status_to_manage:
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
                data_to_summarize = {}
                output_folder_id = None
                output_folder_path = None
                run_id = None
                all_files = []
                #### we only want FASTQ files, no undetermined FASTQ files
                for output in analysis_output:
                    path_normalized = output['path'].strip("/$")
                    path_normalized = path_normalized.strip("^/+") 
                    if os.path.basename(path_normalized) == analysis_metadata['reference']:
                        output_folder_id = output['id']
                        output_folder_path = output['path']
                        run_id = analysis_metadata['reference']
                    #### identify files of interest
                    if len(emedgene_file_exts_of_interest) > 0:
                        ### for each file extention of interest found in the emedgene_file_exts_of_interest file
                        ### gather metadata we will use to populate a csv file
                        for file_ext in emedgene_file_exts_of_interest:
                            if re.search(f"{file_ext}$",os.path.basename(path_normalized)) is not None:
                                all_files.append(output['path'])
                                data_information = {}
                                data_information['path'] =  output['path']
                                data_information['name'] = output['name']
                                data_information['pipeline_urn'] = analysis_metadata['pipeline']['urn']
                                data_to_summarize[output['id']] = data_information
                    else:
                        all_files.append(output['path'])
                        data_information = {}
                        data_information['path'] =  output['path']
                        data_information['name'] = output['name']
                        data_to_summarize[output['id']] = data_information        
                    ###########    
                    if args.verbose:
                        if output['id'] in list(data_to_summarize.keys()):
                            pprint(data_to_summarize[output['id']])                

                ##### raise error condition if we can't find files of interest        
                if len(list(data_to_summarize.keys())) < 1:
                    logging_statement(f"WARNING: Could not find DRAGEN analysis files from ICA analysis: {analysis_metadata['reference']}")
                    logging_statement(f"Found these files: {all_files}")


                ### Create DRAGEN analysis file and upload to ICA
                ### output file will be based on run_id
                if args.dry_run is False and len(list(data_to_summarize.keys())) > 0:
                    dragen_manifest_file = f"{run_id}.dragen_analysis_manifest.csv"
                    logging_statement(f"Creating DRAGEN analysis manifest {dragen_manifest_file}")
                    with open(dragen_manifest_file, 'w') as f:
                        line_arr = ["sample_id","file_name","file_path","data_id","project_id","analysis_id","pipeline_urn"]
                        new_str = ",".join(line_arr)
                        f.write(new_str + "\n")
                        ### name, path, data_id, project_id, etc.
                        case_file_metadata = {}
                        for ids,dts in enumerate(data_to_summarize):
                            sample_id_inferred = os.path.basename(os.path.dirname(data_to_summarize[dts]['path']))
                            new_line = [sample_id_inferred,data_to_summarize[dts]['name'],data_to_summarize[dts]['path'],dts,source_project_id,id,data_to_summarize[dts]['pipeline_urn']]
                            new_line_str = ",".join(new_line)
                            if sample_id_inferred not in case_file_metadata.keys():
                                case_file_metadata[sample_id_inferred] = []
                                case_file_metadata[sample_id_inferred].append(new_line_str)
                            else:
                                case_file_metadata[sample_id_inferred].append(new_line_str)
                        # perform write
                        for sample_idx,sample_name in enumerate(case_file_metadata):
                            for new_line_str in case_file_metadata[sample_name]:
                                if args.verbose:
                                    print(f"DRAGEN analysis_file metadata:{new_line_str}")
                                f.write(new_line_str + "\n")
                    ######## upload manifest file to ICA
                    logging_statement(f"Upload DRAGEN analysis manifest {dragen_manifest_file} to ICA")
                    manifest_file_id = create_data(my_api_key,source_project_name, os.path.basename(dragen_manifest_file), "FILE",filepath="/dragen_analysis_manifests/",project_id=source_project_id)
                    ### perform actual upload
                    creds = ica_data_transfer.get_temporary_credentials(my_api_key,source_project_id, manifest_file_id)
                    ica_data_transfer.set_temp_credentials(creds)
                    ica_data_transfer.upload_file(dragen_manifest_file,creds)

                if args.dry_run is False:
                    if os.path.exists(analyses_managed_table) is True:
                        logging_statement(f"Adding analysis managed from {analyses_managed_table}")
                        #### format is analysis_id_managed,run_id
                        with open(analyses_managed_table, 'a+') as f:
                            if run_id is None:
                                run_id = "Unknown"
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
                            
                            if run_id is None:
                                run_id = "Unknown"
                            line_arr = [id,run_id]
                            new_str = ",".join(line_arr)
                            f.write(new_str + "\n")



if __name__ == "__main__":
    main()