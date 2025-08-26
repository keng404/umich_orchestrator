# umich_orchestrator
An orchestration approach to monitor analyses from Illumina Connected Analytics (ICA) and perform downstream activity for MGI use

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/keng404/umich_orchestrator)

## installation instructions
1) Run locally 
- use ```git clone``` or copy ZIP file and extract contents to desired directory
- use the [requirements.txt](https://github.com/keng404/umich_orchestrator/blob/main/requirements.txt) file to install python modules that are required for script to run properly
2) Run in ICA Bench workspace
- Create ICA Bench workspace and follow steps from ```1)``` to install prequisite modules and run orchestrator script
3) Docker
- Run ```docker pull keng404/umich_orchestrator:0.0.2``` and mount API key and BaseSpace access token file
Current image digest is ```sha256:5711afc83898319bc92783c2b02cced236fb30c12d9af368cf0d5420a4e1554e```
- To verify the image digest, you can run ```docker images --digests``` and check the digest column to see if the digest value matches the one above
- Or you can run 
```bash 
docker pull keng404/umich_orchestrator@sha256:5711afc83898319bc92783c2b02cced236fb30c12d9af368cf0d5420a4e1554e
``` 
when initially pulling the image


## umich_orchestrator.py FAQs

1) How do I generate an API key?
See this [ICA help page](https://help.ica.illumina.com/get-started/gs-getstarted#api-keys). You can either save during key generation or copy+paste to any text file of your choosing.
2) How do I generate an access token file
Download BaseSpace CLI from the [BaseSpace CLI documentation](https://developer.basespace.illumina.com/docs/content/documentation/cli/cli-overview#InstallBaseSpaceSequenceHubCLI) and run the following command.
```bash
bs auth --force --scopes  'READ GLOBAL, CREATE GLOBAL, BROWSE GLOBAL,CREATE PROJECTS, CREATE RUNS, START APPLICATIONS, MOVETOTRASH GLOBAL, WRITE GLOBAL, EMPTY TRASH'
```
By default this will create a file called ```~/.basespace/default.cfg```, this is your ```--basespace_access_token_file```
3) What happens if analysis fails?
Only the happy-path has been implemented. So the orchestrator has no retry logic.
You can use  the script [here](https://github.com/keng404/bssh_parallel_transfer/blob/master/requeue.md) or the [ICA requeue template app](https://keneng87.pyscriptapps.com/ica-analysis-requeue/latest/) if you don't have a template handy. Presuming you have the ICA CLI installed you should be able to copy+paste these templates.

## umich_orchestrator.py command line examples

```bash
python umich_orchestrator.py --api_key_file /opt/api_key.txt --source_project_name ken_debug --destination_project_name Ken_demos 
```

## umich_orchestrator.py command line options

- ```--api_key_file``` : path to text file containing API Key for ICA
- ```--basespace_access_token_file``` : path to text file contain BaseSpace access token
- ```--source_project_name``` : Name of the ICA project where your Autolaunched analysis resides
- ```--destination_project_name``` :  Name of the ICA project where your downstream users --- only need to specify this if the projects are different.
- ```--days_to_archive``` : Data older than these many days will be archived (default is 60 days).
- ```--days_to_delete``` : Data older than these many days will be deleted (default is 90 days).
- ```--dry_run``` : [OPTIONAL] flag that allows you to run the script without performing any linking and updating the analyses_managed_table

## Approach

## helper functions

- [ica_analysis_monitor.py](https://github.com/keng404/umich_orchestrator/blob/main/ica_analysis_monitor.py)
	- API calls and functions to monitor a given analyses
- [ica_analysis_launch.py](https://github.com/keng404/umich_orchestrator/blob/main/ica_analysis_launch.py)
	- API calls and functions to identify a previously run analysis and craft an API template
- [ica_data_transfer.py](https://github.com/keng404/umich_orchestrator/blob/main/ica_data_transfer.py)
	- API calls and functions to aid in download and upload to ICA using BOTO3 (i.e. AWS s3)
- [ica_analysis_outputs.py](https://github.com/keng404/umich_orchestrator/blob/main/ica_analysis_outputs.py)
	- API calls and functions to directly identify the output folder and files from an ICA analysis
- [bssh_utils.py](https://github.com/keng404/umich_orchestrator/blob/main/ica_analysis_outputs.py)
	- API calls and functions to directly identify datasets and runs we can archive/unarchive and delete

## orchestrator wrappers

These are scripts or cron jobs that invoke the orchestrator scripts at a cadence (default. every 10 minutes) that can be configured.

Some guidance on setting up a cron job can be found on this [page](https://www.redhat.com/en/blog/linux-cron-command)

An example bash script that runs every 5 minutes can be found below. Note that this is demo code and has not been tested yet.
- [umich_orchestrator_wrapper.sh](https://github.com/keng404/umich_orchestrator/blob/main/umich_orchestrator_wrapper.sh)

## orchestrator(s)

These are custom script(s) based on an end-user's use-case

## [umich_orchestrator](https://github.com/keng404/umich_orchestrator/blob/main/umich_orchestrator.py) Process Outline

0) Messages from the script are written by default logfile ```orchestrator.log``` in the directory where you run the script
	- As a convenience, you can also use the Docker image ```keng404/umich_orchestrator:0.0.1``` to run this script
1) Monitor BCLConvert analyses in a project you specify
2) If analysis is completed
	- Check if analysis is in a triggered analysis table ```analyses_managed_table.txt```. See [here](https://github.com/keng404/umich_orchestrator/blob/main/analyses_managed_table.txt) for example
	- This can filepath can be customized
	- Format is ```analysis_id_managed,run_id```
		- ```analysis_id_managed``` = analysis_id of BCLConvert analysis
		- ```run_id``` = folder name 		
	- If analysis is not in the managed analysis table
		- link FASTQ data to downstream project if it matches MGI file nomenclature
     		- create a ***download manifest csv*** file to store information so end-users can download data if needed. See [here](https://github.com/keng404/umich_orchestrator/blob/main/20240529_LH00619_0013_B22CCFJLT4_a912e3_9d578d-BclConvert_v4_1_23_patch1-7898625c-01f0-4cc0-b9ed-89216d700613.download_manifest.csv) for example 
				- file format of this CSV file is as follows ```file_name,file_path,data_id,project_id```
					- ```file_name``` : name of the FASTQ file
					- ```file_path``` : path of the FASTQ file on ICA
					- ```data_id``` : data id of the FASTQ file on ICA
					- ```project_id``` : project id of MGI project where the FASTQs are linked to
				- this file is also uploaded to ICA in the MGI project in a folder named ```/fastq_manifests/``` using the value you provide to ```--destination_project_name``` you provide in the command line
3) Check for data to delete
	- Query analyses ran in the ```source_project_name``` ( this will be your BaseSpace managed project) that are older than 90 days --- this is configurable.
	- For each of these analyses, look for a file named ```bsshoutput.json``` in the output. This file will identify by name which BaseSpace projects we want to focus on. 
		- For each BaseSpace project, identify datasetIds we should delete based on creation timestamp and our settings (default is ```older than 90 days```). We also check if the dataset is archived, as we'll need to restore it from archival storage in order to delete it.
	- Query BaseSpace for to identify runIds we should delete based on creation timestamp and our settings (default is ```older than 90 days```). We also check if the run is archived, as we'll need to restore it from archival storage in order to delete it.
	- If analysis does not have ```bsshoutput.json``` in the output, assume that the analysis is managed by ICA and we delete using the ICA API. This may be the case if you need to manually run or requeue an analysis in the BaseSpace managed project. If analysis data is managed by ICA --- we delete the underlying data if it is older than our settings (default is ```older than 90 days```). We also check if the data is archived, as we'll need to restore it from archival storage before deleting.
4) Check for data to archive
	- Query analyses ran in the ```source_project_name``` ( this will be your BaseSpace managed project) that are between 60 to 90 days old --- this is configurable.
	- For each of these analyses, look for a file named ```bsshoutput.json``` in the output. This file will identify by name which BaseSpace projects we want to focus on. If analysis does not have ```bsshoutput.json``` in the output, assume that the analysis is managed by ICA and we archive using the ICA API.
		- For each BaseSpace project, identify datasetIds we should archive based on creation timestamp and our settings (default is ```between 60 and 90 days old```). 
	- Query BaseSpace for to identify runIds we should delete based on creation timestamp and our settings (default is ```between 60 and 90 days old```).

## umich_orchestrator.py TODO list

- [X] Add in code for BaseSpace data management (i.e. deletion and archiving of data)
- [X] [OPTIONAL] Add bash_wrapper to run orchestrator script every 5/10 minutes
- [X] Give instructions for setting up Cron job
- [X] Build and push official docker image based-off of this [Dockerfile](https://github.com/keng404/umich_orchestrator/blob/main/Dockerfile)
- [ ] Add in logic for Clinical Groups needs --- Still gathering requirements
- [ ] Test data management for manually queued/re-queued ICA analyses




