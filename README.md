# umich_orchestrator
An orchestration approach to monitor analyses from Illumina Connected Analytics (ICA) and perform downstream activity for MGI use

## approach

## helper functions

- [ica_analysis_monitor.py](https://github.com/keng404/ica_to_cgw/blob/main/ica_analysis_monitor.py)
	- API calls and functions to monitor a given analyses
- [ica_analysis_launch.py](https://github.com/keng404/ica_to_cgw/blob/main/ica_analysis_launch.py)
	- API calls and functions to identify a previously run analysis and craft an API template
- [ica_data_transfer.py](https://github.com/keng404/ica_to_cgw/blob/main/ica_data_transfer.py)
	- API calls and functions to aid in download and upload to ICA using BOTO3 (i.e. AWS s3)
- [ica_analysis_outputs.py](https://github.com/keng404/ica_to_cgw/blob/main/ica_analysis_outputs.py)
	- API calls and functions to directly identify the output folder and files from an ICA analysis
## orchestrator wrappers

These are scripts or cron jobs that invoke the orchestrator scripts at a cadence (default. every 10 minutes) that can be configured.

Some guidance on setting up a cron job can be found on this [page](https://www.redhat.com/en/blog/linux-cron-command)

## orchestrator(s)

These are custom script(s) based on an end-user's use-case

## [umich_orchestrator](https://github.com/keng404/umich_orchestrator/blob/main/umich_orchestrator.py) Process Outline

0) Messages from the script are written by default logfile ```orchestrator.log``` in the directory where you run the script
	- As a convenience, you can also use the Docker image ```keng404/umich_orchestrator:0.0.1``` to run this script
1) Monitor BCLConvert analyses in a project you specificy
2) If analysis is completed
	- Check if analysis is in a triggered analysis table ```analyses_managed_table.txt```
	- This can filepath can be customized
	- Format is ```analysis_id_managed,run_id```
		- ```analysis_id_managed``` = analysis_id of BCLConvert analysis
		- ```run_id``` = folder name 		
	- If analysis is not in the managed analysis table
		- link FASTQ data to downstream project if it matches MGI file nomenclature
     		- create a manifest csv file to

## umich_orchestrator.py TODO list

- [ ] Add in code for data management (i.e. deletion and archiving of data)
- [ ] [OPTIONAL] Add bash_wrapper to run orchestrator script every 5/10 minutes
- [X] Give instructions for setting up Cron job
- [X] Build and push official docker image based-off of this [Dockerfile](https://github.com/keng404/umich_orchestrator/blob/main/Dockerfile)
- [ ] [OPTIONAL] Add data management functionality to remove linked data or duplicate data?? 

## umich_orchestrator.py FAQs

1) How do I generate an API key?
See this [ICA help page](https://help.ica.illumina.com/get-started/gs-getstarted#api-keys). You can either save during key generation or copy+paste to any text file of your choosing.
2) What happens if analysis fails?
Only the happy-path has been implemented. So the orchestrator has no retry logic.
You can use  the script [here](https://github.com/keng404/bssh_parallel_transfer/blob/master/requeue.md) or the [ICA requeue template app](https://keneng87.pyscriptapps.com/ica-analysis-requeue/latest/) if you don't have a template handy. Presuming you have the ICA CLI installed you should be able to copy+paste these templates.

## umich_orchestrator.py command line examples

```bash

# TEST2 : analysis you monitor and trigger will be in the different ICA projects
python umich_orchestrator.py --api_key_file /opt/api_key.txt --source_project_name ken_debug --destination_project_name Ken_demos 
```

## fcs.ICA_to_CGW.orchestrator.py command line options

- ```--api_key_file``` : path to text file containing API Key for ICA
- ```--source_project_name``` : Name of the ICA project where your Autolaunched analysis resides
- ```--destination_project_name``` :  Name of the ICA project where your downstream users --- only need to specify this if the projects are different.
- ```--days_to_archive``` : Data older than these many days will be archived (default is 60 days).
- ```--days_to_delete``` : Data older than these many days will be deleted (default is 90 days).
- ```--dry_run``` : [OPTIONAL] flag that allows you to run the script without performing any linking and updating the analyses_managed_table


