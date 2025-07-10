# umich_to_emedgene

A script to help grab DRAGEN analysis ICA file paths for case ingestion.


## umich_to_emedgene.py command line examples

```bash
python umich_to_emedgene.py --api_key_file /opt/api_key.txt --source_project_name ken_debug 
```

## umich_to_emedgene.py command line options

- ```--api_key_file``` : path to text file containing API Key for ICA
- ```--source_project_name``` : Name of the ICA project where your Autolaunched analysis resides
- ```--dry_run``` : [OPTIONAL] flag that allows you to run the script without updating the analyses_managed_table
- ```--verbose``` : [OPTIONAL] flag that increases the verbosity of the script
- ```--analyses_files_of_interest``` : [OPTIONAL] path to an allowlist of file extensions used for Emedgene Case-ingestion. This [file](https://github.com/keng404/umich_orchestrator/blob/main/analyses_files_of_interest.txt) by default. This list of file extensions are taken from [here](https://help.emg.illumina.com/release-notes/workbench-and-pipeline-updates/new_in_emedgene_36_-october_8th_2024) but users can modify this list to more accurately reflect their use-case. More guidance on what files Emedgene can ingest can be found [here](https://help.emg.illumina.com/emedgene-analyze-manual/creating_multiple_cases/csv_format_requirements#required-bssh-file-path-format)

## [umich_to_emedgene](https://github.com/keng404/umich_orchestrator/blob/main/umich_to_emedgene.py) Process Outline

0) Messages from the script are written by default logfile ```orchestrator.log``` in the directory where you run the script
1) Monitor DRAGEN Germline analyses in a project you specify
2) If analysis is completed successfully
	- Check if analysis is in a triggered analysis table ```analyses_managed_table.txt```. See [here](https://github.com/keng404/umich_orchestrator/blob/main/analyses_managed_table.txt) for example
	- This can filepath can be customized
	- Format is ```analysis_id_managed,run_id```
		- ```analysis_id_managed``` = analysis_id of DRAGEN germline analysis
		- ```run_id``` = folder name 		
	- If analysis is not in the managed analysis table
		- Identify analysis files generated and subset based on the file extensions listed in this [file](https://github.com/keng404/umich_orchestrator/blob/main/analyses_files_of_interest.txt) by default
     	- create a **dragen_analysis_manifest** file to store information so end-users can download data if needed. See [here](https://github.com/keng404/umich_orchestrator/blob/main/20250707_103905-c4c751ad-3846-4d9c-8611-3483cfd3f0ed.dragen_analysis_manifest.csv) for example 
			- file format of this CSV file is as follows ```sample_id,file_name,file_path,data_id,project_id,analysis_id,pipeline_urn```
				- ```sample_id``` : sample id associated to the file
				- ```file_name``` : name of the  file
				- ```file_path``` : path of the  file on ICA
				- ```data_id``` : data id of the  file on ICA
				- ```project_id``` : project id of  project where the files  are located
				- ```pipeline_urn``` : pipeline URN (Unified Resource Name). A long string that contains the pipeline name and pipeline id. Helps associate pipeline version to analysis data 
			- this file is also uploaded to ICA in a folder named ```dragen_analysis_manifests``` in the source project ```--source_project_name``` you provide in the command line


## Addtional notes

The script functionality can be extended to more explicity handle Emedgene case submission via [CLI](https://help.emg.illumina.com/emedgene-analyze-manual/creating_multiple_cases/batch-case-upload-via-cli) or [API](https://help.emg.illumina.com/integrations/api-beginner-guide#api-reference).

The key data structure ```case_file_metadata``` is a dictionary created for each DRAGEN analysis. The primary key is the ```sample id``` DRAGEN associates to an analysis file along with metadata (i.e. ICA path, ICA analysis id, pipeline id, etc.) that can be used to craft a batch case CSV --- see this [repository](https://github.com/keng404/emg_case_creation) for a demo of how this could work --- or API call to Emedgene to create a case.
