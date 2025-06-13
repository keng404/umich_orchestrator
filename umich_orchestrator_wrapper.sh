#!/usr/bin/bash

usage()
{
        echo "umich_orchestrator_wrapper.sh"
        echo "Ken Eng 13-Jun-2025"
        echo
        echo "Runs umich.py"
        echo "An orchestration approach to monitor analyses from Illumina Connected Analytics (ICA)"
        echo "and carry out downstream Clinical and MGI activity"
        echo
        echo "Usage: $0 [-a api_key_file] [-s source_project_name] [-m pipeline_name_to_monitor] \ "
        echo "                           [-t pipeline_name_to_trigger]  \ "
        echo "                           [OPTIONAL] [-d destination_project_name] [-f api_template_file] \ "
        echo
        echo "  [-a api_key_file]            	- path to text file containing API Key for ICA"
        echo "  [-s source_project_name]  		- Name of the ICA project where your BCLConvert/DRAGEN analysis resides"
        echo "  [-d destination_project_name]   	- [OPTIONAL] Name of the ICA project that your MGI group(s) use "
        echo "  [-b basespace_access_token_file]            	- path to text file containing access token for BaseSpace"
        echo "  [-h help]                 		- Display this page"
        exit
}

#--api_key_file : path to text file containing API Key for ICA
#--source_project_name : Name of the ICA project where your BCLConvert/DRAGEN analysis analysis resides
#--destination_project_name : Name of the ICA project that your MGI group(s) use
#--basespace_access_token_file : path to text file containing access token for BaseSpace
api_key_file=""
source_project_name=""
destination_project_name=""
basespace_access_token_file=""
# Seconds between checks
update_delay_sec=600 # Candence between each trigger of the fcs.ICA_to_CGW.orchestrator.py script
while getopts ":a:s:d:m:t:f:h" Option
        do
        case $Option in
                a ) api_key_file="$OPTARG" ;;
                s ) source_project_name="$OPTARG" ;;
                d ) destination_project_name="$OPTARG" ;;
                b ) basespace_access_token_file="$OPTARG" ;;
                h ) usage ;;
                * ) echo "Unrecognized argument. Use '-h' for usage information."; exit 255 ;;
        esac
done
shift $(($OPTIND - 1))


if [[ "$api_key_file" == "" || "$basespace_access_token_file" == "" || "$destination_project_name" == ""   || "$source_project_name" == "" ]]
then
        usage
fi

if [ ! -r "$api_key_file" ]
then
	echo "Error: can't open API KEY FILE ($1)." >&2
	exit 1
fi

if [ ! -r "$basespace_access_token_file" ]
then
	echo "Error: can't open BaseSpace access token FILE ($1)." >&2
	exit 1
fi



# Loop until control-C'ed out
while : ; do
	clear
	python umich_orchestrator.py --api_key_file "$api_key_file" --source_project_name "$source_project_name" --destination_project_name "$destination_project_name" --basespace_access_token_file "$basespace_access_token_file"
sleep $update_delay_sec
done  
