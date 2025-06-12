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

def dummy_function(creds,**kwargs):
    basespace_url = f"https://api.basespace.illumina.com/v2/restore"
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['Authorization'] = f"Bearer {creds}"
    headers['Content-Type'] = "application/json"
    data = dict()
    print(f"{kwargs['run_ids']}")
    for key, value in kwargs.items():
        if key == "dataset_ids":
            data['DatasetIds'] = value
        elif key == "run_ids":
            data['RunIds'] = value
        elif key == "project_ids":
            data['ProjectIds'] = value  
    return data

x = dummy_function("ABC",dataset_ids = ['1','2','4'],run_ids = ['1'])

print(x)