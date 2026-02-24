##### This is an example of job API search for an array of molecules #####

import requests
import os
from typing import List,Optional,Dict,Any
import time
import json
from utils import submit_molsearch,get_job_status,get_molsearch_page

API_KEY="Your API Key" 

### Define your input molecules (array) and parameters
INPUT_MOLECULES=[ "CC(=O)Oc1ccccc1C(=O)O", "CC(=O)Oc1ccncc1C(=O)O", "CC(=O)Oc1cnccc1C(=O)O" ]

### Embedding similarity threshold (molecules returned have >=0.7 th)
SIM_TH=0.7  # 0 to 1 range

### Custom ranges for ADMET Properties/Descriptors. Available property names are in ./available_properties
PROP_RANGES={ 
            "lipophilicity_astrazeneca": { "min": -2, "max": -1 },
            "h_bond_acceptors": { "min": 2, "max": 5 } 
            }

### Database name
DB_NAME="ZINC15"


## Prepare data structure for final molecules
FINAL_MOLECULES={}

for k,query in enumerate(INPUT_MOLECULES):
    print(f"SEARCHING for query {k+1}/{len(INPUT_MOLECULES)} --> ",query)
    # Step 1 : Submit a CHEESE job for each individual molecules
    job_name=submit_molsearch(search_input=query,
                    search_type="espsim_shape",
                    search_quality="fast",
                    db_names=[DB_NAME],
                    API_KEY=API_KEY)


    # Step 2 : Wait for its completion 

    job_status=get_job_status(job_name,API_KEY=API_KEY)

    while job_status!="SUCCESS":
        print(f"Job is {job_status} Waiting...")
        time.sleep(1)
        job_status=get_job_status(job_name,API_KEY=API_KEY)

    print(f"Job is completed")



    # Step 3 : Extract filtered molecules with their properties
    DB_NAME="ZINC15"
    FINAL_MOLECULES[query]={}

    print("Extracting filtered properties (will take approximately 3 mins)...")
    for page_num in range(100):
        if page_num%20==0:
            print(f"Progress {page_num}%")
        page=get_molsearch_page(job_name=job_name,
                                db_name=DB_NAME,
                                page_size=1000,
                                page_num=page_num,
                                prop_ranges=PROP_RANGES,
                                sim_th=SIM_TH,
                                API_KEY=API_KEY)
        filtered_idx=[i for i in range(len(page["in_prop_range"])) if page["in_prop_range"][i]]
        for key,values in page.items():
            if key in FINAL_MOLECULES[query]:
                FINAL_MOLECULES[query][key]+=[values[i] for i in filtered_idx]
            else:
                FINAL_MOLECULES[query][key]=[values[i] for i in filtered_idx]


## Step 5 : Save the results as JSON
## The output is a JSON with keys as queries and results as values
json.dump(FINAL_MOLECULES,open(f"cheese_{DB_NAME}_filtered_results.json","w"),indent=3)