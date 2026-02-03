import requests
import os
from typing import List,Optional,Dict,Any
import time
import json

API_KEY="Your CHEESE API Key here" 


CHEESE_URL="https://api.cheese.deepmedchem.com"


def submit_molsearch(
    search_input: str,
    search_type: str = "espsim_shape", # espsim_shape, espsim_electrostatic, morgan
    search_quality: str = "fast", # fast, accurate, very_accurate
    db_names: List[str] = ["ZINC15"]
):
    """
    Submit a molecule search job and return the job_name.
    """

    url = f"{CHEESE_URL}/submit_molsearch"

    headers = {
        "accept": "application/json",
        "X-API-Key": API_KEY,
    }

    params = {
        "search_input": search_input,
        "search_type": search_type,
        "search_quality": search_quality,
        "db_names": db_names
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # raises if 4xx/5xx

    job_name=response.json()
    return job_name


def get_job_status(job_name:str):
    """
    Monitor job status
    """

    url = f"{CHEESE_URL}/job_status"

    headers = {
        "accept": "application/json",
        "X-API-Key": API_KEY,
    }

    params = {
        "job_name": job_name
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # raises if 4xx/5xx

    status=response.json()
    return status


def get_molsearch_page(
    job_name: str,
    db_name: str = "ZINC15",
    page_size: int = 30,
    page_num: int = 0,
    include_properties: bool = True,
    sim_th: float = 0,
    prop_ranges: Optional[Dict[str, Dict[str, float]]] = None
) -> Dict[str, Any]:
    """
    Fetch a page of molecule search results from a job with filtering 
    - The maximum items that you can extract in total is 100K molecules.
    - You can filter by the similarity threshold parameter sim_th. Molecules returned have similarity >= sim_th
    - You can filter by descriptor/ADMET property ranges. 
    """

    url = f"{CHEESE_URL}/get_molsearch_page"

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
        "X-API-Key": API_KEY,
    }


    params = {
        "job_name": job_name,
        "db_name": db_name,
        "page_size": page_size,
        "page_num": page_num,
        "include_properties": str(include_properties).lower(),
        "sim_th": sim_th
    }

    payload = {
        "prop_ranges": prop_ranges or {}
    }

    response = requests.post(
        url,
        headers=headers,
        params=params,
        json=payload,
    )
    response.raise_for_status()

    return response.json()



### Use the functions

# Step 1 : Submit a CHEESE job 
job_name=submit_molsearch(search_input="CCCC",
                search_type="espsim_shape",
                search_quality="fast",
                db_names=["ZINC15"])


# Step 2 : Wait for its completion 

job_status=get_job_status(job_name)

while job_status!="SUCCESS":
    print(f"Job is {job_status} Waiting...")
    time.sleep(1)
    job_status=get_job_status(job_name)

print(f"Job is completed")


### Step 3 : Define your custom similarity, descriptors, and ADMET filters
### Supported properties/descriptors are in ./available_properties.txt

sim_th=0.7
prop_ranges={ 
            "lipophilicity_astrazeneca": { "min": -2, "max": -1 },
            "heavy_atoms": { "min": 3, "max": 6 },
            "h_bond_acceptors": { "min": 2, "max": 2 } 
            }



# Step 4 : Extract filtered molecules with their properties
db_name="ZINC15"
filtered_molecules={}

print("Extracting filtered properties...")
for page_num in range(100):
    if page_num%10==0:
        print(f"Progress {page_num}%")
    page=get_molsearch_page(job_name=job_name,
                            db_name=db_name,
                            page_size=1000,
                            page_num=page_num,
                            prop_ranges=prop_ranges)

    filtered_idx=[i for i in range(len(page["in_prop_range"])) if page["in_prop_range"][i]]
    
    for key,values in page.items():
        if key in filtered_molecules:
            filtered_molecules[key]+=[values[i] for i in filtered_idx]
        else:
            filtered_molecules[key]=[values[i] for i in filtered_idx]
    break


## Step 5 : Save the results as JSON
json.dump(filtered_molecules,open(f"{job_name}_{db_name}_filtered_results.json","w"),indent=3)