##### This is an example of job API search for an array of molecules #####

import requests
import os
from typing import List,Optional,Dict,Any
import time
import json

CHEESE_URL="https://api.cheese.deepmedchem.com"


def submit_molsearch(
    search_input: str,
    search_type: str = "espsim_shape", # espsim_shape, espsim_electrostatic, morgan
    search_quality: str = "fast", # fast, accurate, very_accurate
    db_names: List[str] = ["ZINC15"],
    API_KEY:str="XXXXX",
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


def get_job_status(job_name:str,
                   API_KEY:str):
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
    prop_ranges: Optional[Dict[str, Dict[str, float]]] = None,
    API_KEY:str="XXXX"
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
