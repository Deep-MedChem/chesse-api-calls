# chesse-api-calls - Jobs API

This is an example of a python script that uses the CHEESE Jobs API. The example consists in searching an array of molecules in a database, performs property filtering and saves the results as JSON.  

1. Just clone the whole repo and go to `cheese-api-calls/jobs_api`
2. Modify the `array_search_job.py` python script accordingly. The most important items are 
    - `API_KEY` : Your CHEESE API key
    - `INPUT_MOLECULES` : Array of input molecules (SMILES)
    - `DB_NAME` : Database name
    - `SIM_TH` : Embedding similarity threshold
    - `PROP_RANGES` : Custom ranges for molecular descriptors ADMET properties. Valid property names are in `./available_properties.txt`

3. Responses are saved as JSON files. An example is in `./cheese_ZINC15_filtered_results`