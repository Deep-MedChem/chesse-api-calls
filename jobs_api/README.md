# CHEESE API Calls - Jobs API

Demonstrates searching an array of molecules using the CHEESE Jobs API. For each query molecule, the script submits an async search job via `/submit_molsearch`, polls `/job_status` until completion, then fetches paginated results via `/get_molsearch_page` with similarity threshold and ADMET property filtering. Results are saved as JSON.

## Usage

1. Clone the repo and go to `cheese-api-calls/jobs_api`
2. Modify `array_search_job.py` with your parameters:
    - `API_KEY` : Your CHEESE API key
    - `INPUT_MOLECULES` : Array of input molecules (SMILES)
    - `DB_NAME` : Database name (e.g. `ZINC15`, `ENAMINE-REAL`)
    - `SIM_TH` : Embedding similarity threshold (0 to 1, molecules returned have similarity >= this value)
    - `PROP_RANGES` : Custom ranges for molecular descriptors and ADMET properties. Valid property names are in `./available_properties.txt`
3. Run `python array_search_job.py`

## Output

Results are saved as JSON files. An example is in `./cheese_ZINC15_filtered_results.json`

## Files

- `array_search_job.py` : Main script that loops over input molecules, submits jobs, and collects filtered results
- `utils.py` : Shared helper functions for job submission, status polling, and paginated result fetching