# CHEESE API Calls

A collection of Python scripts demonstrating the [CHEESE Search API](https://api.cheese-dev.deepmedchem.com/docs) for molecular similarity search across chemical databases.

## Scripts

### `cheese_api_to_csv.py`

Batch molecular similarity search using the `/molsearch` and `/batch_search` endpoints. Reads an input CSV of SMILES, queries CHEESE for nearest neighbors, and writes results to an output CSV. Supports resume, retries, configurable search types (`morgan`, `espsim_shape`, `espsim_electrostatic`), and search quality levels. Limited to 100 neighbors per query — for larger searches, use `jobs_api/molsearch_jobs_to_csv.py`.

### `synthongpt_api_to_csv.py`

SynthonGPT job-based search using the `/submit_synthongpt_job` endpoint. Submits asynchronous synthon search jobs for each query molecule, polls for completion, then downloads paginated results. Designed for searching synthon databases (e.g. `CHEMSPACE-FREEDOM-SYNTHON`).

### `jobs_api_example.py`

A simple end-to-end example of the CHEESE Jobs API workflow:
1. Submit a search job via `/submit_molsearch`
2. Poll `/job_status` until completion
3. Fetch paginated results via `/get_molsearch_page` with similarity and ADMET property filtering

### `jobs_api/`

Jobs API examples for large-scale searches (up to 100K neighbors per query). Includes array search with JSON output and CSV-based batch search with resume support. See [`jobs_api/README.md`](jobs_api/README.md) for details.

## Authentication

All endpoints require an API key passed via the `X-API-Key` header. Set it via `--api-key` or the `CHEESE_API_KEY` environment variable.

## Available Properties

The file `available_properties.txt` lists all ADMET properties and molecular descriptors available for filtering (e.g. `lipophilicity_astrazeneca`, `herg`, `molecular_weight`, `clogp`).
