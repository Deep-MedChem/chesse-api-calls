# CHEESE API Calls - Jobs API

Large-scale similarity search examples using the CHEESE Jobs API (`/submit_molsearch` + `/job_status` + `/get_molsearch_page`). Supports up to 100,000 neighbors per query.

## Scripts

### `array_search_job.py` — JSON output

Searches an array of molecules with similarity threshold and ADMET property filtering. Results are saved as JSON.

1. Modify `array_search_job.py` with your parameters:
    - `API_KEY` : Your CHEESE API key
    - `INPUT_MOLECULES` : Array of input molecules (SMILES)
    - `DB_NAME` : Database name (e.g. `ZINC15`, `ENAMINE-REAL`)
    - `SIM_TH` : Embedding similarity threshold (0 to 1, molecules returned have similarity >= this value)
    - `PROP_RANGES` : Custom ranges for molecular descriptors and ADMET properties. Valid property names are in `./available_properties.txt`
2. Run `python array_search_job.py`

An example output is in `./cheese_ZINC15_filtered_results.json`

### `molsearch_jobs_to_csv.py` — CSV output

CSV-in/CSV-out variant with resume support. Reads query molecules from a CSV file, submits async jobs, and writes results incrementally to an output CSV. Can pick up where it left off if interrupted (`--resume`).

```
python molsearch_jobs_to_csv.py \
  --api-key YOUR_KEY \
  --input-csv queries.csv \
  --smiles-col smiles \
  --id-col id \
  --db-name ENAMINE-REAL \
  --n 500 \
  --out results.csv
```

### `utils.py`

Shared helper functions for job submission, status polling, and paginated result fetching.