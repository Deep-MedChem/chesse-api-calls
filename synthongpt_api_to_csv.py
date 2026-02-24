# %%
#!/usr/bin/env python3
import os
import time
import argparse
import csv
import requests
from typing import Dict, Any, List, Tuple, Optional, Iterable


def submit_synthongpt_job(
    api_url: str,
    headers: Dict[str, str],
    smiles: str,
    db_name: str,
    search_quality: str,
    include_properties: bool = False,
    include_metadata: bool = False,
    timeout: int = 60,
) -> str:
    """
    Submit SynthonGPT job.
    Uses query params + empty JSON body {}.
    Returns job_id/job_name (string).
    """
    url = f"{api_url.rstrip('/')}/submit_synthongpt_job"
    params = {
        "search_input": smiles,
        "db_name": db_name,
        "include_properties": str(include_properties).lower(),
        "include_metadata": str(include_metadata).lower(),
        "search_quality": search_quality,
    }
    r = requests.post(url, params=params, json={}, headers=headers, timeout=timeout)
    _raise_for_status_with_hint(r, where="submit_synthongpt_job")

    # Robust parsing of job id/name
    try:
        js = r.json()
    except Exception:
        js = r.text.strip()

    if isinstance(js, str) and js:
        return js

    if isinstance(js, dict):
        for key in ("job_name", "job_id", "id", "job", "result"):
            v = js.get(key)
            if isinstance(v, str) and v:
                return v
        for v in js.values():
            if isinstance(v, str) and v:
                return v

    raise RuntimeError(f"Unexpected submit response shape: {js!r}")


def get_molsearch_page(
    api_url: str,
    headers: Dict[str, str],
    job_name: str,
    page_num: int,
    page_size: int,
    db_name: str,
    db_name_as_list: bool = True,
    timeout: int = 60,
) -> Dict[str, Any]:
    """
    Fetch one results page via POST /get_molsearch_page.
    Some deployments want db_name as list, others as scalar. Controlled via db_name_as_list.
    """
    url = f"{api_url.rstrip('/')}/get_molsearch_page"
    params: Dict[str, Any] = {
        "job_name": job_name,
        "page_num": page_num,
        "page_size": page_size,
        "db_name": [db_name] if db_name_as_list else db_name,
    }
    r = requests.post(url, params=params, json={}, headers=headers, timeout=timeout)
    _raise_for_status_with_hint(r, where="get_molsearch_page")
    return r.json()


def wait_until_results_available(
    api_url: str,
    headers: Dict[str, str],
    job_name: str,
    db_name: str,
    page_size: int,
    poll_sec: float,
    max_wait_sec: int,
    db_name_as_list: bool = True,
    timeout: int = 60,
) -> None:
    """
    Reliable waiting: probe page 0 until it returns a non-empty list.
    Does NOT rely on /job_status.
    """
    start = time.time()
    attempt = 0

    while True:
        attempt += 1
        try:
            page0 = get_molsearch_page(
                api_url=api_url,
                headers=headers,
                job_name=job_name,
                page_num=0,
                page_size=page_size,
                db_name=db_name,
                db_name_as_list=db_name_as_list,
                timeout=timeout,
            )
            ids = page0.get("id", []) or []
            if isinstance(ids, list) and len(ids) > 0:
                return
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            # auth/validation issues should fail fast
            if code in (401, 403, 422):
                raise
            # otherwise treat as "still processing"
        except Exception:
            pass

        if time.time() - start > max_wait_sec:
            raise TimeoutError(f"Timeout waiting for results for job={job_name} after {max_wait_sec}s")

        # gentle backoff (max 10s)
        sleep_t = min(10.0, poll_sec * (1.0 + 0.15 * min(attempt, 20)))
        time.sleep(sleep_t)


def iter_results_paged(
    api_url: str,
    headers: Dict[str, str],
    job_name: str,
    db_name: str,
    total_needed: int,
    page_size: int,
    db_name_as_list: bool = True,
    timeout: int = 60,
) -> List[Tuple[str, str, Optional[float]]]:
    """
    Returns list of (hit_smiles, hit_id, similarity).
    Downloads up to total_needed hits.
    """
    out: List[Tuple[str, str, Optional[float]]] = []
    page_num = 0

    while len(out) < total_needed:
        ## Progress log
        if page_num == 0:
            print(f"Fetching results page {page_num} (up to {total_needed} total hits)...")
        elif page_num % 10 == 0:
            print(f"Fetching results page {page_num} (collected {len(out)}/{total_needed} hits so far)...")
        page = get_molsearch_page(
            api_url=api_url,
            headers=headers,
            job_name=job_name,
            page_num=page_num,
            page_size=page_size,
            db_name=db_name,
            db_name_as_list=db_name_as_list,
            timeout=timeout,
        )
        smiles_list = page.get("smiles", []) or []
        id_list = page.get("id", []) or []
        sim_list = page.get("similarity", []) or []

        n = min(len(smiles_list), len(id_list), len(sim_list))
        if n == 0:
            break

        for i in range(n):
            rid = id_list[i]
            if rid == "Query Molecule":
                continue

            hit_smiles = smiles_list[i]
            hit_id = str(rid).strip().replace("-DMCH", "")

            try:
                sim: Optional[float] = float(sim_list[i])
            except Exception:
                sim = None

            out.append((hit_smiles, hit_id, sim))
            if len(out) >= total_needed:
                break

        if n < page_size:
            break

        page_num += 1

    return out


def write_csv_header_if_needed(writer: csv.writer, wrote_header: bool) -> bool:
    if not wrote_header:
        writer.writerow(["query_id", "query_smiles", "hit_smiles", "hit_id", "similarity"])
        return True
    return wrote_header


def process_one_query(
    api_url: str,
    headers: Dict[str, str],
    query_id: str,
    smiles: str,
    db_name: str,
    n: int,
    page_size: int,
    poll_sec: float,
    max_wait_sec: int,
    include_properties: bool,
    include_metadata: bool,
    db_name_as_list: bool,
    timeout: int,
) -> List[Tuple[str, str, Optional[float]]]:
    job_name = submit_synthongpt_job(
        api_url=api_url,
        headers=headers,
        smiles=smiles,
        db_name=db_name,
        search_quality=str(n),
        include_properties=include_properties,
        include_metadata=include_metadata,
        timeout=timeout,
    )
    wait_until_results_available(
        api_url=api_url,
        headers=headers,
        job_name=job_name,
        db_name=db_name,
        page_size=page_size,
        poll_sec=poll_sec,
        max_wait_sec=max_wait_sec,
        db_name_as_list=db_name_as_list,
        timeout=timeout,
    )

    rows = iter_results_paged(
        api_url=api_url,
        headers=headers,
        job_name=job_name,
        db_name=db_name,
        total_needed=n,
        page_size=page_size,
        db_name_as_list=db_name_as_list,
        timeout=timeout,
    )
    return rows


def iter_input_csv(path: str, smiles_col: str, id_col: Optional[str]) -> Iterable[Tuple[str, str]]:
    """
    Yields (query_id, smiles) from input CSV.
    - smiles_col: name of column containing SMILES
    - id_col: optional column name for ID; if None, uses row index starting at 1
    """
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("Input CSV has no header row.")

        if smiles_col not in reader.fieldnames:
            raise ValueError(f"SMILES column '{smiles_col}' not found. Available: {reader.fieldnames}")

        if id_col is not None and id_col not in reader.fieldnames:
            raise ValueError(f"ID column '{id_col}' not found. Available: {reader.fieldnames}")

        for idx, row in enumerate(reader, start=1):
            smiles = (row.get(smiles_col) or "").strip()
            if not smiles:
                continue
            qid = (row.get(id_col) or "").strip() if id_col else str(idx)
            if not qid:
                qid = str(idx)
            yield qid, smiles


def load_processed_query_ids(out_path: str) -> set:
    """
    For --resume: reads existing output CSV and returns set of query_id already present.
    """
    processed = set()
    if not os.path.exists(out_path):
        return processed

    with open(out_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return processed
        if "query_id" not in reader.fieldnames:
            return processed
        for row in reader:
            qid = (row.get("query_id") or "").strip()
            if qid:
                processed.add(qid)
    return processed


def _raise_for_status_with_hint(r: requests.Response, where: str) -> None:
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        code = r.status_code
        body = (r.text or "")[:600]
        hint = ""
        if code in (401, 403):
            hint = " (auth error: check X-API-Key and host)"
        elif code == 422:
            hint = " (validation error: check param names/types; try toggling --db-name-as-list)"
        raise requests.HTTPError(f"{where} HTTP {code}{hint}: {body}", response=r) from e


def main():
    p = argparse.ArgumentParser(description="CHEESE SynthonGPT: CSV (SMILES per row) -> N nearest -> output CSV")
    p.add_argument("--api-url", default=os.getenv("CHEESE_API_URL", "https://api.cheese.deepmedchem.com"))
    p.add_argument("--api-key", default=os.getenv("CHEESE_API_KEY", ""), help="X-API-Key (or set CHEESE_API_KEY)")
    p.add_argument("--db-name", default="CHEMSPACE-FREEDOM-142B", help="DB for SynthonGPT")

    p.add_argument("--input-csv", required=True, help="Input CSV with at least a SMILES column")
    p.add_argument("--smiles-col", default="smiles", help="Column name in input CSV with query SMILES")
    p.add_argument("--id-col", default=None, help="Optional column name in input CSV for query ID")

    p.add_argument("--n", type=int, default=10000, help="How many nearest molecules to download per query")
    p.add_argument("--page-size", type=int, default=100, help="Paging size")
    p.add_argument("--poll-sec", type=float, default=1.5, help="Polling interval base")
    p.add_argument("--max-wait-sec", type=int, default=600, help="Max wait for results to appear per query")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout per request (sec)")

    p.add_argument("--out", default="synthongpt_results.csv", help="Output CSV path (all queries appended)")
    p.add_argument("--resume", action="store_true", help="Skip query_id already present in output")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing output file")

    p.add_argument("--include-properties", action="store_true")
    p.add_argument("--include-metadata", action="store_true")

    p.add_argument(
        "--db-name-as-list",
        action="store_true",
        help="Send db_name as list on /get_molsearch_page. If you get 422, try toggling this on/off.",
    )
    args = p.parse_args()

    if not args.api_key:
        raise SystemExit("Missing --api-key (or set CHEESE_API_KEY env var).")

    headers = {"X-API-Key": args.api_key, "accept": "application/json"}

    if args.overwrite and os.path.exists(args.out):
        os.remove(args.out)

    processed = load_processed_query_ids(args.out) if args.resume else set()

    wrote_header = False
    file_exists = os.path.exists(args.out)
    if file_exists and not args.overwrite:
        wrote_header = True

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "a", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        wrote_header = write_csv_header_if_needed(writer, wrote_header)

        for qid, smiles in iter_input_csv(args.input_csv, args.smiles_col, args.id_col):
            if args.resume and qid in processed:
                print(f"[SKIP] query_id={qid} already in output")
                continue

            print(f"[RUN] query_id={qid} | smiles={smiles}")

            try:
                hits = process_one_query(
                    api_url=args.api_url,
                    headers=headers,
                    query_id=qid,
                    smiles=smiles,
                    db_name=args.db_name,
                    n=args.n,
                    page_size=args.page_size,
                    poll_sec=args.poll_sec,
                    max_wait_sec=args.max_wait_sec,
                    include_properties=args.include_properties,
                    include_metadata=args.include_metadata,
                    db_name_as_list=args.db_name_as_list,
                    timeout=args.timeout,
                )
            except Exception as e:
                print(f"[ERROR] query_id={qid} failed: {e}")
                continue

            for hit_smiles, hit_id, sim in hits:
                writer.writerow([qid, smiles, hit_smiles, hit_id, sim])

            f_out.flush()
            processed.add(qid)
            print(f"[OK] query_id={qid} hits={len(hits)} -> appended")

    print(f"[DONE] Output written to: {args.out}")


if __name__ == "__main__":
    main()
