#!/usr/bin/env python3
import os
import time
import argparse
import csv
import requests
from typing import Dict, Any, List, Tuple, Optional, Iterable


def _raise_for_status_with_hint(r: requests.Response, where: str) -> None:
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        code = r.status_code
        body = (r.text or "")[:800]
        hint = ""
        if code in (401, 403):
            hint = " (auth error: check X-API-Key and host)"
        elif code == 422:
            hint = " (validation error: check param names/types)"
        elif code == 429:
            hint = " (rate limit: slow down / backoff)"
        raise requests.HTTPError(f"{where} HTTP {code}{hint}: {body}", response=r) from e


def iter_input_csv(path: str, smiles_col: str, id_col: Optional[str]) -> Iterable[Tuple[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("Input CSV has no header row.")
        if smiles_col not in reader.fieldnames:
            raise ValueError(f"SMILES column '{smiles_col}' not found. Available: {reader.fieldnames}")
        if id_col is not None and id_col not in reader.fieldnames:
            raise ValueError(f"ID column '{id_col}' not found. Available: {reader.fieldnames}")

        for idx, row in enumerate(reader, start=1):
            smi = (row.get(smiles_col) or "").strip()
            if not smi:
                continue
            qid = (row.get(id_col) or "").strip() if id_col else str(idx)
            if not qid:
                qid = str(idx)
            yield qid, smi


def load_processed_query_ids(out_path: str) -> set:
    processed = set()
    if not os.path.exists(out_path):
        return processed
    with open(out_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "query_id" not in reader.fieldnames:
            return processed
        for row in reader:
            qid = (row.get("query_id") or "").strip()
            if qid:
                processed.add(qid)
    return processed


def write_csv_header_if_needed(writer: csv.writer, wrote_header: bool) -> bool:
    if not wrote_header:
        writer.writerow(["query_id", "query_smiles", "hit_smiles", "hit_id", "similarity"])
        return True
    return wrote_header


def submit_molsearch_job(
    api_url: str,
    headers: Dict[str, str],
    smiles: str,
    db_names: List[str],
    n_neighbors: int,
    search_type: str,
    search_quality: str,
    include_properties: bool,
    timeout: int,
) -> str:
    url = f"{api_url.rstrip('/')}/submit_molsearch"
    params = {
        "search_input": smiles,
        "search_type": search_type,
        "search_quality": search_quality,
        "db_names": db_names,
        "n_neighbors": int(n_neighbors),
        "include_properties": str(include_properties).lower(),
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    _raise_for_status_with_hint(r, where="submit_molsearch")

    js = r.json()
    if isinstance(js, str) and js:
        return js
    if isinstance(js, dict):
        for key in ("job_name", "job_id", "id"):
            v = js.get(key)
            if isinstance(v, str) and v:
                return v
    raise RuntimeError(f"Unexpected submit_molsearch response: {js!r}")


def get_job_status(
    api_url: str,
    headers: Dict[str, str],
    job_name: str,
    timeout: int,
) -> str:
    url = f"{api_url.rstrip('/')}/job_status"
    r = requests.get(url, params={"job_name": job_name}, headers=headers, timeout=timeout)
    _raise_for_status_with_hint(r, where="job_status")
    return r.json()


def get_molsearch_page(
    api_url: str,
    headers: Dict[str, str],
    job_name: str,
    db_name: str,
    page_num: int,
    page_size: int,
    sim_th: float,
    timeout: int,
) -> Dict[str, Any]:
    url = f"{api_url.rstrip('/')}/get_molsearch_page"
    params = {
        "job_name": job_name,
        "db_name": db_name,
        "page_size": page_size,
        "page_num": page_num,
        "include_properties": "false",
        "sim_th": sim_th,
    }
    r = requests.post(url, params=params, json={"prop_ranges": {}},
                      headers={**headers, "Content-Type": "application/json"}, timeout=timeout)
    _raise_for_status_with_hint(r, where="get_molsearch_page")
    return r.json()


def wait_for_job(
    api_url: str,
    headers: Dict[str, str],
    job_name: str,
    poll_sec: float,
    max_wait_sec: int,
    timeout: int,
) -> str:
    start = time.time()
    while True:
        status = get_job_status(api_url, headers, job_name, timeout)
        if status == "SUCCESS":
            return status
        if status in ("FAILURE", "REVOKED"):
            raise RuntimeError(f"Job {job_name} ended with status: {status}")
        if time.time() - start > max_wait_sec:
            raise TimeoutError(f"Timeout waiting for job {job_name} after {max_wait_sec}s")
        time.sleep(poll_sec)


def fetch_all_hits(
    api_url: str,
    headers: Dict[str, str],
    job_name: str,
    db_name: str,
    n_needed: int,
    page_size: int,
    sim_th: float,
    timeout: int,
) -> List[Tuple[str, str, Optional[float]]]:
    out: List[Tuple[str, str, Optional[float]]] = []
    page_num = 0

    while len(out) < n_needed:
        page = get_molsearch_page(api_url, headers, job_name, db_name, page_num, page_size, sim_th, timeout)
        smiles_list = page.get("smiles", []) or []
        id_list = page.get("id", []) or []
        sim_list = page.get("similarity", []) or []

        n = min(len(smiles_list), len(id_list), len(sim_list))
        if n == 0:
            break

        for i in range(n):
            if id_list[i] == "Query Molecule":
                continue
            hit_id = str(id_list[i]).strip().replace("-DMCH", "")
            try:
                sim = float(sim_list[i])
            except Exception:
                sim = None
            out.append((smiles_list[i], hit_id, sim))
            if len(out) >= n_needed:
                break

        if n < page_size:
            break
        page_num += 1

    return out


def main():
    p = argparse.ArgumentParser(
        description="CHEESE Jobs API: CSV -> /submit_molsearch -> paginated results -> output CSV. "
                    "Supports up to 100K neighbors per query."
    )

    p.add_argument("--api-url", default=os.getenv("CHEESE_API_URL", "https://api.cheese.deepmedchem.com"))
    p.add_argument("--api-key", default=os.getenv("CHEESE_API_KEY", ""), help="X-API-Key (or set CHEESE_API_KEY)")

    p.add_argument("--db-name", default="ZINC15")
    p.add_argument("--input-csv", required=True)
    p.add_argument("--smiles-col", default="smiles")
    p.add_argument("--id-col", default=None)

    p.add_argument("--n", type=int, default=500, help="Neighbors per query (up to 100000)")
    p.add_argument("--search-type", default="espsim_shape")
    p.add_argument("--search-quality", default="fast")
    p.add_argument("--sim-th", type=float, default=0.0, help="Minimum similarity threshold")
    p.add_argument("--page-size", type=int, default=100)
    p.add_argument("--timeout", type=int, default=120, help="HTTP timeout per request (sec)")

    p.add_argument("--poll-sec", type=float, default=2.0, help="Polling interval for job status")
    p.add_argument("--max-wait-sec", type=int, default=600, help="Max wait per job")

    p.add_argument("--out", default="molsearch_jobs_results.csv")
    p.add_argument("--resume", action="store_true", help="Skip query_id already present in output")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing output file")

    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--sleep-between", type=float, default=0.5, help="Sleep between queries")

    args = p.parse_args()

    if not args.api_key:
        raise SystemExit("Missing --api-key (or set CHEESE_API_KEY env var).")

    headers = {"X-API-Key": args.api_key, "accept": "application/json"}

    if args.overwrite and os.path.exists(args.out):
        os.remove(args.out)

    processed = load_processed_query_ids(args.out) if args.resume else set()
    wrote_header = os.path.exists(args.out) and not args.overwrite

    items = list(iter_input_csv(args.input_csv, args.smiles_col, args.id_col))
    if args.resume:
        items = [(qid, smi) for (qid, smi) in items if qid not in processed]

    print(f"[INFO] {len(items)} queries to process, db={args.db_name}, n={args.n}")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "a", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        wrote_header = write_csv_header_if_needed(writer, wrote_header)

        for qid, qsmiles in items:
            print(f"[RUN] query_id={qid} | smiles={qsmiles}")

            last_err: Optional[Exception] = None
            for attempt in range(args.retries + 1):
                try:
                    job_name = submit_molsearch_job(
                        api_url=args.api_url,
                        headers=headers,
                        smiles=qsmiles,
                        db_names=[args.db_name],
                        n_neighbors=args.n,
                        search_type=args.search_type,
                        search_quality=args.search_quality,
                        include_properties=False,
                        timeout=args.timeout,
                    )
                    print(f"  Job: {job_name}")

                    wait_for_job(
                        api_url=args.api_url,
                        headers=headers,
                        job_name=job_name,
                        poll_sec=args.poll_sec,
                        max_wait_sec=args.max_wait_sec,
                        timeout=args.timeout,
                    )

                    hits = fetch_all_hits(
                        api_url=args.api_url,
                        headers=headers,
                        job_name=job_name,
                        db_name=args.db_name,
                        n_needed=args.n,
                        page_size=args.page_size,
                        sim_th=args.sim_th,
                        timeout=args.timeout,
                    )

                    for hit_smiles, hit_id, sim in hits:
                        writer.writerow([qid, qsmiles, hit_smiles, hit_id, sim])

                    f_out.flush()
                    processed.add(qid)
                    print(f"[OK] query_id={qid} hits={len(hits)}")
                    last_err = None
                    break

                except Exception as e:
                    last_err = e
                    sleep_t = 2.0 + 2.0 * attempt
                    print(f"[WARN] attempt={attempt+1} failed ({type(e).__name__}): {e}; sleeping {sleep_t:.1f}s")
                    time.sleep(sleep_t)

            if last_err is not None:
                print(f"[ERROR] query_id={qid} failed after retries: {last_err}")

            if args.sleep_between > 0:
                time.sleep(args.sleep_between)

    print(f"[DONE] Output written to: {args.out}")


if __name__ == "__main__":
    main()
