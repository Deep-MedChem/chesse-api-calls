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


def parse_hits(payload: Any) -> List[Tuple[str, str, Optional[float]]]:
    """
    Supports typical CHEESE formats:
      - {"neighbors":[{...}, ...]}
      - {"smiles":[...], "id":[...]} (no score)
    Returns: (hit_smiles, hit_id, score)
    """

    def pick_score(n: Dict[str, Any]) -> Optional[float]:
        # Try many common keys. CHEESE can return either similarity or distance-like values.
        candidates = [
            # similarity-like (higher = more similar)
            "similarity", "sim", "score",

            # fingerprint / classic
            "tanimoto", "morgan_tanimoto",

            # shape / esp similarity
            "shape_similarity", "shape_sim", "shape_tanimoto",
            "espsim", "espsim_shape", "espsim_similarity", "espsim_sim",
            "electrostatic_similarity", "esp_similarity", "esp_sim",

            # embedding-based similarity
            "cosine_similarity", "cos_sim", "cosine_sim",

            # distance-like (lower = more similar, used only if nothing else exists)
            "embedding_distance", "distance", "dist",
            "espsim_distance", "shape_distance", "esp_distance",
        ]

        val = None
        for k in candidates:
            if k in n and n.get(k) is not None:
                val = n.get(k)
                break

        # Sometimes score is nested
        if val is None:
            for k in ("metrics", "meta", "metadata"):
                if isinstance(n.get(k), dict):
                    d = n[k]
                    for kk in candidates:
                        if kk in d and d.get(kk) is not None:
                            val = d.get(kk)
                            break
                if val is not None:
                    break

        if val is None:
            return None

        try:
            return float(val)
        except Exception:
            return None

    if isinstance(payload, dict) and isinstance(payload.get("neighbors"), list):
        out = []
        for n in payload["neighbors"]:
            if not isinstance(n, dict):
                continue

            hit_smiles = (n.get("smiles") or "").strip()
            hit_id = str(n.get("zinc_id") or n.get("id") or n.get("name") or n.get("identifier") or "").strip()
            hit_id = hit_id.replace("-DMCH", "")

            score = pick_score(n)

            if hit_smiles and hit_id and hit_id != "Query Molecule":
                out.append((hit_smiles, hit_id, score))
        return out

    if isinstance(payload, dict) and isinstance(payload.get("smiles"), list) and isinstance(payload.get("id"), list):
        out = []
        n = min(len(payload["smiles"]), len(payload["id"]))
        for i in range(n):
            smi = str(payload["smiles"][i]).strip()
            rid = str(payload["id"][i]).strip().replace("-DMCH", "")
            if smi and rid and rid != "Query Molecule":
                out.append((smi, rid, None))
        return out

    return []

def call_batch_search(
    api_url: str,
    headers: Dict[str, str],
    smiles_list: List[str],
    db_name: str,
    n_neighbors: int,
    search_type: str,
    search_quality: str,
    timeout: int,
) -> Any:
    """
    GET /batch_search
    Trik: některé implementace berou list parametrů jako opakované query paramy.
    requests to umí přes list values.
    """
    url = f"{api_url.rstrip('/')}/batch_search"
    params = {
        "search_input": smiles_list,          # repeated query param
        "search_type": search_type,
        "search_quality": search_quality,
        "db_names": db_name,
        "n_neighbors": int(n_neighbors),
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    _raise_for_status_with_hint(r, where="batch_search")
    return r.json()


def call_molsearch(
    api_url: str,
    headers: Dict[str, str],
    smiles: str,
    db_name: str,
    n_neighbors: int,
    search_type: str,
    search_quality: str,
    timeout: int,
) -> Any:
    url = f"{api_url.rstrip('/')}/molsearch"
    params = {
        "search_input": smiles,
        "search_type": search_type,
        "search_quality": search_quality,
        "db_names": [db_name],
        "n_neighbors": int(n_neighbors),
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    _raise_for_status_with_hint(r, where="molsearch")
    return r.json()


def main():
    p = argparse.ArgumentParser(description="CHEESE batch_search/molsearch: CSV -> N nearest -> output CSV")

    p.add_argument("--api-url", default=os.getenv("CHEESE_API_URL", "https://api.cheese.deepmedchem.com"))
    p.add_argument("--api-key", default=os.getenv("CHEESE_API_KEY", ""), help="X-API-Key (or set CHEESE_API_KEY)")

    p.add_argument("--db-name", default="CHEMSPACE-10B-RO5")
    p.add_argument("--input-csv", required=True)
    p.add_argument("--smiles-col", default="smiles")
    p.add_argument("--id-col", default=None)

    p.add_argument("--n", type=int, default=10000)
    p.add_argument("--search-type", default="espsim_shape")
    p.add_argument("--search-quality", default="fast")
    p.add_argument("--timeout", type=int, default=180)

    p.add_argument("--out", default="molsearch_results.csv")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--overwrite", action="store_true")

    p.add_argument("--sleep-between", type=float, default=0.2)
    p.add_argument("--retries", type=int, default=2)

    # batch knobs
    p.add_argument("--batch-size", type=int, default=1, help="How many queries per /batch_search call (start with 1)")
    p.add_argument("--no-batch", action="store_true", help="Force per-query /molsearch instead of /batch_search")

    args = p.parse_args()

    if not args.api_key:
        raise SystemExit("Missing --api-key (or set CHEESE_API_KEY env var).")

    headers = {"X-API-Key": args.api_key, "accept": "application/json"}

    if args.overwrite and os.path.exists(args.out):
        os.remove(args.out)

    processed = load_processed_query_ids(args.out) if args.resume else set()
    wrote_header = os.path.exists(args.out) and not args.overwrite

    items = [(qid, smi) for (qid, smi) in iter_input_csv(args.input_csv, args.smiles_col, args.id_col)]
    if args.resume:
        items = [(qid, smi) for (qid, smi) in items if qid not in processed]

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "a", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        wrote_header = write_csv_header_if_needed(writer, wrote_header)

        i = 0
        while i < len(items):
            batch = items[i : i + max(1, args.batch_size)]
            i += len(batch)

            # per-item log
            for qid, smi in batch:
                print(f"[RUN] query_id={qid} | smiles={smi}")

            last_err: Optional[Exception] = None
            for attempt in range(args.retries + 1):
                try:
                    if not args.no_batch and len(batch) > 0:
                        # call /batch_search
                        smiles_list = [s for _, s in batch]
                        payload = call_batch_search(
                            api_url=args.api_url,
                            headers=headers,
                            smiles_list=smiles_list,
                            db_name=args.db_name,
                            n_neighbors=args.n,
                            search_type=args.search_type,
                            search_quality=args.search_quality,
                            timeout=args.timeout,
                        )

                        # Expected: list of results aligned with smiles_list OR dict with results
                        # We handle both:
                        if isinstance(payload, list) and len(payload) == len(batch):
                            for (qid, qsmiles), one in zip(batch, payload):
                                hits = parse_hits(one)
                                for hit_smiles, hit_id, sim in hits[: args.n]:
                                    writer.writerow([qid, qsmiles, hit_smiles, hit_id, sim])
                                processed.add(qid)
                                print(f"[OK] query_id={qid} hits={len(hits)} -> appended")
                        else:
                            # Fallback: treat as single payload applied to first query
                            qid, qsmiles = batch[0]
                            hits = parse_hits(payload)
                            for hit_smiles, hit_id, sim in hits[: args.n]:
                                writer.writerow([qid, qsmiles, hit_smiles, hit_id, sim])
                            processed.add(qid)
                            print(f"[OK] query_id={qid} hits={len(hits)} -> appended")

                    else:
                        # per query /molsearch
                        for qid, qsmiles in batch:
                            payload = call_molsearch(
                                api_url=args.api_url,
                                headers=headers,
                                smiles=qsmiles,
                                db_name=args.db_name,
                                n_neighbors=args.n,
                                search_type=args.search_type,
                                search_quality=args.search_quality,
                                timeout=args.timeout,
                            )
                            hits = parse_hits(payload)
                            for hit_smiles, hit_id, sim in hits[: args.n]:
                                writer.writerow([qid, qsmiles, hit_smiles, hit_id, sim])
                            processed.add(qid)
                            print(f"[OK] query_id={qid} hits={len(hits)} -> appended")

                    f_out.flush()
                    last_err = None
                    break

                except Exception as e:
                    last_err = e
                    sleep_t = 1.0 + 1.5 * attempt
                    print(f"[WARN] batch attempt={attempt+1} failed ({type(e).__name__}): {e}; sleeping {sleep_t:.1f}s")
                    time.sleep(sleep_t)

            if last_err is not None:
                print(f"[ERROR] batch failed after retries: {last_err}")

            if args.sleep_between > 0:
                time.sleep(args.sleep_between)

    print(f"[DONE] Output written to: {args.out}")


if __name__ == "__main__":
    main()