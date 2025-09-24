#!/usr/bin/env python3
import os, time, csv, json, argparse, requests, threading
from typing import Tuple, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = "https://api.mercadopago.com/v1/payments"
_write_lock = threading.Lock()
_session_local = threading.local()

def _build_session(token: str, proxies: Optional[dict], timeout: float, pool_size: int) -> requests.Session:
    s = requests.Session()
    # Connection pool tuned for concurrency
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=Retry(total=0, backoff_factor=0)  # we handle retries ourselves
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "mp-orders-fetch/parallel-1.0"
    })
    if proxies:
        s.proxies.update(proxies)
    # store default timeout on the session object
    s.request_timeout = timeout  # custom attr
    return s

def get_session(token: str, proxies: Optional[dict], timeout: float, pool_size: int) -> requests.Session:
    # One Session per thread for connection reuse
    sess = getattr(_session_local, "sess", None)
    if sess is None:
        sess = _build_session(token, proxies, timeout, pool_size)
        _session_local.sess = sess
    return sess

def fetch_payment(payment_id: str, token: str, proxies: Optional[dict],
                  timeout: float = 15.0, max_retries: int = 3, backoff: float = 1.2,
                  pool_size: int = 10) -> Dict[str, Any]:
    """
    Returns a row dict for CSV with keys:
    payment_id, order_id, external_reference, http_status, error
    """
    sess = get_session(token, proxies, timeout, pool_size)
    url = f"{API_BASE}/{payment_id}"

    for attempt in range(1, max_retries + 1):
        try:
            resp = sess.get(url, timeout=getattr(sess, "request_timeout", timeout))
            status = resp.status_code
            ct = resp.headers.get("Content-Type", "")

            data = None
            err = None
            if ct.startswith("application/json"):
                try:
                    data = resp.json()
                except json.JSONDecodeError:
                    err = "Invalid JSON in response"
            else:
                if not (200 <= status < 300):
                    err = (resp.text or "")[:500]

            # Handle rate limits / transient errors with backoff
            if status in (429, 500, 502, 503, 504) and attempt < max_retries:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if (retry_after and retry_after.isdigit()) else backoff ** attempt
                time.sleep(sleep_s)
                continue

            order_id = ""
            external_reference = ""
            if 200 <= status < 300 and isinstance(data, dict):
                order = data.get("order")
                if isinstance(order, dict):
                    order_id = str(order.get("id") or "")
                external_reference = str(data.get("external_reference") or "")
                return {
                    "payment_id": payment_id,
                    "order_id": order_id,
                    "external_reference": external_reference,
                    "http_status": status,
                    "error": ""
                }
            else:
                # surface API error fields if present
                if isinstance(data, dict):
                    api_msg = data.get("message") or data.get("error") or data.get("cause")
                    if api_msg:
                        err = str(api_msg)
                return {
                    "payment_id": payment_id,
                    "order_id": "",
                    "external_reference": "",
                    "http_status": status,
                    "error": err or f"HTTP {status}"
                }
        except requests.RequestException as e:
            if attempt < max_retries:
                time.sleep(backoff ** attempt)
                continue
            return {
                "payment_id": payment_id,
                "order_id": "",
                "external_reference": "",
                "http_status": 0,
                "error": f"Request failed: {e}"
            }

    # Fallback (shouldn’t happen)
    return {
        "payment_id": payment_id,
        "order_id": "",
        "external_reference": "",
        "http_status": 0,
        "error": "Exhausted retries"
    }

def parse_ids(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                yield s

def main():
    parser = argparse.ArgumentParser(description="Fetch Mercado Pago order/external_reference by payment id (parallel).")
    parser.add_argument("--infile", required=True, help="TXT with one payment ID per line.")
    parser.add_argument("--outfile", default="mp_orders.csv", help="Output CSV (default: mp_orders.csv).")
    parser.add_argument("--token", default=os.getenv("MP_TOKEN"), help="Mercado Pago token or env MP_TOKEN.")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout (s).")
    parser.add_argument("--retries", type=int, default=3, help="Max retries for 429/5xx.")
    parser.add_argument("--proxy", default=os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY"),
                        help="Proxy URL (e.g. http://user:pass@host:port).")
    parser.add_argument("--workers", type=int, default=5, help="Number of concurrent threads (default: 5).")
    parser.add_argument("--pool-size", type=int, default=20, help="HTTP connection pool size (default: 20).")
    args = parser.parse_args()

    token = (args.token or "").strip()
    if not token:
        raise SystemExit("Error: provide a token with --token or set MP_TOKEN.")

    proxies = None
    if args.proxy:
        p = args.proxy.strip()
        proxies = {"http": p, "https": p}

    payment_ids = list(parse_ids(args.infile))
    total = len(payment_ids)
    if total == 0:
        print("No payment IDs found.")
        return

    fieldnames = ["payment_id", "order_id", "external_reference", "http_status", "error"]
    processed = 0

    # Write header immediately; then append rows as they complete
    with open(args.outfile, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        csvfile.flush()

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(
                    fetch_payment,
                    pid,
                    token,
                    proxies,
                    args.timeout,
                    args.retries,
                    1.2,
                    args.pool_size
                ): pid for pid in payment_ids
            }

            for future in as_completed(futures):
                row = future.result()
                with _write_lock:
                    writer.writerow(row)
                    csvfile.flush()
                    processed += 1
                    # lightweight progress line
                    if processed % 10 == 0 or processed == total:
                        print(f"[{processed}/{total}] wrote rows to {args.outfile}")

    print(f"Done. Processed {processed} payment IDs. Output: {args.outfile}")

if __name__ == "__main__":
    main()
