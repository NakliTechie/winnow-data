"""Storage layer — the scraper works on local `data/`; this syncs it to R2 (S3-compatible)
so the stateless CI runner persists across nights. Local by default; set all four env vars
to enable R2:
  R2_ENDPOINT  R2_BUCKET  R2_ACCESS_KEY_ID  R2_SECRET_ACCESS_KEY
`pull_store()` (R2→local) at start, `push(paths)` (local→R2) after. boto3 imported lazily.
"""
from __future__ import annotations

import os
from typing import Iterable

DATA = os.path.join(os.path.dirname(__file__), "data")


def r2_enabled() -> bool:
    return all(os.environ.get(k) for k in
               ("R2_ENDPOINT", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"))


def _client():
    import boto3  # lazy
    return boto3.client(
        "s3", endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def _rel(path: str) -> str:
    return os.path.relpath(path, DATA).replace(os.sep, "/")


def pull_store() -> None:
    if not r2_enabled():
        print("[storage] local mode (no R2 env) — using data/ as-is")
        return
    c, bucket = _client(), os.environ["R2_BUCKET"]
    os.makedirs(DATA, exist_ok=True)
    n, tok = 0, None
    while True:
        kw = {"Bucket": bucket}
        if tok:
            kw["ContinuationToken"] = tok
        resp = c.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            dst = os.path.join(DATA, obj["Key"])
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            c.download_file(bucket, obj["Key"], dst)
            n += 1
        if not resp.get("IsTruncated"):
            break
        tok = resp.get("NextContinuationToken")
    print(f"[storage] pulled {n} objects from R2 bucket {bucket}")


def push(paths: Iterable[str]) -> None:
    if not r2_enabled():
        return
    c, bucket = _client(), os.environ["R2_BUCKET"]
    n = 0
    for p in paths:
        if os.path.exists(p):
            c.upload_file(p, bucket, _rel(p))
            n += 1
    print(f"[storage] pushed {n} objects to R2 bucket {bucket}")
