# bronze/raw_writer.py (Databricks-friendly, dbutils-safe)

from __future__ import annotations
import json, gzip, os, hashlib, tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Mapping

# --- dbutils helper ---------------------------------------------------------

def _get_dbutils():
    """
    Return a dbutils handle when running in Databricks.
    - In a notebook: uses injected `dbutils`.
    - In jobs/clusters: constructs via SparkSession.
    - Outside Databricks: returns None.
    """
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError:
        pass
    try:
        from pyspark.sql import SparkSession  # type: ignore
        from pyspark.dbutils import DBUtils   # type: ignore
        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    except Exception:
        return None

# --- DBFS helpers -----------------------------------------------------------

def _to_local_path(p: str | Path) -> Path:
    s = str(p)
    if s.startswith("dbfs:/"):
        return Path("/dbfs") / s[len("dbfs:/"):]
    return Path(s)

def _from_local_to_uri(p: str, base_dir: str) -> str:
    if str(base_dir).startswith("dbfs:/") and p.startswith("/dbfs/"):
        return "dbfs:/" + p[len("/dbfs/"):]
    return p

def _is_dbfs_path(p: Path) -> bool:
    return str(p).startswith("/dbfs/")

def _mkdirs(path: Path) -> None:
    """Create parent dirs. Use dbutils for DBFS, normal mkdir otherwise."""
    if _is_dbfs_path(path):
        dbu = _get_dbutils()
        if dbu is None:
            raise RuntimeError("DBFS path detected but dbutils is unavailable.")
        dbu.fs.mkdirs("dbfs:/" + str(path)[len("/dbfs/"):])
    else:
        path.mkdir(parents=True, exist_ok=True)

# --- small utils ------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _checksum_md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def _atomic_write_text(path: Path, text: str) -> None:
    """Sidecar writer (manifest/checksum). Use dbutils on DBFS; atomic tempfile locally."""
    if _is_dbfs_path(path):
        dbu = _get_dbutils()
        if dbu is None:
            raise RuntimeError("DBFS path detected but dbutils is unavailable.")
        _mkdirs(path.parent)
        dbu.fs.put("dbfs:/" + str(path)[len("/dbfs/"):], text, overwrite=True)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wt", delete=False, dir=path.parent, encoding="utf-8") as tmp:
        tmp.write(text)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, path)

# --- main API ---------------------------------------------------------------

def write_raw_jsonl(
    *,
    raw_text: str,
    base_dir: str,                    # e.g., "dbfs:/FileStore/bronze" or "./data/bronze"
    dataset: str,                     # e.g., "spotify_search"
    partitions: Mapping[str, str] | None = None,  # e.g., {"q":"videoclub","type":"artist"}
    run_id: str | None = None,        # if None, auto timestamp id
    page: int = 0,
    overwrite: bool = False
) -> dict[str, str]:

    # Validate JSON is parseable (don’t transform it)
    try:
        json.loads(raw_text)
    except Exception as e:
        raise ValueError(f"raw_text is not valid JSON: {e}") from e

    ts = datetime.now(timezone.utc)
    dt = ts.strftime("%Y-%m-%d")
    rid = run_id or ts.strftime("%Y%m%dT%H%M%S%fZ")

    # Build partitioned directory
    parts: list[Path] = []
    parts.append(_to_local_path(base_dir))
    parts.append(Path(dataset))
    if partitions:
        for k, v in partitions.items():
            safe = str(v).strip().replace("/", "_")
            parts.append(Path(f"{k}={safe}"))
    parts += [Path(f"dt={dt}"), Path(f"run_id={rid}")]
    dest_dir = Path(*parts)
    _mkdirs(dest_dir)

    # File stems
    stem = dest_dir / f"page={page}"
    data_path = stem.with_suffix(".jsonl.gz")
    manifest_path = stem.with_name(stem.name + "._manifest.json")
    checksum_path = stem.with_name(stem.name + "._checksum.txt")

    # Write EXACT payload as one JSONL line (gzipped)
    if data_path.exists() and not overwrite:
        raw = raw_text
    else:
        if _is_dbfs_path(data_path):
            # robust: /tmp -> copy to dbfs
            with tempfile.TemporaryDirectory() as tdir:
                tmp_out = Path(tdir) / data_path.name
                with gzip.open(tmp_out, "wt", encoding="utf-8") as f:
                    f.write(raw_text.strip().replace("\n", " ") + "\n")
                _mkdirs(data_path.parent)
                dbu = _get_dbutils()
                if dbu is None:
                    raise RuntimeError("DBFS path detected but dbutils is unavailable.")
                dbu.fs.cp("file:" + str(tmp_out), "dbfs:/" + str(data_path)[len("/dbfs/"):], recurse=False)
        else:
            data_path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(data_path, "wt", encoding="utf-8") as f:
                f.write(raw_text.strip().replace("\n", " ") + "\n")
        raw = raw_text

    # Checksum sidecar
    checksum = _checksum_md5(raw)
    _atomic_write_text(checksum_path, checksum)

    # best-effort record count (Spotify search: artists.items)
    try:
        obj = json.loads(raw)
        rc = len(obj.get("artists", {}).get("items", []))
    except Exception:
        rc = 1

    # Manifest sidecar
    manifest = {
        "dataset": dataset,
        "path": _from_local_to_uri(str(data_path), base_dir),
        "partitions": dict(partitions or {}),
        "run_id": rid,
        "page": page,
        "record_count": rc,
        "checksum_md5": checksum,
        "fetched_at": _now_iso(),
    }
    _atomic_write_text(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2))

    return {
        "data": _from_local_to_uri(str(data_path), base_dir),
        "manifest": _from_local_to_uri(str(manifest_path), base_dir),
        "checksum": _from_local_to_uri(str(checksum_path), base_dir),
    }
