"""
utils/downloader.py — Shared file download utility for CD Command Center ETL scripts.

Provides a single download_file() function used by all ETL scripts that need to
pull data from URLs instead of relying on manually downloaded files.

Features:
  - Progress display during download (shows MB downloaded)
  - Skips download if a recent copy already exists locally (7-day cache by default)
  - Handles both direct file URLs and zip archives (auto-extracts a target CSV)
  - Retries on transient network errors (up to 3 attempts)
  - Raises clear errors on HTTP failures

Usage:
    from utils.downloader import download_file, download_and_extract_zip

    # Download a file (skips if already exists and is recent):
    local_path = download_file(
        url="https://example.gov/data.xlsx",
        dest_path="data/raw/data.xlsx",
        description="Example dataset",
    )

    # Download a zip and extract one file from it:
    local_csv = download_and_extract_zip(
        url="https://example.gov/data.zip",
        zip_dest="data/raw/data.zip",
        extract_pattern="*.csv",   # glob pattern for the file inside the zip
        extract_dest="data/raw/data.csv",
        description="Example zip dataset",
    )
"""

import os
import time
import fnmatch
import zipfile
import requests

# Age threshold in seconds. If a cached file is newer than this, skip re-download.
# Default: 7 days. Set to 0 to always re-download.
CACHE_MAX_AGE_SECONDS = 7 * 24 * 60 * 60

# How many bytes to download per chunk
CHUNK_SIZE = 1024 * 1024  # 1 MB


def _file_is_recent(path: str, max_age_seconds: int = CACHE_MAX_AGE_SECONDS) -> bool:
    """Return True if the file exists and was modified within max_age_seconds."""
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age < max_age_seconds


def download_file(
    url: str,
    dest_path: str,
    description: str = None,
    force: bool = False,
    max_age_seconds: int = CACHE_MAX_AGE_SECONDS,
    timeout: int = 120,
) -> str:
    """
    Download url to dest_path. Shows progress as MB downloaded.

    Args:
        url:             URL to download.
        dest_path:       Local path to save the file.
        description:     Human-readable label shown in progress output.
        force:           If True, re-download even if a recent file exists.
        max_age_seconds: Skip download if local file is younger than this.
        timeout:         HTTP request timeout in seconds.

    Returns:
        dest_path (the local file path, whether downloaded or cached).

    Raises:
        RuntimeError if the download fails after retries.
    """
    label = description or os.path.basename(dest_path)

    # Use cached file if it's recent enough
    if not force and _file_is_recent(dest_path, max_age_seconds):
        age_hours = (time.time() - os.path.getmtime(dest_path)) / 3600
        print(f"  [cache] {label}: using existing file (downloaded {age_hours:.0f}h ago)")
        print(f"         {dest_path}")
        return dest_path

    # Make sure the destination directory exists
    os.makedirs(os.path.dirname(dest_path) if os.path.dirname(dest_path) else ".", exist_ok=True)

    print(f"  [download] {label}")
    print(f"    from: {url}")
    print(f"    to:   {dest_path}")

    last_exception = None
    for attempt in range(1, 4):  # up to 3 attempts
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (CD Command Center ETL; contact: data@example.org)"
            }
            response = requests.get(url, stream=True, timeout=timeout, headers=headers)

            if response.status_code == 404:
                raise RuntimeError(
                    f"HTTP 404 — file not found at URL.\n"
                    f"  URL: {url}\n"
                    f"  This URL may have changed. Check the source website and update the URL in the ETL script."
                )

            if response.status_code != 200:
                raise RuntimeError(
                    f"HTTP {response.status_code} downloading {label}.\n"
                    f"  URL: {url}"
                )

            total_bytes = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_bytes:
                            pct = downloaded / total_bytes * 100
                            print(
                                f"    {downloaded / 1e6:.1f} MB / {total_bytes / 1e6:.1f} MB "
                                f"({pct:.0f}%)    ",
                                end="\r",
                            )
                        else:
                            print(f"    {downloaded / 1e6:.1f} MB downloaded...", end="\r")

            print(f"    Done: {downloaded / 1e6:.1f} MB saved.           ")
            return dest_path

        except RuntimeError:
            raise  # Don't retry 404s or explicit errors — re-raise immediately
        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < 3:
                wait = 2 ** attempt  # 2s, 4s
                print(f"    Network error (attempt {attempt}/3): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"Failed to download {label} after 3 attempts: {e}\n"
                    f"  URL: {url}"
                ) from last_exception

    return dest_path  # unreachable but satisfies type checker


def download_and_extract_zip(
    url: str,
    zip_dest: str,
    extract_pattern: str,
    extract_dest: str,
    description: str = None,
    force: bool = False,
    max_age_seconds: int = CACHE_MAX_AGE_SECONDS,
) -> str:
    """
    Download a zip file and extract a single file matching extract_pattern.

    Useful for large datasets like EPA EJScreen which are published as zip archives.

    Args:
        url:              URL of the zip file.
        zip_dest:         Where to save the downloaded zip.
        extract_pattern:  Glob pattern to match the target file inside the zip
                          (e.g. "EJSCREEN_2024_Tracts*.csv"). Matches the filename
                          only, not the full path inside the zip.
        extract_dest:     Where to save the extracted file.
        description:      Human-readable label for progress output.
        force:            Re-download even if a recent extracted file exists.
        max_age_seconds:  Cache threshold.

    Returns:
        extract_dest (the path of the extracted file).

    Raises:
        RuntimeError if download fails or no matching file is found in the zip.
    """
    label = description or os.path.basename(extract_dest)

    # If the extracted file is already recent, skip everything
    if not force and _file_is_recent(extract_dest, max_age_seconds):
        age_hours = (time.time() - os.path.getmtime(extract_dest)) / 3600
        print(f"  [cache] {label}: using existing extracted file (downloaded {age_hours:.0f}h ago)")
        print(f"         {extract_dest}")
        return extract_dest

    # Download the zip (uses its own cache check)
    download_file(url, zip_dest, description=f"{label} (zip)", force=force, max_age_seconds=max_age_seconds)

    # Extract the matching file from the zip
    print(f"  [extract] Looking for '{extract_pattern}' inside {zip_dest}...")
    os.makedirs(os.path.dirname(extract_dest) if os.path.dirname(extract_dest) else ".", exist_ok=True)

    with zipfile.ZipFile(zip_dest, "r") as zf:
        members = zf.namelist()
        # Match on just the filename portion (ignore paths inside zip)
        matches = [m for m in members if fnmatch.fnmatch(os.path.basename(m), extract_pattern)]

        if not matches:
            raise RuntimeError(
                f"No file matching '{extract_pattern}' found inside {zip_dest}.\n"
                f"  Files in zip: {members[:20]}"
            )

        # Use the first match (typically there's only one)
        target = matches[0]
        print(f"  [extract] Extracting '{target}' → {extract_dest}")

        with zf.open(target) as src, open(extract_dest, "wb") as dst:
            total = 0
            while True:
                chunk = src.read(CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
                total += len(chunk)
                print(f"    {total / 1e6:.0f} MB extracted...", end="\r")

        print(f"    Done: {total / 1e6:.0f} MB extracted.           ")

    return extract_dest
