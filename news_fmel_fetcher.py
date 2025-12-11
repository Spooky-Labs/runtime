"""
News FMEL Fetcher - Retrieves full news content from FMEL storage

Fetches news articles directly from Cloud Storage using the data_hash.
Path format: fmel/news/{hash[:2]}/{hash}.json (matches data_storage.py)

Uses FMEL_BUCKET environment variable for the bucket name.
"""

import json
import logging
import os
from typing import Dict, Optional

from google.cloud import storage

logger = logging.getLogger(__name__)


class NewsFMELFetcher:
    """
    Fetches full news content from FMEL Cloud Storage.

    Uses FMEL_BUCKET environment variable and constructs blob path
    directly from data_hash (matching data_storage.py pattern).
    """

    def __init__(self, bucket_name: Optional[str] = None):
        self.storage_client = storage.Client()

        # Get bucket name from param or environment (FMEL_BUCKET)
        self.bucket_name = bucket_name or os.environ.get('FMEL_BUCKET')
        if not self.bucket_name:
            raise ValueError("FMEL_BUCKET environment variable required")
        self.bucket = self.storage_client.bucket(self.bucket_name)

        logger.info(f"NewsFMELFetcher initialized with bucket: {self.bucket_name}")

    def fetch(self, data_hash: str) -> Dict:
        """
        Fetch full article from FMEL using data_hash.

        Since we have the data_hash and know the path format from data_storage.py,
        we can fetch directly from Cloud Storage without querying BigQuery first.

        Blob path: fmel/news/{hash[:2]}/{hash}.json

        Args:
            data_hash: SHA-256 hash from Pub/Sub message

        Returns:
            Dict with full article: {headline, summary, content, author, url, symbols, source, ...}
        """
        if not data_hash:
            return {}

        try:
            # Fetch directly from GCS using known path format
            return self._fetch_from_gcs(data_hash)

        except Exception as e:
            logger.error(f"FMEL fetch failed for {data_hash[:16]}: {e}")
            return {}

    def _fetch_from_gcs(self, data_hash: str) -> Dict:
        """
        Fetch JSON from Cloud Storage using data_hash.

        Blob path format (from data_storage.py):
            fmel/{data_type}/{hash[:2]}/{hash}.json

        Example:
            fmel/news/ab/abc123def456....json

        Uses FMEL_BUCKET environment variable for bucket name.
        """
        # Construct blob path directly using the hash (matches data_storage.py)
        blob_path = f"fmel/news/{data_hash[:2]}/{data_hash}.json"

        blob = self.bucket.blob(blob_path)
        return json.loads(blob.download_as_text())
