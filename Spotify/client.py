import requests
import logging
import time
from Spotify.oauth import SpotifyOAuthClient

BASE_URL = "https://api.spotify.com/v1/search"

logger = logging.getLogger(__name__)

class SpotifyClient:
    def __init__(self, auth: SpotifyOAuthClient):
        self.auth = auth
    

    def get_spotify(self, q: str, type: str, limit: int = 10, offset: int = 0, retries: int = 3, backoff: int = 1) -> dict:
        token = self.auth.get_token()
        params = {"q": q, "type": type, "limit": limit, "offset": offset}
        headers = {"Authorization" : f"Bearer {token}"}

        for attempt in range(1, retries+1):
            logger.info("Attempt call API from %s artist=%s type=%s",BASE_URL, q, type)
            try:
                r = requests.get(BASE_URL, params, headers, timeout=20)
                if (r.status_code == 429) or (500 < r.status_code < 600) and attempt < retries:
                    wait = attempt * backoff
                    time.sleep(wait)
                    continue              

                r.raise_for_status()
                data = r.json()
                return data
            
            except requests.exceptions.RequestException as e:
                if attempt < retries:
                    wait = attempt * backoff
                    time.sleep(wait)
                    continue
                raise RuntimeError("API called failed")

