import requests
import logging
import time


logger = logging.getLogger(__name__)

BASE_URL = "https://accounts.spotify.com/api/token"

class SpotifyOAuthClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token, self._exp = None, 0
        if not self.client_id or not self.client_secret:
            raise RuntimeError("Missing SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET")
        
        
    def get_token(self, retires: int = 3, backoff: int = 1):
        if self._token or time.time() < self._exp - 30:
            return self._token
        
        data = {"grant_type": "client_credentials"}
        auth = {self.client_id : self.client_secret}

        for attempt in range(1, retires+1):
            logger.info("requesting the token from the API %s/%s ", attempt, retires)
            try:
                r = requests.post(BASE_URL, data=data, auth=auth, timeout=20)
                if (r.status_code == 429) or (500 < r.status_code < 600) and attempt < retires:
                    wait = attempt * backoff
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                payload = r.json()
                self._token = payload["access_token"]
                self._exp = time.time() + int(payload.get("expires_in", 3600))
                return self._token
            
            except requests.exceptions.RequestException as e:
                if attempt < retires:
                    wait = attempt * backoff
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"Request failed after {retires} attempts: {e}")
        raise RuntimeError("Unexpected failure in fetching the token!")