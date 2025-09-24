# main.py
import json, time
from Spotify.config import get_log, get_env
from Spotify.oauth import SpotifyOAuthClient
from Spotify.client import SpotifyClient  # your existing wrapper
from Spotify.schema import validate_json
from Spotify.raw_writer import write_raw_jsonl  # (your file)

def main():
    get_log()

    # 1) Auth + client
    cid = get_env("SPOTIFY_CLIENT_ID")
    sec = get_env("SPOTIFY_CLIENT_SECRET")
    auth = SpotifyOAuthClient(cid, sec)
    client = SpotifyClient(auth)

    # 2) Call API (dict), validate structure
    q, type_, limit = "videoclub", "artist", 50
    data = client.get_spotify(q=q, type=type_, limit=limit)   # returns dict
    validate_json(data)

    # 3) Prepare raw_text for Bronze (exact string form)
    # If you had requests.Response r, you'd prefer r.text.
    raw_text = json.dumps(data, ensure_ascii=False)

    # 4) Write Bronze (DBFS or local)
    run_id = str(int(time.time() * 1000))
    paths = write_raw_jsonl(
        raw_text=raw_text,
        base_dir="dbfs:/mnt/bronze",
        dataset="spotify_search",
        partitions={"q": q, "type": type_},
        run_id=run_id,
        page=0,
        overwrite=False,
    )

    print("Bronze paths:", paths)

if __name__ == "__main__":
    main()
