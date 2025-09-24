from util import _ensure_dict, _ensure_list


def validate_json(payload: dict) -> None:
    
    _ensure_dict(payload, ["artists"], "payload")

    artists = payload["artists"]

    _ensure_dict(artists, ["items"], "payload['artists']")

    items = payload["artists"]["items"]

    _ensure_list(items, "payload['artists']['items']")

    for i, artist in enumerate(items):
        _ensure_dict(artist, ["followers", "genres", "name", "popularity", "type"], f"payload['artists']['items'][{i}]")