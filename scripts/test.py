# save as check_throttle.py
import requests
from auth import get_spotify_access_token

ARTIST_ID = "0du5cEVh5yTK9QJze8zA0C"  # Bruno Mars (any valid ID works)

def main():
    token = get_spotify_access_token()
    url = f"https://api.spotify.com/v1/artists/{ARTIST_ID}"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers, timeout=10)
    if r.status_code == 429:
        ra = r.headers.get("Retry-After")  # seconds
        print(f"429 Too Many Requests. Retry-After={ra} seconds")
        return

    # Not rate-limited — just show status (keep it simple)
    print(f"OK {r.status_code}")

if __name__ == "__main__":
    main()
