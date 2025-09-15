import requests
import re
import os

def get_spotify_preview_url(track_id: str) -> str:
    """
    Get the Spotify 30s preview URL using the embed endpoint.
    """
    embed_url = f"https://open.spotify.com/embed/track/{track_id}"
    resp = requests.get(embed_url, timeout=10)

    if resp.status_code != 200:
        raise Exception(f"Failed to fetch embed page: {resp.status_code}")

    # Extract preview MP3 URL using regex
    match = re.search(r'"audioPreview"\s*:\s*\{"url":"(https:[^"]+)"\}', resp.text)
    return match.group(1) if match else None

def download_preview(track_id: str, save_dir="previews"):
    """
    Download the 30s preview MP3 for a given Spotify track ID.
    """
    url = get_spotify_preview_url(track_id)
    if not url:
        print(f"❌ No preview available for track {track_id}")
        return

    os.makedirs(save_dir, exist_ok=True)
    filepath = os.path.join(save_dir, f"{track_id}.mp3")

    # Download MP3 file
    resp = requests.get(url, stream=True)
    if resp.status_code == 200:
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(1024):
                f.write(chunk)
        print(f"✅ Downloaded preview: {filepath}")
    else:
        print(f"❌ Failed to download preview for {track_id}")

if __name__ == "__main__":
    # Example: Wonderwall
    track_id = "1qPbGZqppFwLwcBC1JQ6Vr"
    download_preview(track_id)

