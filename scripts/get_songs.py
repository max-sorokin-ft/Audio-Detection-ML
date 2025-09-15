from google.cloud import storage
from auth import get_spotify_access_token
import requests
import json
import logging
import time
from tqdm import tqdm
import argparse
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

"""
    This script is part of the data acquisition pipeline for the project and it is used to get the albums for a given artist from the spotify api.
    It loops through the artists from a given kworb page and gets the albums for each artist from the spotify api.
    The json data is uploaded to a gcs bucket.
"""


def get_artists_from_gcs(bucket_name, blob_name):
    """Gets the artists from the gcs bucket"""
    try:
        logger.info(
            f"Getting artists from gcs bucket {bucket_name} with blob name {blob_name}"
        )
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        artists = json.loads(blob.download_as_string())
        logger.info(
            f"Successfully got {len(artists)} artists from gcs bucket {bucket_name} with blob name {blob_name}"
        )
        return artists
    except Exception as e:
        logger.error(
            f"Error getting artists from gcs bucket {bucket_name} with blob name {blob_name}: {e}"
        )
        raise Exception(
            f"Error getting artists from gcs bucket {bucket_name} with blob name {blob_name}: {e}"
        )
def get_albums_from_gcs(bucket_name, blob_name):
    """Gets the albums from the gcs bucket for a given artist"""
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        albums = json.loads(blob.download_as_string())
        return albums
    except Exception as e:
        logger.error(f"Error getting albums from gcs bucket {bucket_name} with blob name {blob_name}: {e}")
        raise Exception(f"Error getting albums from gcs bucket {bucket_name} with blob name {blob_name}: {e}")

def fetch_album_songs_from_spotify(album_id, token, max_retries=3, sleep_time=1):
    """Gets the songs from the spotify api"""
    for attempt in range(max_retries):
        try:
            url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"limit": 50}
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            backoff_time = sleep_time * (2**attempt)
            logger.warning(f"Error getting album songs from spotify: {e}. Retrying in {backoff_time} seconds.")
            time.sleep(backoff_time)
    logger.error(f"Error getting album songs from spotify: {e}")
    raise Exception(f"Error getting album songs from spotify: {e}")

def process_album_songs_from_spotify(album, token):
    """Processes the songs from the spotify api for a given album"""
    try:
        songs_list = []
        songs = fetch_album_songs_from_spotify(album["spotify_id"], token)
        for song in songs["items"]:
            individual_song = {}
            individual_song["spotify_id"] = song["id"]
            individual_song["name"] = song["name"]
            individual_song["album"] = album["album"]
            individual_song["artists"] = [artist["name"] for artist in song["artists"]]
            individual_song["primary_artist"] = song["artists"][0]["name"]
            individual_song["spotify_url"] = song["external_urls"]["spotify"]
            individual_song["release_date"] = album["release_date"]
            individual_song["duration_ms"] = song["duration_ms"]
            individual_song["explicit"] = song["explicit"]
            songs_list.append(individual_song)
        return songs_list
    except Exception as e:
        logger.error(f"Error processing album songs from spotify: {e}")
        raise Exception(f"Error processing album songs from spotify: {e}")

def write_songs_to_gcs(artists, bucket_name, base_blob_name):
    """Writes the songs from an album for an aritst to the gcs bucket"""
    try:
        token = get_spotify_access_token()
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        for artist in tqdm(artists):
            count = 0
            albums = get_albums_from_gcs(bucket_name, f"{artist['full_blob_name']}/albums.json")
            for album in albums:
                if album["type"] == "album":
                    count += 1
                    blob = bucket.blob(f"{artist['full_blob_name']}/{album['spotify_id']}/songs.json")
                    songs = process_album_songs_from_spotify(album, token)
                    blob.upload_from_string(json.dumps(songs, indent=3, ensure_ascii=False), content_type="application/json")
                    time.sleep(0.5)
            print(f"Successfully wrote albums' songs for {count} albums for artist {artist['artist']}")
        logger.info(f"Successfully wrote albums' songs for {len(artists)} artists to gcs bucket {bucket_name} with blob name {base_blob_name}")
    except Exception as e:
        logger.error(f"Error writing songs to gcs bucket {bucket_name} with blob name {base_blob_name}: {e}")
        raise Exception(f"Error writing songs to gcs bucket {bucket_name} with blob name {base_blob_name}: {e}")
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num_artists",
        type=int,
        default=1,
        help="The number of artists to get albums for",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="If true, the albums will not be written to gcs",
    )
    parser.add_argument(
        "--page_number",
        type=int,
        default=1,
        help="The number of the kworb's page",
    )
    parser.add_argument(
        "--batch_number",
        type=int,
        default=1,
        help="The batch number of the artists",
    )
    args = parser.parse_args()
    artists = get_artists_from_gcs(
        "music-ml-data",
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
    )
    if args.dry_run:
        pass
    else:
        write_songs_to_gcs(artists, "music-ml-data", f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}")