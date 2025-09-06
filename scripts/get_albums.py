from google.cloud import storage
from auth import get_spotify_access_token
import requests
import json
import logging
import time
from tqdm import tqdm
import argparse

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


def get_albums_from_spotify(spotify_artist_id, token, max_retries=3, sleep_time=1):
    """Gets the albums for a given artist from the spotify api"""
    for attempt in range(max_retries):
        try:
            url = f"https://api.spotify.com/v1/artists/{spotify_artist_id}/albums"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"limit": 50, "include_groups": "album,single,compilation", "market": "US"}
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            backoff_time = sleep_time * (2**attempt)
            logger.warning(
                f"Error getting artist's metadata from spotify api: {e}. Retrying in {backoff_time} seconds."
            )
            time.sleep(backoff_time)
    logger.error(
        f"Error getting albums from spotify. Failed after {max_retries} attempts."
    )
    raise RuntimeError(
        f"Error getting albums from spotify. Failed after {max_retries} attempts."
    )


def process_albums_from_spotify(artist, token):
    """Processes the albums for a given artist from the spotify api"""
    try:
        album_list = []
        spotify_id = artist["spotify_id"]
        response = get_albums_from_spotify(spotify_id, token)
        for album in response["items"]:
            individual_album = {}
            individual_album["id"] = album["id"]
            individual_album["name"] = album["name"]
            individual_album["artist_name"] = artist["artist_name"]
            individual_album["url"] = album["external_urls"]["spotify"]
            individual_album["type"] = album["album_type"]
            individual_album["release_date"] = album["release_date"]
            individual_album["total_tracks"] = album["total_tracks"]
            individual_album["is_processed"] = False
            individual_album["images"] = album["images"]
            album_list.append(individual_album)
        logger.info(
            f"Successfully processed {len(album_list)} albums for {artist['artist_name']} from spotify"
        )
        return album_list
    except Exception as e:
        logger.error(f"Error processing albums from spotify: {e}")
        raise Exception(f"Error processing albums from spotify: {e}")

def write_albums_to_gcs(bucket_name, base_blob_name, artists):
    """Writes the albums to the gcs bucket"""
    try:
        token = get_spotify_access_token()
        client = storage.Client.from_service_account_json("gcp_creds.json")
        for artist in tqdm(artists):
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(f"{base_blob_name}/{artist['spotify_id']}_{artist['artist_name']}/albums.json")
            albums = process_albums_from_spotify(artist, token)
            blob.upload_from_string(
                json.dumps(albums, indent=3, ensure_ascii=False),
                content_type="application/json",
            )
        logger.info(
            f"Successfully wrote albums for {len(artists)} artists to gcs bucket {bucket_name} with base blob name {base_blob_name}"
        )
    except Exception as e:
        logger.error(
            f"Error writing albums to gcs bucket {bucket_name} with base blob name {base_blob_name}: {e}"
        )
        raise Exception(
            f"Error writing albums to gcs bucket {bucket_name} with base blob name {base_blob_name}: {e}"
        )


def dry_run(artists, number_of_artists, base_blob_name):
    try:
        """Dry run to write albums for a given artist to a file"""
        token = get_spotify_access_token()
        for artist in tqdm(artists[:number_of_artists]):
            albums = process_albums_from_spotify(artist, token)
            with open(f"{artist['spotify_id']}_{artist['artist_name']} albums.json", "w") as f:
                logger.info(
                    f"Dry run: Would write {len(albums)} albums to {base_blob_name}/{artist['spotify_id']}_{artist['artist_name']}/albums.json"
                )
                json.dump(albums, f, indent=3, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error writing albums to json file: {e}")
        raise Exception(f"Error writing albums to json file: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num_artists",
        type=int,
        default=1,
        help="The number of artists to scrape",
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
        help="The page number of the kworb's page to scrape",
    )
    args = parser.parse_args()
    artists = get_artists_from_gcs(
        "music-ml-data",
        f"raw-json-data/artists_page{args.page_number}_kworb/artists.json",
    )
    if args.dry_run:
        dry_run(
            artists,
            args.num_artists,
            f"raw-json-data/artists_page{args.page_number}_kworb",
        )
    else:
        write_albums_to_gcs(
            "music-ml-data",
            f"raw-json-data/artists_page{args.page_number}_kworb",
            artists,
        )
