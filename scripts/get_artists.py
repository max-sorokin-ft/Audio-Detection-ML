import requests
from bs4 import BeautifulSoup
from auth import get_spotify_access_token
from tqdm import tqdm
from datetime import datetime
import json
import time
import logging
from google.cloud import storage


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

"""
    This script is part of the data acquisition pipeline for the project and it is used to get the artists and their metadata.
    It first scrapes the kworb's page to get the artists names, spotify id, and listeners.
    Then it uses the spotify id to get the artist's metadata from the spotify api.
    The metadata is saved to a json file, and uploaded to a gcp bucket.
"""

BASE_URL = "https://kworb.net/spotify/listeners{page_number}.html"


def get_artists_kworb(page_number):
    """Gets the html of the page from kworb's page"""
    logger.info(f"Getting html of page {page_number} from kworb's page")
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        # If page number is 1, then the url is the base url for kworb's page.
        if page_number == 1:
            url = BASE_URL.format(page_number="")
        else:
            url = BASE_URL.format(page_number=page_number)

        # return the html of the page
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully got html of page {page_number} from kworb's page")
        response.encoding = "utf-8"
        return response.text
    except Exception as e:
        logger.error(f"Error getting html page {page_number} for kworb artists: {e}")
        raise RuntimeError(
            f"Error getting html page {page_number} for kworb artists: {e}"
        )


def process_kworb_html(page_number):
    """Processes the html of the page from kworb's page"""
    try:
        html = get_artists_kworb(page_number)
        soup = BeautifulSoup(html, "lxml")
        tr_list = soup.find_all("tr")

        # Get the column map for the artist and listeners columns from the header row to later easily access the data and create a json data structure.
        artist_column_map = {}
        for i, th in enumerate(tr_list[0].find_all("th")):
            # Artist and Listeners are the columns we are interested in from kworb's page
            if th.text.strip() == "Artist" or th.text.strip() == "Listeners":
                artist_column_map[i] = th.text.strip()

        # Create a list of dictionaries for each artist and their data
        artist_list = []
        for tr in tr_list[1:]:
            # Set these intitial values because we want these to come first in the json data structure.
            individual_artist = {
                "spotify_id": None,
                "artist_name": None,
                "spotify_url": None,
                "init_processed_at": None,
            }
            for i, td in enumerate(tr.find_all("td")):
                if i in artist_column_map:
                    if td.find("a"):
                        href = td.find("a")["href"]
                        second_part = href.split("/")[-1]
                        spotify_id = second_part.split("_")[0]
                        individual_artist["spotify_id"] = spotify_id
                    if td.text.strip():
                        if artist_column_map[i] == "Listeners":
                            individual_artist["metrics"] = {}
                            individual_artist["metrics"]["kworb"] = {}
                            individual_artist["metrics"]["kworb"][
                                "monthly_listeners"
                            ] = int(td.text.strip().replace(",", ""))
                        elif artist_column_map[i] == "Artist":
                            individual_artist["artist_name"] = td.text.strip()
            artist_list.append(individual_artist)

        logger.info(f"Successfully processed html page {page_number} for kworb artists")
        return artist_list
    except Exception as e:
        logger.error(f"Error processing html page {page_number} for kworb artists: {e}")
        raise Exception(
            f"Error processing html page {page_number} for kworb artists: {e}"
        )


def get_artist_spotify(batch_artist_list, max_retries=3, sleep_time=1):
    """Gets the artist's metadata from the spotify api"""
    try:
        token = get_spotify_access_token()
    except Exception as e:
        logger.error(f"Error getting spotify access token: {e}")
        raise Exception(f"Error getting spotify access token: {e}")
    for attempt in range(max_retries):
        try:
            headers = {"Authorization": f"Bearer {token}"}

            spotify_ids = [artist["spotify_id"] for artist in batch_artist_list]
            spotify_ids_str = ",".join(spotify_ids)

            params = {"ids": spotify_ids_str}

            response = requests.get(
                f"https://api.spotify.com/v1/artists", headers=headers, params=params
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            backoff_time = sleep_time * (2**attempt)
            logger.warning(
                f"Error getting artist's metadata from spotify api: {e}. Retrying in {backoff_time} seconds."
            )
            time.sleep(backoff_time)
    logger.error(
        f"Error getting artist's metadata from spotify api. Failed after {max_retries} attempts."
    )
    raise RuntimeError(
        f"Error getting artist's metadata from spotify api. Failed after {max_retries} attempts."
    )


def process_spotify_response(artist_list, batch_size=50):
    """Processes the spotify response for batches of artists"""
    try:
        logger.info(f"Processing spotify response for {len(artist_list)} artists")
        for i in tqdm(range(0, len(artist_list), batch_size)):
            batch_artist_list = artist_list[i : i + batch_size]
            response = get_artist_spotify(batch_artist_list)
            for index, artist in enumerate(batch_artist_list):
                artist["spotify_url"] = response["artists"][index]["external_urls"][
                    "spotify"
                ]
                artist["init_processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                artist["metrics"]["spotify"] = {}
                artist["metrics"]["spotify"]["followers"] = int(
                    response["artists"][index]["followers"]["total"]
                )
                artist["metrics"]["spotify"]["popularity"] = int(
                    response["artists"][index]["popularity"]
                )
                artist["spotify_meta"] = {}
                artist["spotify_meta"]["genres"] = response["artists"][index]["genres"]
                artist["spotify_meta"]["images"] = response["artists"][index]["images"]
            time.sleep(1)
        logger.info(
            f"Successfully processed spotify response for {len(artist_list)} artists"
        )
        return artist_list
    except Exception as e:
        logger.error(f"Error processing spotify response: {e}")
        raise Exception(f"Error processing spotify response: {e}")


def write_to_gcs(artist_list, bucket_name, blob_name):
    """Writes the artist list to a json file in a gcp bucket"""
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(artist_list, indent=3, ensure_ascii=False), content_type="application/json")
        logger.info(f"Successfully wrote artist list to gcs bucket {bucket_name} with blob name {blob_name}")
    except Exception as e:
        logger.error(f"Error writing artist list to gcs bucket {bucket_name} with blob name {blob_name}: {e}")
        raise Exception(f"Error writing artist list to gcs bucket {bucket_name} with blob name {blob_name}: {e}")



if __name__ == "__main__":
    artist_list = process_kworb_html(1)
    artist_list = process_spotify_response(artist_list)
    write_to_gcs(artist_list, "music-ml-data", "raw-json-data/artists_page1_kworb/artists.json")

# {
#   "spotify_id": "19y5MFBH7gohEdGwKM7QsP",
#   "name": "Luther Vandross",
#   "spotify_url": "https://open.spotify.com/artist/19y5MFBH7gohEdGwKM7QsP",
#   "ingested_at": "2025-08-24T12:00:00Z",
#   "metrics": {
#     "kworb": {
#       "monthly_listeners": 5331858
#     },
#     "spotify": {
#       "followers": 2500000,
#       "popularity": 71,
#     }
#   },
#   "spotify_meta": {
#     "genres": ["r&b", "soul"],
#     "images": [
#       {
#         "url": "https://i.scdn.co/image/abc",
#         "height": 640,
#         "width": 640
#       }
#     ]
#   }
# }
