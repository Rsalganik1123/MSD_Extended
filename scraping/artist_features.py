import os
import pickle
import requests
from tqdm import tqdm
from utils.scraping_utils import chunks
from utils.spotify_connector import refresh_spotify, load_artist_data
 
def scrape_artist_data(artist_uri, save_path):
    """
    given list of artist uris, load and save artist data
    :param artist_uri:  list of spotify artist uris
    :param save_path:   output path
    :return: metadata of scraped data, missing uris
    """
    all_data = []
    batches = list(chunks(artist_uri, 10))
    missed_artists = []
    sp = refresh_spotify()
    for batch in tqdm(batches):
        try:
            results = load_artist_data(sp, batch)
        except:
            missed_artists = missed_artists + batch
        else:
            for uri, artist in zip(batch, results['artists']):
                try:
                    data = {
                        'artist_uri': artist['uri'],
                        'artist_name': artist['name'],
                        'popularity': artist['followers']['total'],
                        'genres': artist['genres']
                    }
                    all_data.append(data)
                except:
                    missed_artists.append(uri)
    pickle.dump(all_data, open(save_path, 'wb'))
    return all_data, missed_artists