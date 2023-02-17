import os
import pickle
import requests
from tqdm import tqdm
from utils.scraping_utils import chunks
from utils.spotify_connector import refresh_spotify, load_music_data


def scrape_music_feature_data(track_uri, save_path):
    """
    given list of track uris, load and save music feature data
    :param track_uri:  list of spotify track uris
    :param save_path:   save path for music features  
    :return: metadata of scraped data, missing uris
    """
    all_data = []
    batches = list(chunks(track_uri, 100))
    missed_tracks = []
    sp = refresh_spotify()
    for batch in tqdm(batches):
        try:
            results = load_music_data(sp, batch)
        except:
            missed_tracks = missed_tracks + batch
        else:
            for uri, track in zip(batch, results):
                try:
                    data = {
                        'track_uri': uri, 
                        'danceability': track['danceability'],
                        'energy': track['energy'], 
                        'loudness': track['loudness'], 
                        'speechiness': track['speechiness'],
                        'acousticness': track['acousticness'], 
                        'instrumentalness': track['instrumentalness'], 
                        'liveness': track['liveness'], 
                        'valence': track['valence'], 
                        'tempo': track['tempo']
                    }
                    all_data.append(data)
                except:
                    missed_tracks.append(uri)
    pickle.dump(all_data, open(save_path, 'wb'))
    return all_data, missed_tracks