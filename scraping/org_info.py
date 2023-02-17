
import os
import pickle
import requests
import ipdb 
from tqdm import tqdm


import pandas as pd 
import time 
import ipdb 
import os 
from tqdm import tqdm 


from spotipy.exceptions import SpotifyException
from utils.scraping_utils import chunks
from utils.spotify_connector import open_sp_session, load_track_data

import requests
import urllib3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import time



def scrape_org_info_from_track_uri(track_uri, output_path): 
    all_data = []
    batches = list(chunks(track_uri, 30))
    missed_tracks = []
    sp = open_sp_session() 
    for batch in tqdm(batches):
        try:
            results = sp.tracks(tracks=batch)
        except SpotifyException as e: 
            if e.http_status == 429:
                print("'retry-after' value:", e.headers['retry-after'])
                time.sleep(1)
            else: print("other Spotify error", e)
        except Exception as e:
            print("non-spotify error", e)
            missed_tracks = missed_tracks + batch
            print("missed track count", len(missed_tracks))
        else:
            for uri, track in zip(batch, results['tracks']):
                try:
                    album = track['album']
                    artists = track['artists']
                    data = {
                        'track_name': track['name'], 
                        'track_uri': uri, 
                        'album_uri': album['uri'], 
                        'album_name': album['name'], 
                        'artist_names': [a['name'] for a in artists], 
                        'artist_uris': [a['uri'] for a in artists]
                    }
                    all_data.append(data)
                except:
                    missed_tracks.append(uri)    
    pickle.dump(all_data, open(output_path, 'wb'))
    return all_data, missed_tracks
    
def scrape_org_info_chunked(track_uris, output_path, batch_size = 30): 
    
    
    session = requests.Session()
    retry = urllib3.Retry(
        respect_retry_after_header=False
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(),
                        requests_session=session)
    
    batches = list(chunks(track_uris, batch_size))[32663:]

    missed_tracks = [] 
    i = 32663
    start, end = batch_size*i, (batch_size*i) + (batch_size-1) #2730, 2729+30
    for batch in tqdm(batches):
        all_data = []
        path =  output_path + '{}-{}_org.pkl'.format(start, end)
        try:
            time.sleep(8)
            results = sp.tracks(tracks=batch)
        except SpotifyException as e: 
            if e.http_status == 429:
                print("'retry-after' value:", e.headers['retry-after'])
                retry_value = e.headers['retry-after']
                if int(e.headers['retry-after']) > 60: 
                    print("STOP FOR TODAY, retry value too high {}".format(retry_value))
                    exit() 
                else: 
                    time.sleep(retry_values)
                    continue 
            else: 
                print("other Spotify error", e)
                continue 
        except Exception as e:
            print("non-spotify error", e)
            missed_tracks = missed_tracks + batch
            print("missed track count", len(missed_tracks))
            continue 
        else:
            for uri, track in zip(batch, results['tracks']):
                try:
                    album = track['album']
                    artists = track['artists']
                    data = {
                        'track_name': track['name'], 
                        'track_uri': uri, 
                        'album_uri': album['uri'], 
                        'album_name': album['name'], 
                        'artist_names': [a['name'] for a in artists], 
                        'artist_uris': [a['uri'] for a in artists]
                    }
                    all_data.append(data)
                except:
                    missed_tracks.append(uri) 
        # for idx, track in zip(list(range(len(batch))), batch): 
        #     data = {
        #         'idx': idx, 
        #         'track': track
        #     }
        #     all_data.append(data)
        i += 1
        start, end = end+1, end+len(all_data) 
        print("Saving to", path, len(all_data))
        print(all_data[0])   
        # if i > 8531: exit() 
        pickle.dump(all_data, open(path, 'wb')) 

