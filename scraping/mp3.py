import os
import pickle
import requests
from tqdm import tqdm
from utils.scraping_utils import chunks
from utils.spotify_connector import refresh_spotify, load_track_data

import json
import os
import urllib.request

scratch_mp3_path = '/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/mp3/'
def download_item(audio_path, uri, url):
    fn = uri+'.mp3'
    subpath = uri[-2:]
    full_path = os.path.join(audio_path, subpath)
    full_fn = os.path.join(full_path, fn)
    # print(full_path, full_fn)
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    if not os.path.exists(full_fn):
        for i in range(5):
            try:
                urllib.request.urlretrieve(url, full_fn)
            except Exception as e:
                print("retreival failed", e)
                exit() 
            else:
                break
        else:
            print('url error. continue')
    return full_fn

def scrape_mp3_data(track_uri, save_path=None):
    """
    given list of track uris, load and save mp3 data
    :param track_uri:  list of spotify track uris
    :param save_path:   save path for mp3s  
    :return: metadata of scraped data, missing uris
    """
    all_data = []
    batches = list(chunks(track_uri, 50))
    missed_tracks = []
    sp = refresh_spotify()
    for batch in tqdm(batches):
        try:
            results = load_track_data(sp, batch)
        except: 
            missed_tracks = missed_tracks + batch
            print("error in loading tracks, missing tracks:{}".format(len(missed_tracks)))
        for uri, track in zip(batch, results['tracks']):
            # print(uri, track['preview_url']) 
            if track['preview_url']: 
                try: 
                    track_id = uri.split(":")[-1]
                    path = download_item(scratch_mp3_path, track_id, track['preview_url'])
                    data = { 
                        'track_uri': uri, 
                        'mp3_link': track['preview_url'], 
                        'folder': path
                    }
                    all_data.append(data)
                    print('saved to: {}'.format(path))
                except: 
                    missed_tracks.append(uri)
                    print("download failed, missing tracks:{}".format(len(missed_tracks)))
                    
            else: 
                missed_tracks.append(uri)
                print("preview_url for uri:{} is missing, missing tracks:{}".format(uri, len(missed_tracks)))
                
    pickle.dump(all_data, open(save_path, 'wb'))    
            
        
    
def test_one(): 
    uri = '1lzr43nnXAijIGYnCT8M8H'
    sp = refresh_spotify()
    # results = sp.track(track_id=uri)
    results = sp._get('tracks?ids=%s&market=%s'%(uri, 'US'), limit=1)
    print(results)