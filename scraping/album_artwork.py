import os
import pickle
import requests
from tqdm import tqdm
from utils.scraping_utils import chunks
from utils.spotify_connector import refresh_spotify, load_album_data


import requests
import urllib3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import time

def scrape_albums_data_from_track_uri(track_uri, imgs_folder, meta_path): 
    """
    given list of track uris, load and save album data, save images as well
    :param tracks_uri:  list of spotify tracks uris
    :param imgs_folder: output images folder
    :param meta_path:   output meta path
    :return: metadata of scraped data, missing uris
    """
    all_data = []
    batches = list(chunks(track_uri, 100))
    missed_tracks = []
    sp = refresh_spotify()
    for batch in tqdm(batches):
        try:
            results = load_track_data(sp, batch)
        except:
            missed_tracks = missed_tracks + batch
        else:
            for uri, track in zip(batch, results['tracks']):
                try:
                    album = track['album']
                    data = {
                        'track_uri': uri, 
                        'album_uri': album['uri'],
                        'album_name': album['name'],
                        'album_popularity': album['popularity'],
                        'album_release_date': album['release_date'],
                        'album_total_tracks': album['total_tracks']
                    }
                    imgs = album['images']
                    if len(imgs) > 0:
                        if len(imgs) > 1:
                            idx = 1
                        else:
                            idx = 0
                        img_url = album['images'][idx]['url']
                        img_data = requests.get(img_url).content
                        dl_path = os.path.join(imgs_folder, '{0}.jpg'.format(album['uri']))
                        with open(dl_path, 'wb') as handler:
                            handler.write(img_data)
                        data['image_path'] = dl_path
                    else:
                        data['image_path'] = 'NO_IMAGE'
                    all_data.append(data)
                except:
                    if track is not None:
                        raise
                    missed_tracks.append(uri)
    pickle.dump(all_data, open(meta_path, 'wb'))
    return all_data, missed_tracks
    
def scrape_album_data_from_album_uri_chunked(album_uris, imgs_folder, output_path, batch_size = 10, offset = 0):
    session = requests.Session()
    retry = urllib3.Retry(
        respect_retry_after_header=False
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(),
                        requests_session=session)
    
    batches = list(chunks(album_uris, batch_size))[offset:]
    missed_albums = [] 
    start, end = batch_size*offset, (batch_size*offset) + (batch_size-1) #2730, 2729+30
    for batch in tqdm(batches):
        all_data = []
        path =  output_path + '{}-{}_org.pkl'.format(start, end)
        try:
            time.sleep(8)
            results = sp.albums(batch) #sp.tracks(tracks=batch)
        except SpotifyException as e: 
            if e.http_status == 429:
                print("'retry-after' value:", e.headers['retry-after'])
                retry_value = e.headers['retry-after']
                if int(e.headers['retry-after']) > 60: 
                    print("STOP FOR TODAY, retry value too high {}".format(retry_value))
                    exit() 
                else: 
                    time.sleep(retry_values)
                    missed_albums = missed_albums + batch
                    print("missed track count", len(missed_albums))
                    continue 
            else: 
                print("other Spotify error", e)
                missed_albums = missed_albums + batch
                print("missed track count", len(missed_albums))
                continue 
        except Exception as e:
            print("non-spotify error", e)
            missed_albums = missed_albums + batch
            print("missed track count", len(missed_albums))
            continue
        else:
            for uri, album in zip(batch, results['albums']):
                try:
                    data = {
                        'album_uri': album['uri'],
                        'album_name': album['name'],
                        'album_popularity': album['popularity'],
                        'album_release_date': album['release_date'],
                        'album_total_tracks': album['total_tracks']
                    }
                    imgs = album['images']
                    if len(imgs) > 0:
                        if len(imgs) > 1:
                            idx = 1
                        else:
                            idx = 0
                        img_url = album['images'][idx]['url']
                        img_data = requests.get(img_url).content
                        dl_path = os.path.join(imgs_folder, '{0}.jpg'.format(album['uri']))
                        with open(dl_path, 'wb') as handler:
                            handler.write(img_data)
                        data['image_path'] = dl_path
                    else:
                        data['image_path'] = 'NO_IMAGE'
                    all_data.append(data)
                except:
                    if album is not None:
                        raise
                    missed_albums.append(uri)
        # for idx, album in zip(list(range(len(batch))), batch): 
        #     data = {
        #         'idx': idx, 
        #         'album': album
        #     }
        #     all_data.append(data)
        start, end = end+1, end+len(all_data) 
        print("Saving to", path, len(all_data))
        print(all_data[0])   
       
        pickle.dump(all_data, open(path, 'wb')) 
        # break


def scrape_albums_data_from_album_uri(albums_uri, imgs_folder, meta_path):
    """
    given list of album uris, load and save album data, save images as well
    :param albums_uri:  list of spotify album uris
    :param imgs_folder: output images folder
    :param meta_path:   output meta path
    :return: metadata of scraped data, missing uris
    """
    all_data = []
    batches = list(chunks(albums_uri, 10))
    missed_albums = []
    sp = refresh_spotify()
    for batch in tqdm(batches):
        try:
            results = load_album_data(sp, batch)
        except:
            missed_albums = missed_albums + batch
        else:
            for uri, album in zip(batch, results['albums']):
                try:
                    data = {
                        'album_uri': album['uri'],
                        'album_name': album['name'],
                        'album_popularity': album['popularity'],
                        'album_release_date': album['release_date'],
                        'album_total_tracks': album['total_tracks']
                    }
                    imgs = album['images']
                    if len(imgs) > 0:
                        if len(imgs) > 1:
                            idx = 1
                        else:
                            idx = 0
                        img_url = album['images'][idx]['url']
                        img_data = requests.get(img_url).content
                        dl_path = os.path.join(imgs_folder, '{0}.jpg'.format(album['uri']))
                        with open(dl_path, 'wb') as handler:
                            handler.write(img_data)
                        data['image_path'] = dl_path
                    else:
                        data['image_path'] = 'NO_IMAGE'
                    all_data.append(data)
                except:
                    if album is not None:
                        raise
                    missed_albums.append(uri)
    pickle.dump(all_data, open(meta_path, 'wb'))
    return all_data, missed_albums