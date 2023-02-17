# import pandas as pd 
# from utils.private import * 
# from scraping.music_features import scrape_music_feature_data
# from scraping.album_artwork import scrape_albums_data 
# from scraping.artist_features import scrape_artist_data 
# from scraping.mp3 import scrape_mp3_data, test_one
# from processing.text_embeddings import * 

# def scraper(data_path, uri_key, output_path, mode): 
#     data = pd.read_csv(data_path, sep='\t')
#     print("Loaded data for {} with length:{}".format(mode, len(data)))
#     uri = data[uri_key]
#     print("Run {} Extraction")
#     if mode == "album artwork": 
#         return 0 
#     if mode == "music features": 
#         all_data, missed_data = scrape_music_feature_data(uris, output_path)
#     if mode == 'artist features': 
#         all_data, missed_data = scrape_artist_data(uris, output_path)

#     print("Saved data of length:{} , missing track count:{}".format(len(missed_tracks))) 



# scraper(data_path = OG_CSV_data_path+'tracks.csv', uri_key='track_uri', output_file=save_path+'music_features.pkl', mode="music features")

# artists = pd.read_csv(OG_CSV_data_path+'artists.csv', sep='\t')
# tracks = pd.read_csv(OG_CSV_data_path+'tracks.csv', sep='\t')
# albums = pd.read_csv(OG_CSV_data_path+'albums.csv', sep='\t')['album_uri']
# playlists = pd.read_csv(OG_CSV_data_path+'train_playlists.csv', sep='\t')['pid']
# interactions = pd.read_csv(OG_CSV_data_path+'train_interactions.csv', sep='\t') 
# print("Loaded tracks.csv with length:{}".format(len(tracks)))
# uris = tracks['track_uri']
# print("Extracting music features")
# all_data, missed_tracks = scrape_music_feature_data(uris, save_path+'music_features.pkl')
# print("Saved, missing track count:{}".format(len(missed_tracks))) 

# def parse_args():
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         "--input_path",
#         type=str,
#         help="input data path",
#     )
#     parser.add_argument(
#         "--output_path",
#         type=str,
#         help="output data path"
#     )
#     parser.add_argument(
#         "--text_key",
#         type=str,
#         help="key name of the text entry to compute the embedding"
#     )
#     parser.add_argument(
#         "--output_key",
#         type=str,
#         help="key name to save the embedding"
#     )
#     args = parser.parse_args()
#     return args


# def main():
#     args = parse_args()
#     print(args)
#     generate_text_features_file(args.input_path, args.output_path, args.text_key, args.output_key)


# if __name__ == '__main__':
#     main()

# import pickle
# save_path = '/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/mp3/mp3_org.pkl'
# data = pickle.load(open('/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/final_pieces/complete_data_final_3way_with_emb_and_pos_contig.pkl', "rb"))['df_track']
# track_uris = data['track_uri'].unique() 
# print(len(track_uris))
# scrape_mp3_data(track_uris, save_path)

# test_one()

import pandas as pd 
import time 
import ipdb 
import os 
from tqdm import tqdm 
import pickle 

from utils.private import * 
from utils.spotify_connector import refresh_spotify
from scraping.music_features import scrape_music_feature_data 
from scraping.album_artwork import scrape_albums_data_from_track_uri, scrape_album_data_from_album_uri_chunked
from scraping.org_info import scrape_org_info_from_track_uri ,  scrape_org_info_chunked

def test_one(uris): 
    # uri = '1lzr43nnXAijIGYnCT8M8H'
    id= '4wCmqSrbyCgxEXROQE6vtV'
    uri = 'spotify:track:4wCmqSrbyCgxEXROQE6vtV' 
    sp = refresh_spotify()
    print("ready")
    # results = sp.audio_features(uri)
    results = sp.track(uri)
    ipdb.set_trace() 
    # results = load_track_data(sp,uris[:5])
    track = results['tracks'][0]
    album = track['album']
    data = {
        'track_uri': uri, 
        'track_name': track['name'], 
        'album_uri': album['uri'], 
    }
    
    print(results)

def test_one_v2(): 
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    from pprint import pprint
    # import logging
    # logging.basicConfig(level='DEBUG')


    os.environ['SPOTIPY_CLIENT_ID'] = spotify_client_id
    os.environ['SPOTIPY_CLIENT_SECRET'] = spotify_client_secret
    print("Starting Credential Check ")
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials())
    print("Completed Credential Check")
    i = 0
    while True:
        # res = sp.audio_features('spotify:track:3HPHPoXFupNZXfnFXmiJI5')
        # print(res, str(i))
        sp.track('spotify:track:3HPHPoXFupNZXfnFXmiJI5')
        print(res['uri'], str(i))
        i += 1

def test_one_v3(): 
    import requests
    import urllib3
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    from spotipy.exceptions import SpotifyException
    import time

    session = requests.Session()
    retry = urllib3.Retry(
        respect_retry_after_header=False
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(),
                        requests_session=session)

    i = 0
    while True:
        try:
            res = sp.track('spotify:track:3HPHPoXFupNZXfnFXmiJI5')
            # res = sp.audio_features('spotify:track:3HPHPoXFupNZXfnFXmiJI5')
        except SpotifyException as e:
            if e.http_status == 429:
                print("'retry-after' value:", e.headers['retry-after'])
                time.sleep(1)
            else:
                break
       
        # print(res['uri'], str(i))
        print(res, str(i))
        exit() 
        i += 1

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def test_v4(): 
    import requests
    import urllib3
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    from spotipy.exceptions import SpotifyException
    import time

    session = requests.Session()
    retry = urllib3.Retry(
        respect_retry_after_header=False
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)

    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(),
                        requests_session=session)
    
    path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/OG/spotify-uris.tsv'
    b = time.time()
    data = pd.read_csv(path, delimiter = '\t', quoting = 3)
    a = time.time() 
    print("LOADING TOOK:{}".format(a-b))
    output_path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/Mus_Feat/org_info.pkl'
    uris = data.uri.to_list() 
    
    all_data = []
    batches = list(chunks(uris, 30))
    missed_tracks = []
     
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



def load_mus_feat(): 
    path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/OG/spotify-uris.tsv'
    b = time.time()
    data = pd.read_csv(path, delimiter = '\t', quoting = 3)
    a = time.time() 
    print("LOADING TOOK:{}".format(a-b))
    output_path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/Mus_Feat/uris_mus.pkl'
    uris = data.uri.to_list() 
    scrape_music_feature_data(uris, output_path)

# def load_album_feat(): 
#     path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/OG/spotify-uris.tsv'
#     b = time.time()
#     data = pd.read_csv(path, delimiter = '\t', quoting = 3)
#     a = time.time() 
#     print("LOADING TOOK:{}".format(a-b))
#     uris = data.uri.to_list() 
#     imgs_folder = '/home/mila/r/rebecca.salganik/scratch/2bLFM/album_artworks/'
#     df_path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/org_dfs/album_info.pkl'
#     scrape_albums_data_from_track_uri(uris, imgs_folder, df_path)

def load_org_info(): 
    path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/OG/spotify-uris.tsv'
    b = time.time()
    data = pd.read_csv(path, delimiter = '\t', quoting = 3)
    a = time.time() 
    print("LOADING TOOK:{}".format(a-b))
    output_path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/track_uris/'
    uris = data.uri.to_list() 
    scrape_org_info_chunked(uris, output_path)
    # test_one(uris)



def load_album_info(): 
    
    path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/org_dfs/album_uris.pkl'
    b = time.time()
    data = pickle.load(open(path, "rb"))
    a = time.time() 
    print("LOADING TOOK:{}".format(a-b))
    uris = data.album_uri.unique().tolist() 
    img_path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/album_artworks/'
    output_path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/img_org2/'
    scrape_album_data_from_album_uri_chunked(uris, img_path, output_path, batch_size = 10, offset = 0)

# def launch(): 
#     from scraping.org_info import load_org_info_chunked
#     load_org_info_chunked() 



# load_org_info()
# load_album_feat() 
# load_mus_feat() 


load_album_info()

# launch() 