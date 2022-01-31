import pandas as pd 
from utils.private import * 
from scraping.music_features import scrape_music_feature_data
from scraping.album_artwork import scrape_albums_data 
from scraping.artist_features import scrape_artist_data 

def scraper(data_path, uri_key, output_path, mode): 
    data = pd.read_csv(OG_CSV_data_path+data_path, sep='\t')
    print("Loaded data for {} with length:{}".format(mode, len(data)))
    uri = data[uri_key]
    print("Run {} Extraction")
    if mode == "album artwork": 
        return 0 
    if mode == "music features": 
        all_data, missed_data = scrape_music_feature_data(uris, save_path+output_path)
    if mode == 'artist features': 
        all_data, missed_data = scrape_artist_data(uris, save_path+output_path)

    print("Saved data of length:{} , missing track count:{}".format(len(missed_tracks))) 



scraper(data_path = 'tracks.csv', uri_key='track_uri', output_file='music_features.pkl', mode="music features")

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