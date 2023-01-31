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

from utils.private import * 
from scraping.music_features import scrape_music_feature_data 

def load_mus_feat(): 
    path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/OG/spotify-uris.tsv'
    b = time.time()
    data = pd.read_csv(path, delimiter = '\t', quoting = 3)
    a = time.time() 
    print("LOADING TOOK:{}".format(a-b))
    output_path = '/home/mila/r/rebecca.salganik/scratch/2bLFM/Mus_Feat/uris_mus.pkl'
    uris = data.uri.to_list() 
    scrape_music_feature_data(uris, output_path)
    
load_mus_feat() 