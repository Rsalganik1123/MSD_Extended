import pandas as pd 
import pickle 


    """
    given list of music features and corseness binning value, bin the continuous values and save 
    :param music_feature_path:  location of music feature csv 
    :param music_feature_list: list of musical features 
    :param save_path:   output save path
    :param bin_coarseness:   number of bins to allocate features into (quantile)
    """


def binning(music_feature_path, music_feature_list, save_path, bin_coarseness=3): 
    music_feat_all = pd.read_csv(music_feature_path, sep='\t')
    print("Loaded music features with format: ", music_feat_all.dtypes)
    print("Splitting into track_uris, music_feats and converting to float for binning")
    track_uris = music_feat_all[['track_uri']]
    music_feat_isolated = music_feat_all[music_feature_list].astype(float)
    # print("Music Features Isolated with format:", music_feat_isolated.dtypes)
    # assert len(track_uris) == len(music_feat_isolated)
    print("Sanity check: m_feat and track_uri length is equal:",len(track_uris) == len(music_feat_isolated)) 
    music_feat_binned = music_feat_isolated.apply(lambda x: pd.qcut(x, bin_coarseness, labels=list(range(1,4))), axis=0).copy()
    print("Music Features Binned with format:", music_feat_binned.dtypes)
    print("Adding track_uris back")
    music_feat_binned['track_uri'] = list(track_uris['track_uri'])
    print("Final Binned Dataframe Columns:", music_feat_binned.dtypes)
    pickle.dump(music_feat_binned , open(save_path, 'wb'))
    print("Saved to:", save_path)

binning('/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/spotify_in_csv/music_features.csv', 
    ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo'],
    '/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/binned_music_feat.pkl')

# music_feat_all = pd.read_csv('/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/spotify_in_csv/music_features.csv', sep='\t')
# print("Loaded music features with format: ", music_feat_all.dtypes)
# track_uris = music_feat_all[['track_uri']]
# music_feat_isolated = music_feat_all[['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']].astype(float)
# print("Music Features Isolated with format:", music_feat_isolated.dtypes)
# print("Sanity check: m_feat and track_uri length is equal:",len(track_uris) == len(music_feat_isolated)) 
# music_feat_binned = music_feat_isolated.apply(lambda x: pd.qcut(x, 3, labels=list(range(1,4))), axis=0).copy()
# print("Music Features Binned with format:", music_feat_binned.dtypes)

# music_feat_df = music_feat_df.apply(lambda x: pd.qcut(x, 3, labels=list(range(1,4))), axis=0).copy()
# with open('/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/pid_tid_arid_alid_join.pkl', "rb") as f: 
#     print("opened file")
#     all_meta_info = pickle.load(f)
#     print("Loaded meta info with format: ", type(all_meta_info))

