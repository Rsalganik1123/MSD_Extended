import pandas as pd 
from ast import literal_eval
from collections import Counter 
from sklearn.decomposition import PCA
from tqdm import tqdm 
import pickle 

#load artists 
#load all genres 
#different functions for selecting genres: 
    #common genres 
    #top 20 most common genres 
    #PCA based 

def genre_tag_dist(data, k):
    g = list(data['genres'])
    g = [literal_eval(v) for v in g]
    all_tags = [item for sublist in g for item in sublist]
    c = Counter(all_tags).most_common(k)
    genres, count = [v[0] for v in c], [v[1] for v in c]
    return genres, count, c

def filter_top_k(data, k): 
    unique_tags = data["genres"].explode().unique() 
    all_tags = data['genres'].explode()
    top_genres = dict(Counter(all_tags).most_common(k)).keys() 
    print("Top", k, "genres:", top_genres) 
    for g in top_genres: 
        data[g] = data.genres.apply(lambda x: g in x)
    return data 

def remove_least_popular_values(data, min_threshold=0): 
    all_tags = list(data['genres'].explode())
    genre_counts = Counter(all_tags).most_common()
    genre_counts = dict(genre_counts)
    other_count = 0 
    for key, cnts in list(genre_counts.items()):   # list is important here
        if cnts < min_threshold:
            other_count += cnts
            del genre_counts[key]
    genre_counts['other'] = other_count
    return genre_counts
    
def all_genres(data): 
    genre_counts = remove_least_popular_values(data)
    all_tags = list(genre_counts.keys())  
    genre_cols = {}
    
    for g in tqdm(all_tags): 
        genre_cols[g] = data.genres.str.contains(g)
        
    data = pd.concat([data.iloc[:, :-1], pd.DataFrame(genre_cols)], axis=1)
    print(data.columns)
    return data 

def filter_PCA(data): 
    for comp in range(3, data.shape[1]):
        pca = PCA(n_components= comp, random_state=42)
        pca.fit(data)
        comp_check = pca.explained_variance_ratio_
        final_comp = comp
        if comp_check.sum() > 0.85:
            break
        
    Final_PCA = PCA(n_components= final_comp,random_state=42)
    Final_PCA.fit(data)
    cluster_df=Final_PCA.transform(data)
    print(cluster_df.columns)

    # num_comps = comp_check.shape[0] 
    

    # for comp in range(3, df3.shape[1]):
    # pca = PCA(n_components= comp, random_state=42)
    # pca.fit(df3)
    # comp_check = pca.explained_variance_ratio_
    # final_comp = comp
    # if comp_check.sum() > 0.85:
    #     break
        
    # Final_PCA = PCA(n_components= final_comp,random_state=42)
    # Final_PCA.fit(df3)
    # cluster_df=Final_PCA.transform(df3)
    # num_comps = comp_check.shape[0]
    # print("Using {} components, we can explain {}% of the variability in the original data.".format(final_comp,comp_check.sum()))
 

def filter_surface_genres(): 
    surface_genres = ['rock', 'classical', 'hip hop',
       'rap', 'country', 'edm', 'folk', 'pop', 'soul', 'latin', 'jazz'] 

def string_list(data): 
    data = literal_eval(data)
    if not data: 
        return ['empty']
    else: return data

artists = pd.read_csv('/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/spotify_in_csv/artist_features.csv', sep='\t')
artists['genres'] = artists.apply(lambda row: string_list(row['genres']), axis=1)
print(artists)

# artists.to_csv('/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/spotify_in_csv/artist_features2.csv', sep='\t')
# print(artists.columns)
# artists.genres = artists.genres.fillna('empty')
# print(type(artists.genres[298854]), not artists.genres[298854])
# print(artists.genres)

# artists_all_genre = all_genres(artists)
# save_path = '/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/artist_features_all_genres.pkl'
# pickle.dump(artists_all_genre, open(save_path, 'wb'))

artists_top_genre = filter_top_k(artists, 20)
save_path = '/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/artist_features_top_20_genres.pkl'
pickle.dump(artists_top_genre, open(save_path, 'wb'))

def launch(output_path, input_path, encoding, k):
    if encoding == 'all_genres': 
        artists = pd.read_csv('/home/mila/r/rebecca.salganik/scratch/MusicSAGE_Data/OG_CSV/spotify_in_csv/artist_features.csv', sep='\t')
        artists['genres'] = artists.apply(lambda row: string_list(row['genres']), axis=1)
        artists = all_genres(artists)
        # with open (input_path, "rb") as f: 
        #     artists = pickle.load(f)
    if encoding == 'topk': 
        artists = pd.read_csv(input_path, sep='\t')
        artists['genres'] = artists.apply(lambda row: string_list(row['genres']), axis=1)
        artists = filter_top_k(artists, k)
    if encoding == 'PCA': 
        with open (output_path, "rb") as f: 
            artists = pickle.load(f)
            #Send to PCA 
    pickle.dump(artists, open(output_path, 'wb'))

