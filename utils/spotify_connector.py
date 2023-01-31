"""
All functionalities related to spotify data loading
"""

import os
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from utils.private import * 

os.environ['SPOTIPY_CLIENT_ID'] = spotify_client_id
os.environ['SPOTIPY_CLIENT_SECRET'] = spotify_client_secret


def refresh_spotify():
    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)
    return sp

def load_album_data(sp, batch, retry_num=10):
    if retry_num < 0:
        raise Exception('Error')

    retry_num -= 1
    try:
        results = sp.albums(batch)
        return results
    except Exception as e:
        print(e)
        sp = refresh_spotify()
        time.sleep(10)

    return load_album_data(sp, batch, retry_num)

def load_artist_data(sp, batch, retry_num=10):
    if retry_num < 0:
        raise Exception('Error')

    retry_num -= 1
    try:
        results = sp.artists(batch)
        return results
    except Exception as e:
        print(e)
        sp = refresh_spotify()
        time.sleep(10)

    return load_artist_data(sp, batch, retry_num)

def load_music_data(sp, batch, retry_num=10):
    if retry_num < 0:
        raise Exception('Error')

    retry_num -= 1
    try:
        results = sp.audio_features(tracks=batch)
        return results
    except Exception as e:
        print(e)
        sp = refresh_spotify()
        time.sleep(10)

    return load_music_data(sp, batch, retry_num)


def load_track_data(sp, batch, retry_num=10):
    if retry_num < 0:
        raise Exception('Error')

    retry_num -= 1
    try:
        results = sp.tracks(tracks=batch)
        return results
    except Exception as e:
        print(e)
        sp = refresh_spotify()
        time.sleep(10)

    return load_track_data(sp, batch, retry_num)




    