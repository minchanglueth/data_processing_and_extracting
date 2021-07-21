import time
from typing import List
import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
import random

# import unidecode
# import re
# from fuzzywuzzy import fuzz
#from support_function.text_similarity.text_similarity import get_token_set_ratio

def get_itunes_api_result(url: str) -> List[dict]:
    try:
        for _ in range(0, 2):
            response = requests.get(url)
            if response:
                response_map = response.json()
                results = response_map.get("results")
                sleep_time = random.uniform(0.5, 1)
                time.sleep(sleep_time)
                return results
    except Exception:
        logging.debug(f"Got error when calling url [{url}]")
    return []

def check_validate_albumitune(itune_album_id: str, itune_region: str = "us"):
    #     Step 1: check api
    api_url = f"http://itunes.apple.com/lookup?id={itune_album_id}&entity=song&country={itune_region}&limit=1000"
    results = get_itunes_api_result(api_url)
    full_api = []
    for result in results:
        wrapperType = result.get('wrapperType')
        full_api.append(wrapperType)
        full_api = list(set(full_api))
    if "collection" in full_api and "track" in full_api:
        return True
    else:
        #   Step 2: check web url
        web_url = f"https://music.apple.com/{itune_region}/album/{itune_album_id}"
        web_response = requests.get(web_url)
        if web_response:
            html_content = web_response.content
            soup = BeautifulSoup(html_content, 'html.parser')
            try:
                # check existed albumname = soup.title.text
                soup.title.text
                return True
            except AttributeError:
                return False
        else:
            return False

def check_validate_artistitune(itune_artist_id: str, itune_region: str = "us"):
    #     Step 1: check api
    api_url = f"http://itunes.apple.com/lookup?id={itune_artist_id}"
    results = get_itunes_api_result(api_url)
    full_api = []
    for result in results:
        wrapperType = result.get('wrapperType')
        full_api.append(wrapperType)
        full_api = list(set(full_api))
    if "artistName" in full_api and "artistId" in full_api:
        return True
    else:
        #   Step 2: check web url
        web_url = f"https://music.apple.com/{itune_region}/artist/{itune_artist_id}"
        web_response = requests.get(web_url)
        if web_response:
            html_content = web_response.content
            soup = BeautifulSoup(html_content, 'html.parser')
            try:
                # check existed albumname = soup.title.text
                soup.title.text
                return True
            except AttributeError:
                return False
        else:
            return False


def get_itune_id_region_from_itune_url(url: str):
    itune_id = url.split("/")[-1]
    itune_region = url.split("/")[3]
    if '?' in itune_id:
        itune_id = itune_id.split("?")[0]
    return [itune_id, itune_region]

def create_df_validate(df,column_ituneurl,column_new):
    df[column_new] = ''
    for ituneurl in df[column_ituneurl]:
        df[column_new].apply(lambda x: check_validate_artistitune(get_itune_id_region_from_itune_url(ituneurl)[0],get_itune_id_region_from_itune_url(ituneurl)[1]))
    return df

def get_dict(dict_name,function,position,df_column):
    dict_name = {}
    for ituneurl in df_column:
        itune_id_region = function(ituneurl)
        dict_name.update({ituneurl:itune_id_region[position]})
    return dict_name

def check_validate(original_df,check_validate_itune,column_ituneurl):
    original_df['itune_id'] = original_df[column_ituneurl].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[0])
    original_df['itune_region'] = original_df[column_ituneurl].apply(
        lambda x: get_itune_id_region_from_itune_url(url=x)[1])
    # original_df['check_validate'] = original_df[['itune_id','itune_region']].apply(
    #     lambda x: check_validate_itune(itune_artist_id=x['itune_id'], itune_region=x['itune_region']), axis=1)
    return original_df

# def get_max_ratio(itune_album_id: str, input_album_title: str):
#     album_title = get_album_title_artist(itune_album_id=itune_album_id)[0]
#     max_ratio = get_token_set_ratio(str1=input_album_title, str2=album_title)

#     if max_ratio != 100:
#         tracklist = list(get_tracklist_from_album_itune(itune_album_id=itune_album_id))
#         for song in tracklist:
#             song = song[0]
#             k = get_token_set_ratio(str1=song, str2=input_album_title)
#             if max_ratio >= k:
#                 max_ratio = max_ratio
#             else:
#                 max_ratio = k
#     return max_ratio

def get_album_title_artist(itune_album_id: str, itune_region: str = "us"):
    #     Step 1: check api
    api_url = f"http://itunes.apple.com/lookup?id={itune_album_id}&entity=song&country={itune_region}&limit=1000"
    results = get_itunes_api_result(api_url)
    full_api = []
    for result in results:
        wrapperType = result.get('wrapperType')
        full_api.append(wrapperType)
        full_api = list(set(full_api))
    album_info = []
    if "collection" in full_api and "track" in full_api:
        for result in results:
            if result.get('wrapperType') == "collection":
                album_title = result.get('collectionCensoredName')
                album_artist = result.get('artistName')
                album_info.append(album_title)
                album_info.append(album_artist)

    else:
        #   Step 2: check web url
        web_url = f"https://music.apple.com/{itune_region}/album/{itune_album_id}"
        web_response = requests.get(web_url)
        if web_response:
            html_content = web_response.content
            soup = BeautifulSoup(html_content, 'html.parser')
            try:
                album_title_tag = soup.find_all(id="page-container__first-linked-element")
                album_title = album_title_tag[0].text.strip()

                artist_album_tag = soup.find_all("div", {"class": "product-creator typography-large-title"})
                artist_album = artist_album_tag[0].text.strip()
                album_info.append(album_title)
                album_info.append(artist_album)
            except AttributeError:
                print(f"Got error when calling url:{web_url}")
            except IndexError:
                print(f"Got error when calling url:{web_url}")
            except:
                print(f"Got error when calling url:{web_url}")
        else:
            print(f"Got error when calling url:{web_url}")
    if not album_info:
        album_info = ["Itunes returns no result through look up api", "Itunes returns no result through look up api"]
    return album_info

