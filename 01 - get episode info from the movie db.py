# -*- coding: utf-8 -*-
"""
Step 01 - pull episode and cast data from The Movie Database API

Author: Kelly Gilbert
Created: 2022-12-28

Requirements:
  - API Token for The Movie Database API (enter in constants, below)
    Instructons: https://developers.themoviedb.org/3/getting-started/introduction
    
    
"""

import json
import pandas as pd
import requests
import sys
from urllib import parse


# --------------------------------------------------------------------------------------------------
# constants
# --------------------------------------------------------------------------------------------------

API_TOKEN = 'enter token here'

# --------------------------------------------------------------------------------------------------
# functions
# --------------------------------------------------------------------------------------------------

def get_show_id(search_string, api_token):
    """
    Returns the ID associated with the search string.
    """
    
    search_url = 'https://api.themoviedb.org/3/search/tv?language=en-US&page=1' \
                 + f'&query={parse.quote(search_string)}&include_adult=false'
                    
    r_search = requests.get(url=search_url,
                            headers={'Authorization': 'Bearer ' + api_token,
                                     'Content-Type': 'application/json;charset=utf-8'})
    
    if r_search.status_code != 200:
        print(f'ERROR: error finding ID for search string "{search_string}"\n    {r_search.text}')
    
    else:
        search_results = json.loads(r_search.text)['results']
        if len(search_results) > 1:
            print('ERROR: search returned more than one result')
            for i in search_results:
                print(f"    ID {i['id']} : {i['name']}")
            sys.exit(0)      
            
        else:
            print(f"The ID is {(show_id:=search_results[0]['id'])}")
            
        return show_id

        
# --------------------------------------------------------------------------------------------------
# get the seasons and episode counts
# --------------------------------------------------------------------------------------------------

show_id = get_show_id('One Tree Hill', API_TOKEN) 


url = f'https://api.themoviedb.org/3/tv/{show_id}&language=en-US'
r_series = requests.get(url=url,
                 headers={'Authorization': 'Bearer ' + API_TOKEN,
                          'Content-Type': 'application/json;charset=utf-8'})

if r_series.status_code != 200:
    print(f'ERROR: could not get series info.\n    {r_series.text}')
    sys.exit(0)
else:
    series_json = json.loads(r_series.text)
    episode_counts = {s['season_number'] : s['episode_count'] for s in series_json['seasons']}


# --------------------------------------------------------------------------------------------------
# get the episode details
# --------------------------------------------------------------------------------------------------

cols = ['season_number', 'episode_number', 'air_date', 'name', 'overview', 'id', 'runtime']
cast_cols = ['season_number', 'episode_number', 'id', 'name', 'character', 'type']

df_episodes = None
df_cast = None

for s, c in episode_counts.items():
    for e in range(1, c+1):
        url = f'https://api.themoviedb.org/3/tv/{show_id}/season/{s}/episode/{e}?append_to_response=credits&language=en-US'
        r_ep = requests.get(url=url,
                            headers={'Authorization': 'Bearer ' + API_TOKEN,
                                     'Content-Type': 'application/json;charset=utf-8'})
        
        if r_ep.status_code != 200:
            print(f'ERROR: season {s}, episode {e}: {r_ep.text}')
            continue
        else:
            # episode info
            df_episodes = pd.concat([df_episodes, 
                                     pd.json_normalize(json.loads(r_ep.text))[cols]])
            
            
            # cast and guest stars
            df_temp_cast = ( pd.json_normalize(json.loads(r_ep.text),
                                               record_path=['credits','cast'],
                                               meta=['season_number', 'episode_number'])
                               .assign(type='cast') )

            df_temp_gs = ( pd.json_normalize(json.loads(r_ep.text),
                                             record_path=['credits','guest_stars'],
                                             meta=['season_number', 'episode_number'])
                             .assign(type='guest stars') )
            
            df_cast = ( pd.concat([df_cast, 
                                   df_temp_cast[[f for f in df_temp_cast.columns if f in cast_cols]], 
                                   df_temp_gs[[f for f in df_temp_gs.columns if f in cast_cols]]])
                          .reset_index(drop=True) )


# --------------------------------------------------------------------------------------------------
# output to csv
# --------------------------------------------------------------------------------------------------
    
df_episodes.to_csv(r'C:\Users\gilbe\projects\othk\episode_details.csv', index=False)    
df_cast.to_csv(r'C:\Users\gilbe\projects\othk\episode_cast.csv', index=False)    
