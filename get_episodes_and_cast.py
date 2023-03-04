# -*- coding: utf-8 -*-
"""
get_episodes_and_cast.py

Retrieves episode and cast data from IMDB and outputs to csv files.

Outputs:
  - episodes.csv = list of episodes
  - episode_roles.csv = list of individual roles in each episode

Author: Kelly Gilbert
Created: 2023-03-03
"""


import imdb
import pandas as pd


ia = imdb.Cinemagoer()


# get the series ID
series_id = ia.search_movie('one tree hill')[0].movieID


# --------------------------------------------------------------------------------------------------
# retrieve the episodes
# --------------------------------------------------------------------------------------------------

series = ia.get_movie(series_id)
ia.update(series, info=['episodes', 'full_credits'])
eps = series['episodes']
           
key_list = ['season', 'episode', 'title', 'episode', 'original air date', 'plot', 'rating', 'votes']
df_eps = pd.concat([pd.DataFrame({k:[v] for k, v in eps[s][e].items() if k in key_list} 
                                 | {'movieID' : [eps[s][e].movieID]}) 
                    for s in eps.keys()
                    for e in eps[s].keys()],
                   ignore_index=True)


df_eps.to_csv(r'C:\users\gilbe\projects\othk\episodes.csv', index=False)


# --------------------------------------------------------------------------------------------------
# retrieve the cast for each episode
# --------------------------------------------------------------------------------------------------

df_roles = None
for s, e, movieID in zip(df_eps['season'], df_eps['episode'], df_eps['movieID']):

    # get the episode full cast (in this case, the "movie" is the episode)
    ep = ia.get_movie(movieID)
    ia.update(ep, 'full_credits')
    cast = ep['cast']
    
    # fiterate through the cast members
    for c in cast:
        if isinstance(c.currentRole, list):
            roles = c.currentRole
        else:
            roles = [c.currentRole]
        
        # add each role to the dataframe
        df_roles = pd.concat([df_roles,
                              pd.DataFrame({'season' : s * len(roles),
                                            'episode' : [e] * len(roles),
                                            'characterID' : [r.characterID for r in roles],
                                            'role_name' : [r['name'] for r in roles],
                                            'actor_name' : [c['name']] * len(roles),
                                            'personID' : [c.personID] * len(roles) })],
                             ignore_index=True)
        
    
df_roles.to_csv(r'C:\users\gilbe\projects\othk\episode_roles.csv', index=False)
