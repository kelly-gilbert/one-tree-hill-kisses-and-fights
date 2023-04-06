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


from csv import QUOTE_ALL
import imdb
import pandas as pd


ia = imdb.Cinemagoer()


# get the series ID
series_id = ia.search_movie('one tree hill')[0].movieID


# --------------------------------------------------------------------------------------------------
# retrieve basic info about each episode
# --------------------------------------------------------------------------------------------------

series = ia.get_movie(series_id)
ia.update(series, info=['episodes'])
eps = series['episodes']
           
key_list = ['season', 'episode', 'title', 'episode', 'original air date', 'plot', 'rating', 'votes']
df_eps = pd.concat([pd.DataFrame({k:[v.strip()] for k, v in eps[s][e].items() if k in key_list} 
                                 | {'movieID' : [eps[s][e].movieID]}) 
                    for s in eps.keys()
                    for e in eps[s].keys()],
                   ignore_index=True)


# --------------------------------------------------------------------------------------------------
# retrieve the cast for each episode
# --------------------------------------------------------------------------------------------------

df_roles = None
df_runtime = None
for s, e, movieID in zip(df_eps['season'], df_eps['episode'], df_eps['movieID']):
  
    # get the episode info with full cast (in this case, the "movie" is the individual episode)
    ep = ia.get_movie(movieID)
    ia.update(ep, 'full_credits')
    cast = ep['cast']
    
    
    # retrieve the runtime per episode
    df_runtime = pd.concat([df_runtime, 
                            pd.DataFrame({'season' : [s],
                                          'episode' : [e],
                                          'runtime_min' : [ep['runtime'][0]]})] )  
    
    
    # iterate through the cast members and roles
    # occasionally, an actor may play multiple roles in an episode
    for c in cast:
        if isinstance(c.currentRole, list):
            roles = c.currentRole
        else:
            roles = [c.currentRole]
        
        # add each role to the dataframe
        df_roles = pd.concat([df_roles,
                              pd.DataFrame({'season' : [s] * len(roles),
                                            'episode' : [e] * len(roles),
                                            'characterID' : [r.characterID for r in roles],
                                            'role_name' : [r['name'] for r in roles],
                                            'actor_name' : [c['name']] * len(roles),
                                            'personID' : [c.personID] * len(roles) })],
                             ignore_index=True)


# add the runtime to the episodes dataframe
df_eps = df_eps.merge(df_runtime,
                      how='left',
                      on=['season', 'episode'])


# --------------------------------------------------------------------------------------------------
# output the data
# --------------------------------------------------------------------------------------------------

df_eps.to_csv(r'C:\users\gilbe\projects\othk\data\outputs\episodes.csv', 
              index=False, 
              quoting=QUOTE_ALL)

df_roles.to_csv(r'C:\users\gilbe\projects\othk\data\outputs\episode_roles.csv', 
                index=False, 
                quoting=QUOTE_ALL)
