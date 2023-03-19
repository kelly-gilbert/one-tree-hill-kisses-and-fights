# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 16:45:30 2023

@author: gilbe
"""


# !pip install gspread
# !pip install google-api-python-client oauth2client

import gspread
from numpy import where
from oauth2client import service_account
from os import path
import pandas as pd


# --------------------------------------------------------------------------------------------------
# constants
# --------------------------------------------------------------------------------------------------

WORKBOOK_NAME = 'othk'
SHEET_NAME = 'data'
SCOPE = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
CREDS_PATH = r'C:\Users\gilbe\.google\valiant-house-371815-dd08553e4e96.json'
OUTPUT_PATH = r'C:\Users\gilbe\projects\othk'


# --------------------------------------------------------------------------------------------------
# read in the sheet data
# --------------------------------------------------------------------------------------------------

# add credentials to the account and authorize the client sheet
creds = service_account.ServiceAccountCredentials.from_json_keyfile_name(CREDS_PATH, SCOPE)
client = gspread.authorize(creds)


# read in the event data
sheet = client.open(WORKBOOK_NAME).worksheet('data')
df = ( pd.DataFrame(sheet.get_all_records())
         .pipe(lambda df_x: 
                   df_x.melt(id_vars=[c for c in df_x if 'person' not in c], 
                             value_vars=['person_1', 'person_2'], 
                             var_name='person_nbr', 
                             value_name='person_name', 
                             ignore_index=True)) )


# read in the episode and cast data
df_ep = pd.read_csv(path.join(path.join(OUTPUT_PATH, 'data'), 'episodes.csv'))
df_roles = pd.read_csv(path.join(path.join(OUTPUT_PATH, 'data'), 'episode_roles.csv'))


# --------------------------------------------------------------------------------------------------
# normalize and check role names
# --------------------------------------------------------------------------------------------------

renames = { 'Millicent' : 'Millie', 
            'Derek Sommers' : 'Real Derek', 
            'Derek' : 'Psycho Derek',
            'Edwards' : 'Jimmy',
            'Ian Banks' : 'Psycho Derek'}

# characters with a small number of appearances who should have a split name
split_exceptions = ['Bevin Mirskey',
                    'Chuck Skolnik',
                    'Cooper Lee',
                    'Damien West',
                    'Daunte Jones',
                    'David Fletcher',
                    'Ellie Harp',
                    'Erica Marsh',
                    'Ian Kellerman',
                    'Jimmy Edwards',
                    'Kylie Frost',
                    'Larry Sawyer',
                    'Renee Richardson',
                    'Peyton Scott',
                    'Sara Evans',
                    'Shelley Simon',
                    'Taylor James',
                    'Ted Davis',
                    'Tim Smith']


# clean the names from event data
df['join_name'] = df['person_name'].replace(renames)


# clean the names from imdb data
#     replace 'self' with the actor name
#     extract nicknames (e.g. Mouth)
#     if the role appeared at least 10 times, split out the first name
df_roles['join_name'] = ( pd.Series(where(df_roles['role_name'].isin(['Themselves', 
                                                                      'Self']),
                                          df_roles['actor_name'],
                                    where(df_roles['role_name'].str.contains("'.*'"),
                                          df_roles['role_name'].str.extract(".* '(.*)' .*", 
                                                                            expand=False),
                                    where((df_roles.groupby('role_name')['role_name']
                                                   .transform('count') >= 10)
                                              | df_roles['role_name'].isin(split_exceptions),
                                          df_roles['role_name'].str.extract('(.*?)(?: |$).*', 
                                                                            expand=False),
                                          df_roles['role_name']) )))
                            .replace(renames) )



# check for names in events, not in roles
df_check = ( df['join_name'].value_counts().reset_index(name='join_name')
               .merge(df_roles['join_name'].value_counts().reset_index(),
                      how='left',
                      on='index',
                      suffixes=['_event', '_role'])
               .query("join_name_role != join_name_role") )

print(chr(10)*2 + 'The following roles are in the event data, but did not join to the imdb data:' + chr(10))
print(df_check)


# check for same actor, multiple role names
#    if it's the same role spelled different ways, make sure the join_name is the same
df_check = ( df_roles.drop_duplicates(subset=['actor_name', 'join_name'])
                     .groupby(['actor_name']).agg(count=('join_name', 'count'),
                                                  list=('join_name', list))
                     .query("count > 1")
                     .sort_values('count', ascending=True) )

i = 0
while i < len(df_check)-1:
    print(df_check[i:i+10])
    i += 10


df_roles[df_roles['role_name'].str.contains('Edwards')]
,,df[df['join_name'].str.contains('Jason')]




# add the partner count for each person
partner_count = dict(pd.concat([df[['person_1', 'person_2']], 
                                df[['person_2', 'person_1']].set_axis(['person_1', 'person_2'], axis=1)])
                       .groupby('person_1')['person_2'].nunique())
df['partner_count_1'] = df['person_1'].replace(partner_count)
df['partner_count_2'] = df['person_2'].replace(partner_count)


# read in the episode cast
df_cast = pd.read_csv(r'.\episode_cast.csv')
df_cast['character_adj'] = df_cast['character'].str.extract('(.*?)\s.*')

df_characters = ( df.melt(id_vars=['season', 'episode'], 
                          value_vars=['person_1', 'person_2'])
                    .drop(columns='variable')
                    .drop_duplicates() )
df2 = ( df_characters.merge(df_cast, left_on=['season', 'episode', 'value'],
                            right_on=['season_number', 'episode_number', 'character'],
                            how='left', indicator=True)
                     [['season', 'episode', 'value', '_merge']]
                     .rename(columns={'_merge' : '_merge_full'})
                     .merge(df_cast, left_on=['season', 'episode', 'value'],
                            right_on=['season_number', 'episode_number', 'character_adj'],
                            how='left', indicator=True) )

df2['matched_flag'] = where((df2['_merge_full']=='both') | (df2['_merge']=='both'), 1, 0)
df2[df2['matched_flag'] != 1].iloc[0:30]

df


from os import chdir
chdir(r'C:\users\gilbe\projects\othk')



df2 = ( df.groupby(['person_1', 'person_2'], as_index=False)['time'].count()
          .assign(count1=lambda df_x: df_x['person_1'].replace(partner_count),
                  count2=lambda df_x: df_x['person_2'].replace(partner_count))
          .query('count1 > 1 | count2 > 1 | person_1 == "Grubbs" | person_2 == "Grubbs"') )


df_cast.query('season_number==1 & episode_number==1')[['character', 'type']]

df_cast.query('character=="Mia Catalano"')





df.columns


# total count of characters



!pip install gephistreamer
#!pip install pyvis
import networkx as nx
from pyvis.network import 


# keep pairings where at least one person had more than one partner
partner_count = dict(pd.concat([df[['person_1', 'person_2']], 
                                df[['person_2', 'person_1']].set_axis(['person_1', 'person_2'], axis=1)])
                       .groupby('person_1')['person_2'].nunique())

df2 = ( df.groupby(['person_1', 'person_2'], as_index=False)['time'].count()
          .assign(count1=lambda df_x: df_x['person_1'].replace(partner_count),
                  count2=lambda df_x: df_x['person_2'].replace(partner_count))
          .query('count1 > 1 | count2 > 1 | person_1 == "Grubbs" | person_2 == "Grubbs"') )





G = nx.from_pandas_edgelist(df2, source='person_1', target='person_2', edge_attr='time')
nx.draw(G)
nx.draw_planar(G)
nx.draw_kamada_kawai(G, with_labels=True)
nx.draw_spring(G, with_labels=True)
nx.draw_circular(G)
nx.draw_shell(G)
nx.draw_spectral(G)
nx.draw_random(G)


net = Network(notebook=True)
net.from_nx(G)
net.show(r'C:\projects\othk\test.html')




