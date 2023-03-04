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

# read in the sheet data
sheet = client.open(WORKBOOK_NAME).worksheet('data')
df = pd.DataFrame(sheet.get_all_records())


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




