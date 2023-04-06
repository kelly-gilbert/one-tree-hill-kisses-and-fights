# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 16:45:30 2023

@author: gilbe
"""


# !pip install gspread
# !pip install google-api-python-client oauth2client

import gspread
import numpy as np
from oauth2client import service_account
from os import path
import pandas as pd


# --------------------------------------------------------------------------------------------------
# constants
# --------------------------------------------------------------------------------------------------

WORKBOOK_NAME = 'othk'
SHEET_NAME = 'data'
SCOPE = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
CREDS_PATH = creds_path
INPUT_PATH = r'.\inputs'
OUTPUT_PATH = r'.\outputs'


renames = { 'Alice Day' : 'Alice',
            'Bevin Mirskey' : 'Bevin',
            'Billy' : 'Billy (Drug Dealer)',
            'Drug Dealer' : 'Billy (Drug Dealer)',
            'Daunte Jones' : 'Daunte',
            'Derek Sommers' : 'Real Derek', 
            'Derek' : 'Psycho Derek',
            'Edwards' : 'Jimmy',
            'Ian Banks' : 'Psycho Derek',
            'Jimmy Edwards' : 'Jimmy',
            'Larry Sawyer' : 'Larry Sawyer',
            'Nick Chavez' : 'Nick (teacher)',
            'Kylie Frost' : 'Kylie', 
            'Nick Lachey' : 'Nick Lachey',
            'Millicent' : 'Millie', 
            'Peyton Scott' : 'Peyton',
            'Sara Evans' : 'Sara',
            'Tim Smith' : 'Tim',
            'Young Dan' : 'Young Dan',
            'Young Keith' : 'Young Keith'}


# --------------------------------------------------------------------------------------------------
# prep the event data
# --------------------------------------------------------------------------------------------------

# add credentials to the account and authorize the client sheet
creds = service_account.ServiceAccountCredentials.from_json_keyfile_name(CREDS_PATH, SCOPE)
client = gspread.authorize(creds)


# read in the event data
sheet = client.open(WORKBOOK_NAME).worksheet('data')
df_event_raw = pd.DataFrame(sheet.get_all_records())

df_event_raw['main_type'] = df_event_raw['type'].str.extract('(.*?)(?:$|\s)', expand=False)


# normalize the names from event data
person_cols = ['person_1', 'person_2']
df_event_raw[person_cols] = df_event_raw[person_cols].replace(renames)


# sort the person names alphabetically and create the path
person_values = np.array(df_event_raw.loc[:, person_cols])
person_values.sort(axis=1)
df_event_raw[person_cols] = person_values
df_event_raw['path'] = ['|' + '|'.join(p) + '|' for p in person_values]


# create one record for each person by duplicating the dataset
df_event = pd.concat([df_event_raw.assign(person_nbr=1),
                      df_event_raw.assign(person_nbr=2)
                                  .rename(columns={'person_1' : 'person_2',
                                                   'person_2' : 'person_1' })])


# create a list of partners for each main type
df_event['all_partners'] = '|' + (df_event.groupby(['person_1', 'main_type'])
                                      ['person_2'].transform(lambda x: '|'.join(set(x)))) + '|'


# --------------------------------------------------------------------------------------------------
# normalize and check role names from imdb data
# --------------------------------------------------------------------------------------------------

# read in the cast data
df_role = pd.read_csv(path.join(OUTPUT_PATH, 'episode_roles.csv'))


# clean the names from imdb data
#     replace 'self' with the actor name
#     extract nicknames (e.g. Mouth)
#     if the character made at least 5 appearances, split out the first name
df_role['role_name_clean'] = np.where(df_role['role_name'].isin(['Themselves', 'Self']),
                                      df_role['actor_name'],
                             np.where(df_role['role_name'].str.contains("'.*'"),
                                      df_role['role_name'].str.extract(".* '(.*)' .*", 
                                                                      expand=False),
                             np.where(df_role['role_name'].isin(renames.keys()),
                                      df_role['role_name'].replace(renames),
                             np.where(df_role.groupby('role_name')['role_name']
                                             .transform('count') >= 5,
                                      df_role['role_name'].str.extract('(.*?)(?: |$).*', 
                                                                       expand=False),
                                      df_role['role_name']))))


# count episodes by character
# ensure all episodes with an event are captured, even if the role wasn't credited
df_all_eps = ( df_event[~df_event['type'].str.contains('repeat')]
                       [['season', 'episode', 'person_1']].drop_duplicates() 
                  .merge(df_role[['season', 'episode', 'role_name_clean']].drop_duplicates(),
                         how='left',
                         left_on=['season', 'episode', 'person_1'],
                         right_on=['season', 'episode', 'role_name_clean']) )


# check for role names that exist in events, but didn't match to role data
#     this could be due to name mismatches between the event and role data (add to renames dict)
#     this could also be due to uncredited roles (e.g. a flashback to a previous episode - do nothing)
if not (missing := ( df_all_eps[df_all_eps['role_name_clean'].isna()]
                        [['season', 'episode', 'person_1']]
                        .drop_duplicates() )).empty:
    print('The following roles had events, but did not match to role data' + chr(10))
    print(missing.assign(s_e = missing['season'].astype(str) + '-' + missing['episode'].astype(str))
                 .groupby(['person_1'], as_index=False)
                 .agg(episodes = ('s_e', list),
                      count = ('season', 'count'))
                 .sort_values('count', ascending=False))
    print(chr(10)*2)


# add the episode count by role
df_ep_counts = ( df_all_eps.groupby(['role_name_clean', 'season'], as_index=False)
                           .agg(role_season_ep_count = ('episode', 'count'))
                           .assign(role_ep_count = lambda df_x: 
                                                       df_x.groupby(['role_name_clean'], as_index=False)
                                                           ['role_season_ep_count'].transform('sum')) )


df_out = ( df_event.merge(df_ep_counts,
                          how='left',
                          left_on=['person_1', 'season'],
                          right_on=['role_name_clean', 'season'])
                   .drop(columns=['role_name_clean']) )


# export the data
df_out.to_csv(path.join(OUTPUT_PATH, 'event-person.csv'), index=False)


# --------------------------------------------------------------------------------------------------
# output nodes and edges for building the network graph in various tools
# --------------------------------------------------------------------------------------------------

# count of events by path
df_ntwk = ( df_out.groupby(['person_1', 'person_nbr', 'path'], as_index=False)
                  ['type'].count() 
                  .rename(columns={ 'type' : 'count'}) )

df_ntwk['partner_count'] = df_ntwk.groupby('person_1')['path'].transform('nunique')

df_ntwk[['path_p1', 'path_p2']] = df_ntwk['path'].str.extract('\|(.*)\|(.*)\|')


# remove roles that only have each other as partners (except Miranda and Grubbs)
person_dict = ( df_ntwk[['person_1', 'partner_count']]
                    .drop_duplicates()
                    .set_index('person_1') 
                    .squeeze()
              ).to_dict()
person_dict['Miranda'] = 5
person_dict['Grubbs'] = 5

df_ntwk['p1_partners'] = df_ntwk['path_p1'].replace(person_dict)
df_ntwk['p2_partners'] = df_ntwk['path_p2'].replace(person_dict)

df_ntwk = df_ntwk.query("(p1_partners > 1) | (p2_partners > 1)")


# output for gephi
( df_ntwk.groupby(['person_1'], as_index=False)
      ['path'].count()
     .rename(columns={'path' : 'size',
                      'person_1' : 'label'})
     .assign(id = lambda df_x: df_x['label']) 
     .to_csv(path.join(OUTPUT_PATH, 'geph_input_nodes.csv'), index=False) )

( df_ntwk[df_ntwk['person_nbr']==1]['path'].str.extract('\|(.*)\|(.*)\|')
     .assign(Weight=1)
     .rename(columns={0 : 'Source', 1 : 'Target'})
     .groupby(['Source', 'Target'], as_index=False)
     ['Weight'].sum()
     .to_csv(path.join(OUTPUT_PATH, 'gephi_input_edges.csv'), index=False) )
 


# json output
df_sum = ( df_ntwk.groupby('person_1', as_index=False)
              ['path'].count()
              .rename(columns={'path' : 'count'}) )
    
nodes = (',' + chr(10)).join(['      { "id" : "' + n + '", "size" : "' + str(s) + '" }'
                              for n, s 
                              in zip(df_sum['person_1'], df_sum['count'])])

links = (',' + chr(10)).join(['      { "source" : "' + st[0] + '", "target" : "' + st[1] + '" }'
                              for st in ( df_ntwk['path'].drop_duplicates().str.strip('|')
                                                                           .str.split('|') )])

out = """{ 
   "nodes" : [
       """ + nodes + """
   ],
   "links" : [
       """ + links + """
   ]
}"""
   
with open(path.join(OUTPUT_PATH, 'json_network.json'), "w") as f:
    f.write(out)

