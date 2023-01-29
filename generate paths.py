# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 10:26:33 2022

@author: gilbe
"""


from numpy import arccos, arcsin, cos, sin, pi, sqrt
import pandas as pd


# --------------------------------------------------------------------------------------------------
# constants
# --------------------------------------------------------------------------------------------------

WORKBOOK_NAME = 'othk'
SHEET_NAME = 'Sheet1'
SCOPE = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
CREDS_PATH = r'C:\Users\gilbe\.google\valiant-house-371815-dd08553e4e96.json'
OUTPUT_PATH = r'C:\Users\gilbe\projects\othk'


# --------------------------------------------------------------------------------------------------
# functions
# --------------------------------------------------------------------------------------------------

def generate_radial_geojson(node_count, curve_depth, output_path):
    """
    outputs a file containing line objects for paths between each origin/destination pair for n nodes.
    """
    
    df = pd.DataFrame({'node_count' : [n] * n, 'node_id' : range(1, n + 1)})
    
    # add coordinates for each item (rank 1 is at 10:30, and coordinates move clockwise)
    df['angle'] = (df['node_id'] - 1 - n) / n * 2 * pi - 1/4 * pi
    df['x'] = sin(df['angle'])
    df['y'] = cos(df['angle'])
    
    
    # create combos of to/from IDs, create 101 rows per combo to create smooth chords
    cols = ['node_id', 'x', 'y']
    df_combos = ( df[['node_count'] + cols].merge(df[cols], how='cross', suffixes=['1', '2'])
                      .query('node_id1 != node_id2')
                      .merge(pd.DataFrame(range(0, 101), columns=['point_nbr']), how='cross')
                      .rename(columns={'node_id1' : 'origin', 'node_id2' : 'destination' }) )
    
    
    # used to find the location of the midpoint of the arc
    df_combos['segment_length'] = sqrt((df_combos['x2'] - df_combos['x1'])**2 + (df_combos['y2'] - df_combos['y1'])**2)
    df_combos['angle_x'] = cos((arccos(df_combos['x1']) + arccos(df_combos['x2'])) / 2)
    df_combos['angle_y'] = sin((arcsin(df_combos['y1']) + arcsin(df_combos['y2'])) / 2)
    
    
    # arc coordinates
    df_combos['x'] = (( (1 - df_combos['point_nbr']/100)**2 ) * df_combos['x1']) \
                     + (2 * (1 - df_combos['point_nbr']/100) * df_combos['point_nbr']/100 * \
                         ( \
                           ((2 - df_combos['segment_length']) / curve_depth) * \
                           df_combos['angle_x'] \
                         ) \
                       ) \
                     + ((df_combos['point_nbr']/100)**2 * df_combos['x2'])
    
    
    df_combos['y'] = (( (1 - df_combos['point_nbr']/100)**2 ) * df_combos['y1']) \
                     + (2 * (1 - df_combos['point_nbr']/100) * df_combos['point_nbr']/100 * \
                         ( \
                           ((2 - df_combos['segment_length']) / curve_depth) * \
                           df_combos['angle_y'] \
                         ) \
                     ) \
                     + ((df_combos['point_nbr']/100)**2 * df_combos['y2'])
     
    
    # build the geojson string
    df_combos['coord_str'] = ' '*10 + '[' + df_combos['x'].astype(str) + ', ' + df_combos['y'].astype(str) + ']'
    
    
    # summarize by rank1/rank2
    df_shapes = ( df_combos.groupby(['node_count', 'origin', 'destination'], as_index=False)
                            ['coord_str'].apply(lambda x : ',\n'.join(x)) )
    
    df_shapes['feature_str'] = """    {
          "type": "Feature",
          "properties": {
            "node_count" : """ + '"' + df_shapes['node_count'].astype(str) + '"' + """,
            "origin": """ + '"' + df_shapes['origin'].astype(str) + '"' + """,
            "destination": """ + '"' + df_shapes['destination'].astype(str) + '"' + """      
          },
          "geometry": {
            "type": "LineString", 
            "coordinates": [
              """ + df_shapes['coord_str'] + """
            ]
          }
        }"""        
    
    # assemble final geojson string and outptu to file
    geojson_str = """{
      "type": "FeatureCollection",
      "features": [
    """ + ',\n'.join(df_shapes['feature_str']) + """
      ]
    }"""
    
    #df_combos.plot.scatter(x='x', y='y', figsize=(10,10), c='origin', colormap='tab20', colorbar=False)
    
    with open(rf'{output_path}\radial_shapes_{str(n).zfill(3)}.geojson', 'w') as f:
        f.write(geojson_str)


# --------------------------------------------------------------------------------------------------
# generate arc shapefiles for different numbers of nodes
# --------------------------------------------------------------------------------------------------

for n in range(2, 51):
    generate_radial_geojson(n, 2.5, OUTPUT_PATH)


# < 1.5 - path to nextdoor nodes bow outward (the lower the number, the further out)
# 1.5 - path to nextdoor nodes are nearly straight
# > 1.5, curves more toward the center (very large number, e.g. 400 curves about halfway in)


