#Description
"""
Loads earthquakes from the csv documents of the earthquakes folder.
All csv have been downloaded from ICGC's earthquake catalog:
https://www.icgc.cat/en/Citizens/Explore-Catalonia/Earthquakes/Recorded-earthquakes
"""


#Tags
__author__ = 'Óscar Gómez Nonnato'
__date__ = '15/08/2021'


#Libraries
##Standard
import os

##Packages
import pandas

##Local
from load_db import load_instances


#Default variables
DEFAULT_PREVIOUS_CODES = []


#Getting all earthquake files
def get_earthquake_files():
    ##Documentation
    """
    Description
    -----------
    Returns the list of earthquake csv files
    in the ./earthquakes folder.

    Returns
    -------
    A list of earthquake csv files from ICGC's 
    earthquake catalog
    """
    ##Creating file list
    return os.listdir('./ETL/earthquakes')


#Openning an earthquake file with pandas
def get_earthquake_df(my_file):
    ##Documentation
    """
    Description
    -----------
    Returns a pandas dataset of earthquake csv file

    Parameters
    ----------
    my_file: str
        Name of a csv file containing earthquake data
    
    Returns
    -------
    A pandas DataFrame object of the earthquake data
    """
    return pandas.read_csv("./ETL/earthquakes/{}".format(my_file), sep=',')


#Creating earthquake instances
def create_earthquake_instances(my_df, previous_codes):
    ##Documentation
    """
    Description
    -----------
    Changes dataframe columns so they are not dependant on download language.
    Eliminates instances with repeated codes from previous files.
    Excludes non-local earthquakes and earthquakes not sourced by the ICGC.
    Eliminates unnecessary colums to create database instance.
    
    Parameters
    ----------
    my_df: pandas.core.frame.DataFrame
        A pandas DataFrame object containing earthquake data, its columns
        are the following:
        (
            code, date, time, latitude,
            longitude, depth, magnitude,
            magnitude type, region, area,
            source, code
        )
    previous_codes: list[str]
        List of earthquake codes from previous files 
    
    Returns
    -------
    earthquake_instances: list
        A list of sql instances for the earthquake table on the local
        seismic_cat database, the column structure is the following:
        (
            code, date, time, latitude,
            longitude, depth, magnitude
        )
    unique_codes: list
        A list of the dataset's not repeated codes
    """
    ##Changing column names
    table_cols = my_df.columns
    default_table_cols = (
        'code', 'date', 'time',
        'latitude', 'longitude', 'depth',
        'magnitude', 'magnitude type', 'region',
        'area', 'source', 'other_code'
    )
    cols_dict = dict((k,v) for k,v in zip(table_cols, default_table_cols))
    my_df = my_df.rename(columns=cols_dict)
    
    ##Filtering earthquakes
    unique_codes = list(set(my_df['code']).difference(set(previous_codes)))
    my_df = my_df[(my_df['code'].isin(unique_codes)) & (my_df['area']=='Local') & (my_df['source']=='ICGC')]

    ##Eliminating unnecessary columns
    my_df = my_df[my_df.columns[:7]]

    ##Creating instances
    earthquake_instances = my_df.values.tolist()
    return (earthquake_instances, unique_codes)


#Main
def main():
    previous_codes = DEFAULT_PREVIOUS_CODES
    earthquake_files = get_earthquake_files()
    for earthquake_file in earthquake_files:
        earthquake_table = get_earthquake_df(earthquake_file)
        earthquake_instances, previous_codes = create_earthquake_instances(earthquake_table, previous_codes)
        load_instances(earthquake_instances, 'earthquake')


#Execution
if __name__ == "__main__":
    main()