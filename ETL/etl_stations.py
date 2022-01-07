#Description
"""
Loads ICGC's stations basic data from a raw txt file and transforms
them into station table instances to load them into the local database.
Only stations containing a broadband velocimeter are registered.
The sation's basic data has been downloaded from:
https://www.icgc.cat/Ciutada/Explora-Catalunya/Terratremols/Xarxes-sismiques
"""

#Tags
__author__ = 'Óscar Gómez Nonnato'
__date__ = '15/08/2021'


#Libraries
##Local
from load_db import load_instances


#Getting stations raw list
def get_stations_raw():
    ##Documentation
    """
    Description
    -----------
    Extracts a txt file containing all stations
    basic data from the ./stations folder and divides
    the information by lines.

    Returns
    -------
    station_list_raw: list
        A list of all the stations basic data
    """
    
    ##Extracting stations raw information
    with open('./ETL/stations/Seismic stations. Institut Cartogràfic i Geològic de Catalunya.txt', 'r') as station_file:
        station_list_raw = station_file.readlines()[1:]
        return station_list_raw


#Creating station instances
def create_station_instances(station_list_raw):
    ##Documentation
    """
    Description
    -----------
    Parses station information and transforms it into the
    format of the station table in to the local database.
    Only stations containing a broadband velocimeter are
    selected.
    
    Parameters
    ----------
    station_list_raw: list
        A list of all the stations basic data
    
    Returns
    -------
    register_instances: list
        List contaning basic station data. The format of these
        instances adjusts to the register table schema in the 
        local database.
    """
    
    ##Parsing stations
    stations_instances = []
    while len(station_list_raw) > 0:
        current_station = station_list_raw[:3]
        station_list_raw = station_list_raw[3:]
        st_network, st_name = current_station[0].strip('\n').split('.')
        st_data = current_station[2].split('\t')
        st_lat, st_lon, st_alt = st_data[1:4]
        st_type = st_data[4]
        if(st_type == 'Broadband velocimeter' or st_type == 'Broadband velocimeter and Accelerometer'):
            stations_instances.append((st_name, st_network, float(st_lat), float(st_lon), int(st_alt)))
    return stations_instances


#Main
def main():
    station_list_raw = get_stations_raw()
    stations_instances = create_station_instances(station_list_raw)
    load_instances(stations_instances, 'station')


#Execution
if __name__ == '__main__':
    main()