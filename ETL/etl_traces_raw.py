#Description
"""
Downloads raw traces including an earthquek and then slices them to situate the earthquake at
a random position relative to their start and final.

To execute this script you need to activate a conda environment with obpsy installed:

conda activate [obspy environment]
"""

#Tags
__author__ = "Óscar Gómez Nonnato"
__date__ = "29/08/2021"

#Libraries
##Standard
import csv
from datetime import datetime, timedelta
import os
import random as rd

##Packages
import pandas as pd
import sqlite3
import obspy
from obspy.core.utcdatetime import UTCDateTime

##Local
from load_db import load_trace_db, LOCAL_SEISMIC_DB, TRACE_LOCATION_MAP


#Constants
LOG_URL = os.getcwd()
BASE_URL = "http://ws.icgc.cat/fdsnws/dataselect/1/query?"
RAW_TRACE_LOCATION = TRACE_LOCATION_MAP[0]
REGISTER_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
WINDOW_DATETIME_FORMAT = '%Y-%m-%dT%H:%M'
BIG_WINDOW_WIDTH = timedelta(minutes=10)
SMALL_WINDOWS_WIDTH = timedelta(minutes=5)
PS_OFFSET = timedelta(seconds=2)


#Extracting registers
def extract_registers(cursor):
    ##Documentation
    """
    Description
    -----------
    Extracts a collection of earthquake registers not contained
    within a seismic crisis and that have not had their traces
    downloaded. The register data is combined with its corresponding
    earthquake information from the local database.
    
    Parameters
    ----------
    cursor: sqlite3.Cursor
        sqlite3 cursor of the local database connection
    
    Returns
    -------
    valid_registers: pandas.core.frame.DataFrame
        DataFrame containing the selected registers
    """

    ##Extracting earthquake codes outside seismic crisis
    codes_query = """
        select code from earthquake as e where not exists (\
            select start, final from crisis as c where \
                e.date>=c.start and e.date<=c.final \
        )
    """
    valid_codes = cursor.execute(codes_query)
    get_code = lambda c_t: c_t[0]
    valid_codes = [get_code(code) for code in valid_codes]

    ##Extracting error earthquake codes
    error_requests = pd.read_csv('raw_traces_errorRequest_codes.csv')
    error_requests.columns = ('code', 'station')
    error_requests['code'] = [str(c) for c in error_requests['code']]
    valid_codes = list(set(valid_codes).difference(set(error_requests['code'])))
    valid_codes = str(tuple(valid_codes))

    ##Extraing registers with valid codes that do not have their raw traces
    registers_query = """
        select r.code, r.name, e.date, r.p_time, r.s_time from register as r\
            join earthquake e on r.code=e.code\
                where r.code in {} and not exists (
                    select t.code, t.name from trace as t where
                        t.type=0 and r.code=t.code and r.name=t.name
                )
    """.format(valid_codes)
    valid_registers = cursor.execute(registers_query)

    ##Creating dataframe from the response
    valid_registers = pd.DataFrame(valid_registers)
    valid_registers.columns = ('code', 'name', 'date', 'p_time', 's_time')
    return valid_registers
    

#Getting a column of datetime objects
def get_datetime_column(date_column, pick_column, dt_format):
    ##Documentation
    """
    Description
    -----------
    Combines the earthquake data with its picking times to 
    obtain picking timestamps.
    
    Parameters
    ----------
    date_column: list
        Earthquake date
    pick_column: list
        Register P or S pick
    dt_format: str
        Datetime format
    
    Returns
    -------
    datetime_col: list
        List of timestamps of register picks
    """
    
    ##Combining earthquake dates and picks into timestamps
    datetime_col = date_column + 'T' + pick_column
    datetime_col = [datetime.strptime(datetime_i, dt_format) for datetime_i in datetime_col]
    return datetime_col


#Getting windows from a register instance
def get_window_margin(pick_column, direction):
    ##Documentation
    """
    Description
    -----------
    Sets the traces time margins using P or S pick times as reference.
    The trace's window margins are obtained by displacing a constant amount
    of time (determined by BIG_WINDOW_WIDTH) backwards from the P picks and
    forwards from the S picks. Afterwards the resulting starting of finishing
    are transformed into the API'S format (determined by WINDOW_DATETIME_FORMAT).
    
    Parameters
    ----------
    pick_column: list
        Column with P or S pick timestamps
    direction: int
        Either 1 for P picks or -1 for S picks
    
    Returns
    -------
    window_margins: list
        List of window start or final datetimes
    """
    
    ##Calculating window margin
    displacement = BIG_WINDOW_WIDTH/2
    window_margins = [pick + direction*displacement for pick in pick_column]
    window_margins = [datetime.strftime(w_m, WINDOW_DATETIME_FORMAT) for w_m in window_margins]
    return window_margins


#Downloading traces
def extract_trace_from_api(start_time, final_time, station, network="CA", component="*", location="*", error_message="404"):
    ##Documentation
    """
    Description
    -----------
    Uses the obspy read method to query the following API:
        http://ws.icgc.cat/fdsnws/dataselect/1/query?starttime={}&endtime={}&network={}&station={}&location={}&channel={}&nodata={}
    
    Parameters
    ----------
    start_time: str
        Trace window starttime timestamp
    final_time: str
        Trace window endtime timestamp
    station: str
        Station the measures the trace
    network: str
        Network of stations, to which the station belongs.
        The Catalan network CA is chosen as default.
    component: str
        Station component of the trace. By default all trace
        components are selected.
    location: str
        Region in which the station is located. By default all
        regions are selected.
    error_message: str
        Error message that will appear in case of query failure.
    
    Returns
    -------
    trace_stream: obspy.core.stream.Stream
        Datastream object contaninig trace signal and metadata
    """
    
    ##Query parameters
    start_time_query = "starttime=" + start_time
    final_time_query = "endtime=" + final_time
    network_query = "network=" + network
    station_query = "station=" + station
    component_query = "channel=" + component
    location_query = "location=" + location
    error_query = "nodata=" + error_message
    
    ##Querying to API
    trace_query = BASE_URL + \
        start_time_query + "&" + \
        final_time_query + "&" + \
        network_query + "&" + \
        station_query + "&" + \
        location_query + "&" + \
        component_query + "&"+ \
        error_query
    trace_stream = obspy.read(trace_query) 
    return trace_stream


#Get shuffled start and final times
def get_shuffled_times(p_dt, s_dt):
    ##Documentation
    """
    Description
    -----------
    Selects random window start and finish times to slice it ensuring the
    earthquake is always included in the final window. Returns a start and
    final formatted version to store in the local database and another one
    to slice the trace using obspy.
    
    Parameters
    ----------
    p_dt: datetime.datetime
        P pick datetime.
    s_dt: datetime.datetime
        S pick datetime.
    
    Returns
    -------
    times: tuple
        Contains the raw trace's start and final datetimes in their timestamp format
        compatible with the local database and in their obspy format so slice the trace.
    """
    
    ##Calculating minimum and maximum trace start times
    window_start_min = s_dt + PS_OFFSET - SMALL_WINDOWS_WIDTH ###The S waves are not visible earlier
    window_start_max = p_dt - PS_OFFSET ###The P waves are not visible later
    period = window_start_max - window_start_min
    
    ##Randomly assigning start time
    start_shift = rd.randint(0, int(period.total_seconds()) * 1000)
    new_start_time = window_start_min + timedelta(milliseconds=start_shift)
    new_final_time = new_start_time + SMALL_WINDOWS_WIDTH

    ##New Time formatted for the database
    start_time_db = datetime.strftime(new_start_time, REGISTER_DATETIME_FORMAT)
    final_time_db = datetime.strftime(new_final_time, REGISTER_DATETIME_FORMAT)

    ##New time formatted for slicing the trace
    start_time_tr = start_time_db + 'Z'
    final_time_tr = final_time_db + 'Z'
    start_time_tr = UTCDateTime(start_time_tr)
    final_time_tr = UTCDateTime(final_time_tr)

    ##Times tuple
    times = (start_time_db, final_time_db, start_time_tr, final_time_tr) 
    return times


#Main
def main():
    ##Connect to database
    connection = sqlite3.connect(LOCAL_SEISMIC_DB)
    cursor = connection.cursor()

    ##Extracting registers from db
    registers = extract_registers(cursor)
    total_registers = str(len(registers))
    
    ##Transforming registers dataframe: datetime columns
    date_col = registers['date']
    p_col = registers['p_time']
    s_col = registers['s_time']
    p_col = get_datetime_column(date_col, p_col, REGISTER_DATETIME_FORMAT)
    s_col = get_datetime_column(date_col, s_col, REGISTER_DATETIME_FORMAT)
    registers['p_datetime'] = p_col
    registers['s_datetime'] = s_col
    del registers['date']
    del registers['p_time']
    del registers['s_time']
    del date_col

    ##Transforming registers dataframe: register windows
    registers['window_start'] = get_window_margin(p_col, -1)
    registers['window_final'] = get_window_margin(p_col, 1)
    del p_col
    del s_col

    ##Extracting, shuffling and loading traces
    os.chdir(RAW_TRACE_LOCATION)
    for i, register in registers.iterrows():
        ###Register variables
        code = register['code']
        station = register['name']
        start_time = register['window_start']
        final_time = register['window_final']
        p_time = register['p_datetime']
        s_time = register['s_datetime']

        ###Querying register
        try: 
            stream = extract_trace_from_api(start_time, final_time, station)
        except:
            print('Request Error (code: ' + code + ', station: ' + station + ')')
            with open(LOG_URL + '\\raw_traces_errorRequest_codes.csv', 'a', newline="") as error_list:
                writter = csv.writer(error_list)
                writter.writerow([code, station])
            continue

        ###Generating shuffled times
        st_db, ft_db, st_tr, ft_tr = get_shuffled_times(p_time, s_time)

        ###Slicing and saving trace information
        for tr in stream:
            tr_c = tr.slice(st_tr, ft_tr)
            component = tr_c.stats.component
            try:
                tr_c_name = "{}_{}_{}.mseed".format(code, station, component)
                tr_c.write(tr_c_name, format="MSEED")
                load_trace_db(cursor, code, station, component, st_db, ft_db, 0, tr_c_name)
            except:
                print('Unique Error (code: ' + code + ', station: ' + station + ')')
                with open(LOG_URL + '\\raw_traces_errorUNIQUE_codes.csv', 'a', newline="") as error_list:
                    writter = csv.writer(error_list)
                    writter.writerow([code, station])
                break
            

        ###Logging results
        print(code + '-' + station + ': ' + str(i) + '/' + total_registers)
        connection.commit()

    ##Closing connection
    cursor.close()
    connection.close()


#Execution
if __name__ == "__main__":
    main()