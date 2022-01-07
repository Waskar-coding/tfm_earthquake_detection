#Description
"""
Contains a script to load instances to any table given
its name and the name of the local database, these features
can be exported into any ETL script.
"""

#Tags
__author__ = 'Óscar Gómez Nonnato'
__date__ = '15/08/2021'


#Libraries
##Packages
import sqlite3


#Local database name
LOCAL_SEISMIC_DB = "../seismic_cat.db"
TRACE_LOCATION_MAP = [
    "D:\\traces\\traces_raw",
    "D:\\traces\\traces_filtered_5hz",
    "D:\\traces\\traces_spectrogram_5hz",
    "D:\\traces\\traces_record_5hz",
    "D:\\traces\\traces_filtered_2hz",
    "D:\\traces\\traces_spectrogram_2hz",
    "D:\\traces\\traces_record_2hz"
]


#Load instances into database
def load_instances(my_instances, my_table):
    ##Documentation
    """
    Description
    -----------
    Loads list of instances into seismic_cat database
    
    Parameters
    ----------
    my_instances: list[tuple]
        List of instances with their corresponding
        table structure
    my_table: str
        Name of the table to load the instances
    """
    
    ##Connect to database
    connection = sqlite3.connect(LOCAL_SEISMIC_DB)
    cursor = connection.cursor()

    ##Load instances
    value_placeholders = '?,'*(len(my_instances[0])-1) + '?'
    my_query = "INSERT INTO {} VALUES({});".format(my_table, value_placeholders)
    cursor.executemany(my_query, my_instances)

    ##Save changes and close
    connection.commit()
    cursor.close()
    connection.close()


#Load a trace in to the database
def load_trace_db(cursor, code, station, component, start, final, t_type, doc_name):
    ##Documentation
    """
    Description
    -----------
    Registers a trace into the local database
    
    Parameters
    ----------
    cursor: sqlite3.Cursor
        sqlite3 cursor of the local database connection
    code: str
        Earthquake code
    statio: str
        Seismic station name
    component:
        Seismic station component
    start: str
        Trace starting timestamp
    final: str
        Trace finish timestamp
    t_type: int
        Trace type
    doc_name: str
        Name of the file where the trace image is located
    """
    
    ##Inserting trace register into database
    location = TRACE_LOCATION_MAP[t_type] + "\\" + doc_name
    my_query = "INSERT INTO trace VALUES{}".format((code, station, component, start, final, str(t_type), location))
    cursor.execute(my_query)