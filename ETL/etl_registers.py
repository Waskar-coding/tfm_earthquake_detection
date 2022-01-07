#Description
"""
Queries register data from new earthquake instances, parses the response (gse file)
to extract P and S wave arrival times and magnitudes. Deep phases and uncomplete
registers are then discarted. The remaning registers are transformed into the format
of the register table of the local database and stored.
"""

#Tags
__author__='Waskar-coding'
__date__='14/08/2021'

#Libraries
##Packages
import requests
import sqlite3

##Local scripts
from load_db import load_instances, LOCAL_SEISMIC_DB


#Extract: Getting register table
def extract_earthquake_codes():
    ##Documentation
    """
    Description
    -----------
    Extracts all earthquake codes from the earthquake and register tables
    of the local database and finds the earthquake codes that do not appear
    in the register table.
    
    Returns
    -------
    codes: list[str] 
        A list of codes from the earthquake table that do not appear
        in the register table
    """
    
    ##Connect to database
    connection = sqlite3.connect(LOCAL_SEISMIC_DB)
    cursor = connection.cursor()

    ##Load instances
    codes_e = cursor.execute("SELECT (code) FROM earthquake").fetchall()
    codes_r = cursor.execute("SELECT (code) FROM register").fetchall()
    codes = list(set(codes_e).difference(set(codes_r)))

    ##Save changes and close
    cursor.close()
    connection.close()

    ##Return list of codes
    codes = list(map(lambda code_tuple: code_tuple[0], codes))
    return codes


#Extract: Getting register table
def extract_registers(earthquake_code):
    ##Documentation
    """
    Description
    -----------
    Queries ICGC's earthquake database for gse documentation on
    an earthquake using its code. This documentation includes 
    phase arrival times, trace amplitudes and magnitudes of each
    station that recorded the earthquake.

    A table of stations registers is selected among other general
    data. If no register table is found a None value will be and 
    the earthquake will be discarted. If the table is found it will
    be parsed and returned.
    
    Parameters
    ----------
    earthquake_code: str
        An ICGC earthquake code, a 5 digit number
    
    Returns
    -------
    register_table: list | None
        A list of tuples containing each line of the register table, on gse
        files P wave and S wave registers are separated and have different
        data points. These function will return the raw separated registers.
        If the gse file does not contain a station register table
        None is returned
    """
    
    ##Querying ICGC database
    response = requests.get('https://sismocat.icgc.cat/sisweb2/siswebclient_gse_external.php?seccio=gse&codi=' + earthquake_code)

    ##Selecting table
    has_registers = False
    register_table = str(response.content).split('\\r\\n')
    for i,line in enumerate(register_table):
        if line.startswith('Sta'):
            register_index = i + 1
            has_registers = True
    if has_registers == False:
        print(earthquake_code)
        register_table = None
        return register_table
    register_table = register_table[register_index:-2]

    ##Parsing register table
    register_table = list(map(
        lambda line: tuple(filter(
            lambda w: w != '',
            line.split(' ')
        )),
        register_table
    ))
    return register_table


#Transform: Selecting P and S pairs
def transform_registers_raw_to_pairs(register_raw):
    ##Documentation
    """
    Description
    -----------
    Pairs P and S wave registers for each station, eliminates
    registers that do not have a pairing station. Registers from
    the same station appear next to each other in the table. This
    function will check consecutive registers to find pairs and add
    their registers togheter into a pair list.
    
    Parameters
    ----------
    register_raw: list
        List of tuples containing each P and S wave register of an
        earthquake.
    
    Returns
    -------
    register_pairs: list
        List of tuple pairs containing instances of register_raw
        that share a station.
    """
    
    register_pairs = []
    contador = 0
    while contador < len(register_raw)-1:
        p_line = register_raw[contador]
        s_line = register_raw[contador+1]
        if p_line[0] == s_line[0]:
            register_pairs.append((p_line, s_line))
            contador += 2
        else:
            contador += 1
    return register_pairs
    

#Transform: Filtering deep phases
def transform_registers_pairs_filter(register_pairs):
    ##Documentation
    """
    Description
    -----------
    Filters deep phases (P or S phases not named with a single P or S),
    and register pairs with incomplete P or S phase registers (they must
    be of a certain length).
    
    Parameters
    ----------
    register_pairs: list
        List of tuple pairs containing instances of registers
        that share a station.
    
    Returns
    -------
    filtered_register_pairs: list
        Register pairs that have passed this function filters
    
    """
    
    
    ##Filtering out deep phases
    register_pairs = list(filter(lambda r: r[0][3]=='P' and r[1][3]=='S', register_pairs))

    ##Filtering out incomplete registers
    filtered_register_pairs = list(filter(lambda r: len(r[0])==8 and len(r[1])==12, register_pairs))
    return filtered_register_pairs


#Transform: Crafting registers
def transform_registers_pairs_to_instances(code, register_pairs):
    ##Documentation
    """
    Description
    -----------
    Transforms register pairs into the local database format
    
    Parameters
    ----------
    code: str
        An ICGC earthquake code, a 5 digit number
    register_pairs: list
        List of tuple pairs containing instances of registers
        that share a station.
    
    Returns
    -------
    register_instances: list
        List of tuples containing P and S wave arrival times, amplitudes,
        and magnitudes for each passed pair of registers. The format of these
        tuples adjusts to the register table schema in the local database.
    """
    
    ##Transforming instance pairs to db format
    register_instances = []
    for register_pair in register_pairs:
        p_r, s_r = register_pair
        register_instances.append((code, p_r[0], p_r[1], p_r[4], s_r[4], s_r[7], s_r[8]))
    return register_instances


#Main
def main():
    earthquake_codes = extract_earthquake_codes()
    total_codes = len(earthquake_codes)
    counter = 0
    for earthquake_code in earthquake_codes:
        registers_raw = extract_registers(earthquake_code)
        if registers_raw == None:
            continue
        register_pairs = transform_registers_raw_to_pairs(registers_raw)
        register_pairs = transform_registers_pairs_filter(register_pairs)
        register_instances = transform_registers_pairs_to_instances(earthquake_code,register_pairs)
        if(len(register_instances) == 0):
            continue
        load_instances(register_instances, 'register')

        counter+=1
        print("{}: {}/{}".format(earthquake_code, counter, total_codes))


#Execution
if __name__ == '__main__':
    main()