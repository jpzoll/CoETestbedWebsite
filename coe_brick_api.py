from install_modules import *
if missing_libraries: install_missing_libraries(missing_libraries)
import re
import pymysql
import brickschema
import pyodbc as odbc
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import sys
import os

#
# 🧱 Ensure Brick Schema file is present in current working directory
#

script_directory = os.path.dirname(os.path.abspath(__file__))
brick_file_name = "COE_10_10.ttl"
try:
    current_directory = os.path.dirname(os.path.abspath(__file__))
    g = brickschema.Graph().load_file(f"{script_directory}/{brick_file_name}")
except FileNotFoundError:
    print(f"\nThe file '{brick_file_name}' does not exist in the current directory. Exiting...\n")
    quit()
except Exception as e:
    print(f"An error occurred: {e}")


#
# 🤲 Connecing to the database with pyodcb
# Ensure that you have the proper credentials to login into this database. This pulls them from environment variables for security.
# If you don't have the credentials in your environment, add them there or replace the os.environ.get() lines with the actual info.
#

dsn = os.environ.get('DB_DSN')
user = os.environ.get('DB_USER')
password = os.environ.get('DB_PASS')
database = os.environ.get('DB_DATABASE')
connString = 'DSN={0};UID={1};PWD={2};DATABASE={3};'.format(dsn,user,password,database)
server = os.environ.get('DB_SERVER')
connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};Trust_Connection=yes;UID={user};PWD={password};TrustServerCertificate=yes;'
if None in (dsn, user, password, database, server):
    print(dsn, user, password, database, server)
    print("One or more required environment variables for the CoE Database information are missing.")
    exit(1)  # Exit the script with an error code
try:
    conn = odbc.connect(connectionString)
except odbc.Error as e:
    print(f"An error occurred: {e}")



def get_relations(bs_sql, full_name = None):
  # Find the index of WHERE
  where_index = bs_sql.upper().find('WHERE')
  # Extract the substring before WHERE
  sub_str = bs_sql[:where_index]

  # Use regular expressions to find ?xxx patterns
  # The pattern looks for words that start with "?" and captures the part after "?"
  result = re.findall(r'\?(\w+)', sub_str)

  df = pd.DataFrame.from_records(list(g.query(bs_sql)),columns=result)
  if full_name == 1:
    df = df.applymap(lambda x: str(x))
  else:
    df = df.applymap(lambda x: str(x).split('#')[-1])
  return df


def get_all_floors():
    query = """
    SELECT ?floor
    WHERE {
        ?floor a brick:Floor .
    }
    """
    df = get_relations(query)
    return(df)

def get_floor_rooms(floor):

    query = f"""
    SELECT ?room
    WHERE {{
    bldg:{floor} a brick:Floor ;
                        brick:hasPart ?room .
    }}
    """
    df = get_relations(query)
    return df['room'].tolist()

def get_room_sensors(room):
    print("GET_ROOM_SENSORS")
    query = f"""
    SELECT ?sensor
    WHERE {{
    ?sensor brick:hasLocation bldg:{room} .
    }}
    """
    df = get_relations(query)
    df_list = df['sensor'].tolist()
    # for i in range(len(df_list)):
    #    df_list[i] = df_list[i].replace('(', ' (')
    # print(f"df_list: {df_list}")
    return df_list

def get_room_sensors_and_uuids(room):
    query = f"""
    SELECT ?sensor ?uuid
    WHERE {{
    ?sensor brick:hasLocation bldg:{room} ;
    brick:hasTimeseriesReference ?uuid .
    }}
    """
    df = get_relations(query)
    return df

#
# 📸 Getting a snapshot of the current data point readings for a sensor RIGHT NOW
#   This can either take a sensor name or its UUID.
#   Regardless, the UUID is used to query the CoE database,
#   where a JOIN is made with the Sensors table and IAQ table to get the readings
#

def get_sensor_data(uuid=None):
    if uuid is None:
        print("You must pass a UUID.")
        return None

    #sensor = get_sensor_name(uuid)

    #if sensor is None:
    #    print(f"No sensor found for UUID {uuid}. Cannot query CoE DB. Exiting...")
    #    return None

    sql = f"""
    SELECT TOP 20 iaq.*
    FROM [COE].[dbo].[Sensors] as sensors
    JOIN COE.dbo.IAQ as iaq
    ON sensors.SensorID = iaq.SensorId
    WHERE [BRICK_UUID] = '{uuid}'
    ORDER BY iaq.DT DESC
    """
    # Assuming `conn` is a valid connection object
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        if data:
            # Convert to Dataframe
            data = [list(row) for row in data]
            columns = [column[0] for column in cursor.description]
            df = pd.DataFrame(data, columns=columns)
            df = df.to_json(orient='records', date_format='iso')
            return df
        else:
            print(f"No data found for the given sensor uuid: {uuid}.")
            return None
    except odbc.Error as err:
        print(f"Exception in pyodbc: {err}")
        return None


def get_sensor_points(sensor):
    # if '\\' in sensor or '(' in sensor or ')' in sensor:
    #     print(f"\n\nSENSOR NAME CONTAINS BACKSLASH, DISREGARDING: {sensor}\n\n")
    #     return []
    # else:
    #    pass
        #print(f"\n\nGETTING SENSOR POINTS FOR : {sensor}\n\n")
    

    query = f"""
    SELECT ?point
    WHERE {{
    bldg:{sensor}
        brick:hasPoint ?point
    }}
    """
    df = get_relations(query)
    print(df['point'].tolist())
    return df['point'].tolist()

# General Functions. Can be used outside of building main JSON object 

def get_all_sensors():
   query = f"""
    SELECT ?sensor
    WHERE {{
        ?sensor a ?sensorName ;
    }}
    """
   df = get_relations(query)
   return df['sensor'].tolist()

def get_sensor_uuid_df():
   query = """
    SELECT ?sensor ?uuid
    WHERE {
    {
        ?sensor a ?Sensor ;
                brick:hasTimeseriesReference ?uuid .
    }
    UNION
    {
        ?sensor brick:hasTimeseriesReference ?uuid .
    }
    }
    """
   df = get_relations(query)
   return df

def get_sensor_uuid(sensor):
    # sensor = sensor.replace(' (', r'\(')
    # sensor = sensor.replace(')', r'\)')
    query = f"""
    SELECT DISTINCT ?uuid
    WHERE {{
    {{
        bldg:{sensor} a ?Sensor ;
                brick:hasTimeseriesReference ?uuid .
    }}
    UNION
    {{
        bldg:{sensor} brick:hasTimeseriesReference ?uuid .
    }}
    }}
    """
    print(f"sensor in get_sensor_uuid: {sensor}")
    df = get_relations(query)
    result_array = df['uuid'].tolist()
    return str(result_array[0]) if result_array != [] else None