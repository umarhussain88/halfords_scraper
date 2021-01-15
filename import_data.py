import pyodbc 
import pandas as pd 
from sqlalchemy import create_engine

server = 'DESKTOP-9TJUVID' 
database = 'testdb' 
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+database+";Trusted_Connection=Yes")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)



def import_data_to_db(dataframe,connection,schema,table_name):
    dataframe.to_sql(f"{table_name}",schema=schema,
    if_exists='replace',
    index = False, 
    con=connection 
    )


