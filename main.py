import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import sqlite3

from sqlalchemy.sql.expression import except_

DATABASE_LOCATION = 'sqlite:///mercadolibre.sqlite'

if __name__ == "__main__":

    today = datetime.now()

    r = requests.get('https://api.mercadolibre.com/sites/MLA/search?q=chromecast&limit=50#json')

    data = r.json()

    #print(data)

def check_if_valid_data(df: pd.DataFrame) -> bool:

    if df.empty:
        print("No items dowloaded. Finishing execution")
        return False
    
    if pd.Series(df['id']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    if df.isnull().values.any():
        raise Exception("Null value found")


#Extract

id = []
title = []
price =[]
Cantidad_vendida = []
address = []
seller = []
listing_type = []

for items in data['results']:
    id.append(items['id'])
    title.append(items['title'])
    price.append(items['price'])
    Cantidad_vendida.append(items["sold_quantity"])
    address.append(items['address']['state_name'])        
    listing_type.append(items['listing_type_id'])
    

items_dict = {
    "id": id,
    "title": title,
    "price":price,
    "cantidad_vendida": Cantidad_vendida,
    "address":address,
    "listing_type":listing_type,
} 

items_df = pd.DataFrame(
    items_dict,
    columns=[
        'id',
        'title',
        'price',
        'cantidad_vendida',
        'address',
        'listing_type'
        
    ])


#Transfor
if check_if_valid_data(items_df):
    print("Data valid, proceed to Load")

#Load

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn =sqlite3.connect('mercadolibre.sqlite')
cursor = conn.cursor()

sql_query = """
CREATE TABLE IF NOT EXISTS mercadolibre(
    id VARCHAR(200),
    title VARCHAR(200),
    price INT(100000),
    cantidad_vendida INT(100000),
    address VARCHAR(200),
    listing_type VARCHAR(200),
    CONSTRAINT primary_key_constraint PRIMARY KEY (id)
)
"""

cursor.execute(sql_query)
print("Database create successfully")

try:
    items_df.to_sql('mercadolibre', engine, index=False, if_exists='append')
except:
    print('Data already exists in the database')

conn.close()
print("Close database successfully")

items_df.reset_index().to_csv("Datos_de_Mercadolibre.csv", header=True, index=False)