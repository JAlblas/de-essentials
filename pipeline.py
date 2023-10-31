''' 
A pipeline which fetches data from an API, transforms the data, and uploads it into a SQLite database through pandas. 
'''

import requests
import pandas as pd
from sqlalchemy import create_engine


def fetch_data():
    """ 
    Fetches data from https://api.energidataservice.dk/meta/dataset/Gasflow into a variable
    """
    url = 'https://api.energidataservice.dk/dataset/PowerSystemRightNow?limit=100'
    data = requests.get(url).json()
    return data["records"]


def transform(data):
    """ Transforms the dataset"""
    df = pd.DataFrame(data)
    print(f"Total number of rows {len(data)}")
    return df


def ingest(df):
    """ Ingests data into sqllite database"""
    engine = create_engine('sqlite:///data.db')
    df.to_sql('energy', engine, if_exists='replace')


def main():
    '''
    Calls the different functions
    '''
    json = fetch_data()
    df = transform(json)
    print(df)
    ingest(df)


if __name__ == "__main__":
    main()
