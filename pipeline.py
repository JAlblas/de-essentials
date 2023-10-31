''' 
A pipeline which fetches data from an API, transforms the data, and uploads it into a SQLite database through pandas. 
'''

import requests
import pandas as pd
import sqlalchemy


def fetch_data():
    """ 
    Fetches data from https://api.energidataservice.dk/meta/dataset/Gasflow into a variable
    """
    url = 'https://api.energidataservice.dk/meta/dataset/Gasflow'
    data = requests.get(url).json()
    return data["columns"]


def transform(data):
    """ Transforms the dataset"""
    print(data)
    df = pd.DataFrame(data)
    print(f"Total number of rows {len(data)}")
    return df


def main():
    '''
    Calls the different functions
    '''
    json = fetch_data()
    transform(json)


if __name__ == "__main__":
    main()
