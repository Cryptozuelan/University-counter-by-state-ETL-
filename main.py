import requests
import pandas as pd
from sqlalchemy import create_engine


# Extract data from API

API_URL = "http://universities.hipolabs.com/search?country=United+States"


def extract(url) -> dict:
    data = requests.get(url).json()
    return data


def transform(data: dict, countryState: str) -> pd.DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    df = pd.DataFrame(data)
    print(f"Total Number of universities from API {len(data)}")
    df = df[df["name"].str.contains(countryState)]
    print(f"Number of universities in {countryState}: {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains", "country", "web_pages", "name"]]


def load(df: pd.DataFrame) -> None:
    """ Loads data into a sqllite database"""
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace')


data = extract(API_URL)
df = transform(data, 'South Carolina')
load(df)
