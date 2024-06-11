import os
import pytest
import pandas as pd
from pipeline import get_url_data
from process import process_traffic_flow_data, process_weather_data

data_dir = 'data'
csv_data_files = [
    'traffic_flow_data.csv',
    'weather_data.csv'
]

traffic_flow_url = 'https://data-sdublincoco.opendata.arcgis.com/datasets/sdublincoco::traffic-flow-data-jan-to-june-2021-sdcc.csv'

@pytest.fixture(scope='module')
def traffic_flow_data():
    traffic_df = get_url_data(traffic_flow_url)
    assert traffic_df is not None
    assert not traffic_df.empty
    return traffic_df

@pytest.fixture(scope='module')
def weather_data():
    weather_csv_file = os.path.join(data_dir, 'dublin_weather_data.csv')
    assert os.path.exists(weather_csv_file), f"ERROR: {weather_csv_file} does not exist."
    weather_df = pd.read_csv(weather_csv_file)
    assert not weather_df.empty, f"ERROR: {weather_csv_file} is empty."
    return weather_df

def test_get_traffic_flow_data_success():
    traffic_df = get_url_data(traffic_flow_url)
    assert traffic_df is not None
    assert not traffic_df.empty

def test_traffic_flow_data(traffic_flow_data):
    output = process_traffic_flow_data(traffic_flow_data)
    
    assert output is not None
    
    # Check for the columns in the output dictionary
    expected_columns = ['date', 'flow']
    for columns in expected_columns:
        assert columns in output


def test_weather_data(weather_data):
    output = process_weather_data(weather_data)
    
    assert output is not None
    # Check for the columns in the output dictionary
    expected_columns = ['datetime', 'temp', 'humidity', 'precip', 'windspeed', 'visibility', 'icon']
    for columns in expected_columns:
        assert columns in output


def test_csv_data_files_exist():
    """
    Verify that the CSV files specified in csv_data_files exist in the data directory.
    """
    for filename in csv_data_files:
        filepath = os.path.join(data_dir, filename)
        assert os.path.exists(filepath), f"ERROR: {filepath} does not exist."


