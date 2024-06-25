import os
import io
import pytest
import pandas as pd
from pipeline import get_url_data, save_df_to_csv
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

mock_weather_data = """
name,datetime,tempmax,tempmin,temp,feelslikemax,feelslikemin,feelslike,dew,humidity,precip,precipprob,precipcover,preciptype,snow,snowdepth,windgust,windspeed,winddir,sealevelpressure,cloudcover,visibility,solarradiation,solarenergy,uvindex,severerisk,sunrise,sunset,moonphase,conditions,description,icon,stations
dublin,2021-01-01,4.8,-1.6,2,1.8,-6.1,-1.9,0.6,90.4,0.969,100,4.17,rain,0,0.3,40.7,21.3,311.1,1016.9,35.3,22.6,20.2,1.7,2,,2021-01-01T08:40:18,2021-01-01T16:17:18,0.58,"Rain, Partially cloudy",Partly cloudy throughout the day with early morning rain.,rain,"03971099999,F4142,EIDW,E7034,03969099999,03967099999,EIME"
dublin,2021-01-02,3.1,-2.2,0.4,-1,-7.2,-4.2,-0.7,92.4,0.078,100,4.17,rain,0,0.1,42.5,22.6,289.3,1021.3,47.6,18.4,14.8,1.2,1,,2021-01-02T08:40:05,2021-01-02T16:18:27,0.62,"Rain, Partially cloudy",Partly cloudy throughout the day with afternoon rain.,rain,"03971099999,F6683,F4142,EIDW,03969099999,03967099999,EIME,03961099999"
dublin,2021-01-03,3.1,-1.9,0.5,-0.6,-6.4,-3.6,-1.1,89.1,0.796,100,4.17,rain,0,0,51.8,22,339.1,1024.5,59.2,16.6,11.9,1,1,,2021-01-03T08:39:50,2021-01-03T16:19:39,0.65,"Rain, Partially cloudy",Partly cloudy throughout the day with late afternoon rain.,rain,"03971099999,F6683,F4142,EIDW,03969099999,03967099999,EIME,03961099999"
dublin,2021-01-04,4.7,2.2,3.5,0.4,-2,-0.7,0.4,80.1,3.153,100,12.5,rain,0,0,48.6,24,51.3,1027.4,71.3,16.4,9.4,0.8,1,,2021-01-04T08:39:31,2021-01-04T16:20:54,0.69,"Rain, Partially cloudy",Partly cloudy throughout the day with rain.,rain,"03971099999,F6683,F4142,EIDW,03969099999,F8809,03967099999,EIME,03961099999"
dublin,2021-01-05,4.6,1.4,2.7,0.4,-2.6,-1.3,0,82.5,0.518,100,8.33,rain,0,0,45.7,22.8,25.7,1028.2,71.1,17.7,17.2,1.4,1,,2021-01-05T08:39:08,2021-01-05T16:22:11,0.73,"Rain, Partially cloudy",Partly cloudy throughout the day with rain.,rain,"03971099999,F6683,F4142,EIDW,03969099999,03967099999,EIME"
"""

@pytest.fixture(scope='module')
def weather_data():
    weather_df = pd.read_csv(io.StringIO(mock_weather_data))
    assert not weather_df.empty, "Mock weather data is empty."
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
    
    save_df_to_csv(output, 'traffic_flow_data.csv')


def test_weather_data(weather_data):
    output = process_weather_data(weather_data)
    
    assert output is not None
    # Check for the columns in the output dictionary
    expected_columns = ['datetime', 'temp', 'humidity', 'precip', 'windspeed', 'visibility', 'icon']
    for columns in expected_columns:
        assert columns in output
    
    save_df_to_csv(output, 'weather_data.csv')


def test_csv_data_files_exist():
    """
    Verify that the CSV files specified in csv_data_files exist in the data directory.
    """
    for filename in csv_data_files:
        filepath = os.path.join(data_dir, filename)
        assert os.path.exists(filepath), f"ERROR: {filepath} does not exist."


