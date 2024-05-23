import os
import logging
import pandas as pd
import requests
import io
from process import process_traffic_flow_data, process_weather_data

def get_url_data(url):
    """
    Fetch data from the given URL and load it into a DataFrame.

    Args:
        url (str): URL to fetch data from.

    Returns:
        pd.DataFrame: DataFrame containing the fetched data, or None if an error occurs.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        data_df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        logging.info("Data successfully retrieved from URL and loaded into DataFrame.")
        return data_df
    except requests.RequestException as e:
        logging.error(f"Unable to download data from URL: {e}")
        return None
    except pd.errors.EmptyDataError as e:
        logging.error(f"CSV file appears to be empty: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error occurred while fetching data from URL: {e}")
        return None

def save_df_to_csv(df_data, filename):
    """
    Save the given DataFrame to a CSV file.

    Args:
        df_data (pd.DataFrame): DataFrame to save.
        filename (str): Filename to save the DataFrame to.
    """
    directory = 'data'
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Directory created: {directory}")
    try:
        df_data.to_csv(os.path.join(directory, filename), index=False)
        logging.info(f"Data successfully saved to {filename}.")
    except Exception as e:
        logging.error(f"Error encountered while saving file: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Process traffic flow data
        traffic_flow_url = 'https://data-sdublincoco.opendata.arcgis.com/datasets/sdublincoco::traffic-flow-data-jan-to-june-2021-sdcc.csv'
        traffic_flow_df = get_url_data(traffic_flow_url)
        output_csv = 'traffic_flow_data.csv'
        
        if traffic_flow_df is not None:
            traffic_flow_data = process_traffic_flow_data(traffic_flow_df)
            if traffic_flow_data is not None:
                save_df_to_csv(traffic_flow_data, output_csv)
            else:
                logging.error("Processing of traffic flow data failed.")
        else:
            logging.error("Unable to retrieve traffic flow data.")
        
        # Process weather data
        weather_url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=JFUWSDF8WDRYFZZFBZ3W9M8FE&taskId=82943c3c20942672294f3f2292ede040&zip=false'
        weather_df = get_url_data(weather_url)
        output_csv = 'weather_data.csv'
        
        if weather_df is not None:
            weather_data = process_weather_data(weather_df)
            save_df_to_csv(weather_data, output_csv)
        else:
            logging.error("Processing of weather data failed.")
    
    except Exception as e:
        logging.error(f"Error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
