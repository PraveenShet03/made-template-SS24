import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_traffic_flow_data(traffic_flow_df):
    """
    Process traffic flow data by converting date, dropping unnecessary columns,
    aggregating by date, and rounding the flow values to integers.

    Args:
        traffic_flow_df (pd.DataFrame): DataFrame containing traffic flow data.

    Returns:
        pd.DataFrame: Processed DataFrame with aggregated and rounded flow values.
    """
    try:
        # Convert 'date' column to datetime format
        traffic_flow_df['date'] = pd.to_datetime(traffic_flow_df['date'], format='%d/%m/%Y', errors='coerce')
        
        # Verify for any null values after conversion
        if traffic_flow_df['date'].isnull().any():
            logger.error("Null values detected in the 'date' column after conversion.")
            return None
        
        columns_to_drop = [
            'site', 'start_time', 'end_time', 'flow_pc', 'stops', 'stops_pc', 
            'del', 'del_pc', 'cong', 'cong_pc', 'dsat', 'dsat_pc', 'ObjectId'
        ]
        traffic_flow_df = traffic_flow_df.drop(columns=columns_to_drop)
        
        # Aggregate the DataFrame by the date column, calculating the mean for numeric values i.e flow value
        aggregated_df = traffic_flow_df.groupby('date').mean(numeric_only=True).reset_index()
        
        # Round the values in the 'flow' column to the nearest integer
        aggregated_df['flow'] = aggregated_df['flow'].round().astype(int)
        
        # Reformat the 'date' column back to the original format
        aggregated_df['date'] = aggregated_df['date'].dt.strftime('%d/%m/%Y')
        
        return aggregated_df  
    
    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}")
        return None
    
def process_weather_data(weather_df):
    """
    Process weather data by dropping unnecessary columns.

    Args:
        weather_df (pd.DataFrame): DataFrame containing weather data.

    Returns:
        pd.DataFrame: Processed DataFrame with selected columns removed.
    """
    columns_to_drop = [
        'name', 'tempmax', 'tempmin', 'feelslikemax', 'feelslikemin', 'feelslike', 'dew', 
        'precipprob', 'precipcover', 'preciptype', 'snow', 'snowdepth', 'windgust', 'winddir', 
        'sealevelpressure', 'cloudcover', 'solarradiation', 'solarenergy', 'uvindex', 
        'severerisk', 'sunrise', 'sunset', 'moonphase', 'conditions', 'description', 'stations'
    ]
    weather_df = weather_df.drop(columns=columns_to_drop)
    return weather_df
