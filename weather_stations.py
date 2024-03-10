import streamlit as st
import datetime as dt
import pandas as pd
import asyncio

from env_canada import ECWeather, ECHistorical
from env_canada.ec_historical import get_historical_stations

# https://dd.weather.gc.ca/observations/xml/PE/yesterday/yesterday_pe_20240208_e.xml


WEATHER_SITES_URL = 'https://dd.weather.gc.ca/citypage_weather/docs/site_list_provinces_en.csv'
WEATHER_URL = 'https://dd.weather.gc.ca/citypage_weather/xml/PE/s0000026_e.xml'

@st.cache_data
def load_sites_data():
    """Loads weather sites from Environment Canada's most recent sites list

    Returns:
        DataFrame: a pandas DataFrame containing site codes, names, and lat/long
    """
    data = pd.read_csv(WEATHER_SITES_URL, header=1)
    lowercase = lambda x: str(x).lower()
    data.rename(lowercase, axis='columns', inplace=True)
    
    data['latitude'] = data['latitude'].str[:-1].astype(float)
    data['longitude'] = data['longitude'].str[:-1].astype(float)
    data['longitude'] = data['longitude'].multiply(-1)
    
    return data


def split_date_range(range_col):
    """Splits a column that contains a `start_date | end_date` string into separate columns

    Args:
        range_col (Series): A DataFrame column to split

    Returns:
        Series: Two new columns with dates separated
    """
    
    return pd.Series(range_col.split('|', 1))


def process_station_dates(df):
    """Accepts a DataFrame containing `hlyRange`, `dlyRange` and `mlyRange` columns, and splits
    them into `<period>_data_start`, `<period>_data_end` formats.  Removes original range data.
    Converts dates to datetime columns.

    Args:
        df (DataFrame): A DataFrame containing `hlyRange`, `dlyRange`, `mlyRange`

    Returns:
        DataFrame: Returns a transformed DataFrame with separated start / end columns, as datetimes.
    """
    df = df.T
    df[['hourly_data_start', 'hourly_data_end']] = df['hlyRange'].apply(split_date_range)
    df[['daily_data_start', 'daily_data_end']] = df['dlyRange'].apply(split_date_range)
    df[['monthly_data_start', 'monthly_data_end']] = df['mlyRange'].apply(split_date_range)
    
    cols_to_process = [
        'hourly_data_start',
        'hourly_data_end',
        'daily_data_start',
        'daily_data_end',
        'monthly_data_start',
        'monthly_data_end',
    ]
    
    for col in cols_to_process:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%d')
    
    df = df.drop(columns=['hlyRange', 'dlyRange', 'mlyRange'])
    
    return df


def choose_historical_station_id(df):
    """Parses historical station data to retrieve the closest station that has recent daily data.

    Args:
        df (DataFrame): Historical weather station data, as output from process_station_dates

    Returns:
        df: A single station row that contains the most recent daily data
    """
    # filter only those that have daily data
    selected_stations = df[df['daily_data_end'] == dt.datetime.strftime((dt.datetime.today() - dt.timedelta(days=1)), format='%Y-%m-%d')]
    selected_stations = selected_stations.sort_values(by='proximity', ascending=True)
    selected_station = selected_stations['id'].iloc[0]
    
    return selected_station

    
def get_conditions(station_id):
    """Retrieves the latest weather conditions from a given weather station.

    Args:
        station_id (String): Must be in the format `Province Code`/`Station Code`.

    Returns:
        dict: Dictionary containing the latest weather conditions.
    """
    # station_id='ON/s0000430'
    weather = ECWeather(station_id=station_id, language='english')
    asyncio.run(weather.update())
    
    return weather.conditions


def format_conditions_data(conditions_data):
    """Transforms DataFrame for readability.

    Args:
        conditions_data (DataFrame): Weather conditions data as output by get_conditions

    Returns:
        df: A DataFrame with reordered columns for readability
    """
    
    df = pd.DataFrame()
    df['label'] = conditions_data['label']
    df['value'] = conditions_data['value']
    df['unit'] = conditions_data['unit']
    
    return df
    
    
def display_conditions(station):
    """Retrieves current weather conditions from a given station site.

    Args:
        station (DataFrame): A single station site row, as output from load_sites_data
    """
    
    station_id = f"{station['province codes'].values[0]}/{station['codes'].values[0]}"
    conditions = get_conditions(station_id)
    conditions_data = pd.DataFrame.from_dict(conditions).T
    conditions_data = format_conditions_data(conditions_data)
    
    # Update the session state
    st.session_state.conditions = conditions_data

    
def display_historical(station_id):
    """Retrieves historical weather data from a given station site row, as output from load_sites_data.
    Updates display, including station site metadata.

    Args:
        station_id (DataFrame): A single station site row, as output from load_sites_data
    """
    station_coords = (station['latitude'].values[0], station['longitude'].values[0])
    station_id = lookup_stations(station_coords, radius=25, limit=10)
    metadata, history = get_historical_data(station_id)
    
    # Update session state
    st.session_state.history = history
    st.session_state.stn_name = metadata['name']
    st.session_state.stn_identifier = metadata['climate_identifier']
    st.session_state.stn_location = (float(metadata['latitude']), float(metadata['longitude']))


def lookup_stations(coordinates, radius, limit):
    """Retrieves the closest weather station site to the provided coordinates that has daily data.

    Args:
        coordinates (tuple): Latitude and longitude, formatted as a tuple
        radius (int): Distances in kilometers to search, from the center of the coordinates provided
        limit (int): The total number of records to return, value one of [10, 25, 50, 100]

    Returns:
        int: The historical station ID for the retrieved station.
    """
    stations = asyncio.run(get_historical_stations(coordinates, radius=radius, limit=limit))
    stations_df = pd.DataFrame.from_dict(stations)
    stations_df = process_station_dates(stations_df)
    station_id = choose_historical_station_id(stations_df)

    return int(station_id)

    
def get_historical_data(station_id):
    """Retrieves historical station metadata and weather data from a given historical station ID.

    Args:
        station_id (int): The historical station ID as provided by lookup_stations

    Returns:
        _type_: _description_
    """
    ec_en_csv = ECHistorical(station_id=station_id, year=2024, language='english', format='csv')
    asyncio.run(ec_en_csv.update())
    
    metadata = ec_en_csv.metadata
    df = pd.read_csv(ec_en_csv.station_data)
    
    return metadata, df
    
    
def update_displays(station):
    """Updates data tables in main app

    Args:
        station (DataFrame): A single DataFrame row that contains weather station information
        provided by load_sites_data.
    """
    
    display_conditions(station)
    display_historical(station)



### Main


st.sidebar.header('Canadian Weather Stations')

with st.spinner('Loading data...'):
# Load available WEATHER_SITES
    data_load_state = st.text('Loading data...')
    station_data = load_sites_data()
    data_load_state.text('')


provinces = ['AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT']

province_to_filter = st.sidebar.selectbox('Choose a province', provinces)
filtered_data = station_data[station_data['province codes'] == province_to_filter]

st.subheader(f'Locations of all stations in {province_to_filter}')
st.map(filtered_data)

stations_to_filter = st.sidebar.selectbox(f'Choose a station in {province_to_filter}', filtered_data['english names'])
station = filtered_data[filtered_data['english names'] == stations_to_filter]

if 'conditions' not in st.session_state:
    st.session_state.conditions = pd.DataFrame()
    
conditions_output_box = st.subheader('Current Conditions')
conditions_output_df = st.dataframe(st.session_state.conditions)

if 'history' not in st.session_state:
    st.session_state.history = pd.DataFrame()
    
history_output_box = st.subheader('Historical Data')

col1, col2, col3 = st.columns(3)

if 'stn_name' not in st.session_state:
    st.session_state.stn_name = ''
if 'stn_identifier' not in st.session_state:
    st.session_state.stn_identifier = ''
if 'stn_location' not in st.session_state:
    st.session_state.stn_location = ''


col1.caption('Station Name')
col1.text(st.session_state.stn_name)
col2.caption('Climate Identifier')
col2.text(st.session_state.stn_identifier)
col3.caption('Station Location')
col3.text(st.session_state.stn_location)

st.warning('Use the Refresh Data button on the sidebar to update this section.  Displays daily historical data closest to your chosen station.', icon='⚠️')

history_output_df = st.dataframe(st.session_state.history)

st.sidebar.button('Refresh Data',
                  on_click=update_displays(station),
                  )