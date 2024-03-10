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
def load_data():
    data = pd.read_csv(WEATHER_SITES_URL, header=1)
    lowercase = lambda x: str(x).lower()
    data.rename(lowercase, axis='columns', inplace=True)
    
    data['latitude'] = data['latitude'].str[:-1].astype(float)
    data['longitude'] = data['longitude'].str[:-1].astype(float)
    data['longitude'] = data['longitude'].multiply(-1)
    
    return data


def split_date_range(range_string):
    
    return pd.Series(range_string.split('|', 1))



def process_station_dates(df):
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
    # filter only those that have daily
    selected_stations = df[df['daily_data_end'] == dt.datetime.strftime((dt.datetime.today() - dt.timedelta(days=1)), format='%Y-%m-%d')]
    selected_stations = selected_stations.sort_values(by='proximity', ascending=True)
    selected_station = selected_stations['id'].iloc[0]
    
    return selected_station

    
def get_conditions(station_id):
    # station_id='ON/s0000430'
    weather = ECWeather(station_id=station_id, language='english')
    asyncio.run(weather.update())
    
    return weather.conditions


def format_conditions_data(conditions_data):
    
    df = pd.DataFrame()
    df['label'] = conditions_data['label']
    df['value'] = conditions_data['value']
    df['unit'] = conditions_data['unit']
    
    return df
    
    
def display_conditions(station):
    
    station_id = f"{station['province codes'].values[0]}/{station['codes'].values[0]}"
    conditions = get_conditions(station_id)
    conditions_data = pd.DataFrame.from_dict(conditions).T
    
    conditions_data = format_conditions_data(conditions_data)
    
    st.session_state.conditions = conditions_data

    
def display_historical(station_id):
    station_coords = (station['latitude'].values[0], station['longitude'].values[0])
    station_id = lookup_stations(station_coords, radius=25, limit=10)
    metadata, st.session_state.history = get_historical_data(station_id)
    
    st.session_state.stn_name = metadata['name']
    st.session_state.stn_identifier = metadata['climate_identifier']
    st.session_state.stn_location = (float(metadata['latitude']), float(metadata['longitude']))


def lookup_stations(coordinates, radius, limit):
    stations = asyncio.run(get_historical_stations(coordinates, radius=radius, limit=limit))
    stations_df = pd.DataFrame.from_dict(stations)
    stations_df = process_station_dates(stations_df)
    station_id = choose_historical_station_id(stations_df)

    return int(station_id)

    
def get_historical_data(station_id):
    ec_en_csv = ECHistorical(station_id=station_id, year=2024, language='english', format='csv')
    asyncio.run(ec_en_csv.update())
    
    metadata = ec_en_csv.metadata
    df = pd.read_csv(ec_en_csv.station_data)
    
    return metadata, df
    
    
def update_displays(station):
    
    display_conditions(station)
    display_historical(station)


st.sidebar.header('Canadian Weather Stations')

with st.spinner('Loading data...'):
# Create a text element and let the user know the data is loading.
    data_load_state = st.text('Loading data...')
    # Load 10,000 rows of data into the dataframe.
    station_data = load_data()
    # Notify the user that the data was successfully loaded.
    data_load_state.text('')


provs = ['AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT']

prov_to_filter = st.sidebar.selectbox('Choose a province', provs)
filtered_data = station_data[station_data['province codes'] == prov_to_filter]

st.subheader(f'Locations of all stations in {prov_to_filter}')
st.map(filtered_data)

stations_to_filter = st.sidebar.selectbox(f'Choose a station in {prov_to_filter}', filtered_data['english names'])
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





