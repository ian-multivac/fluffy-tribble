# Weather Stations

This Streamlit app fetches and displays weather conditions from Canadian weather stations.  It uses the [env-canada](https://pypi.org/project/env-canada/) to retrieve current and historical weather conditions from [Environment Canada's repositories](https://dd.weather.gc.ca).

## Getting Started

Set up a virtual environment and activate it:

```bash
python3 -m venv env
source env/bin/activate
```

Then, install the required libraries:

```bash
pip install -r requirements.txt
```

To run the app, use

```bash
streamlit run weather_stations.py
```
