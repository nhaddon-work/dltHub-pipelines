import dlt
import requests
import logging
import os
import bi_snowflake_connector
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Airflow env variables
api_key = os.environ.get('API_KEY')
schema = os.environ.get('SNOWFLAKE_SCHEMA')

# shared key and headers, uncomment when running locally
# api_key = dlt.secrets.get("sources.country_state_city.api_key")

# Set secrets here since we can't upload the secrets toml file
dlt.secrets["destination.snowflake.credentials.private_key_passphrase"] = os.environ["TARGET_SNOWFLAKE_PRIVATE_KEY_FILE_PWD"]

# Not sure why these can't be passed to the config file at all but trying this now
dlt.config["destination.snowflake.credentials.database"] = os.environ["SNOWFLAKE_DATABASE"]
dlt.config["destination.snowflake.credentials.schema"] = os.environ["SNOWFLAKE_SCHEMA"]
dlt.config["destination.snowflake.credentials.warehouse"] = os.environ["SNOWFLAKE_WAREHOUSE"]
dlt.config["destination.snowflake.credentials.role"] = os.environ["SNOWFLAKE_ROLE"]
dlt.config["destination.snowflake.credentials.username"] = os.environ["SNOWFLAKE_USER"]
dlt.config["destination.snowflake.credentials.account"] = os.environ["SNOWFLAKE_ACCOUNT"]
dlt.config["destination.snowflake.credentials.host"] = os.environ["SNOWFLAKE_HOST"]
dlt.config["destination.snowflake.credentials.private_key_path"] = os.environ["TARGET_SNOWFLAKE_PRIVATE_KEY_FILE"]

@dlt.resource
# Fetch the list of all countries with country details
def fetch_country_details():
    url = "https://api.countrystatecity.in/v1/countries"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        countries = response.json()
        for country in countries:
            iso2 = country.get("iso2")
            if iso2:
                detail_url = f"https://api.countrystatecity.in/v1/countries/{iso2}"
                detail_response = requests.get(detail_url, headers=headers)
                if detail_response.status_code == 200:
                    yield detail_response.json()
                else:
                    logging.error(f"Failed to fetch details for {iso2}. Status code: {detail_response.status_code}")
                    yield {"error": f"Failed to fetch details for {iso2}", "status_code": detail_response.status_code}
            else:
                logging.warning(f"Missing iso2 code in country data: {country}")
                yield {"error": "Missing iso2 code in country data", "country": country}
    else:
        logging.error(f"Failed to fetch countries list. Status code: {response.status_code}")
        yield {"error": "Failed to fetch countries list", "status_code": response.status_code}


def get_all_states(headers):
    """Fetches and returns the list of states only once."""
    url = "https://api.countrystatecity.in/v1/states"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        logging.info("Successfully fetched states data.")
        return response.json()
    else:
        logging.error(f"Failed to fetch states. Status code: {response.status_code}")
        return []


@dlt.resource
# Fetch the list of all states
def fetch_states(states_data):
    """Yields all states one by one from the global cache."""
    for state in states_data:
        yield state


@dlt.resource
def fetch_cities(states_data, headers):
    """Yields all cities based on the cached state list. Stops after 3 minutes. For testing only"""
    # start_time = time.time()
    # timeout_seconds = 180  # 3 minutes

    for index, state in enumerate(states_data):
    #     if time.time() - start_time > timeout_seconds:
    #         logging.warning("fetch_cities timed out after 3 minutes. Returning partial results.")
    #         break

        country_iso2 = state.get('country_code')
        state_iso2 = state.get('iso2')
        if not country_iso2 or not state_iso2:
            continue

        city_url = f"https://api.countrystatecity.in/v1/countries/{country_iso2}/states/{state_iso2}/cities"
        response = requests.get(city_url, headers=headers)
        logging.info(f"Index {index}: Fetching cities for {country_iso2}-{state_iso2}: {response.status_code}")

        if response.status_code == 200:
            cities = response.json()
            for city in cities:
                # For each city, add in another API here to get the population data as requested for CDP abstraction
                # using name, admin code and country to find the exact match in the other API
                # url = (
                #     "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
                #     "geonames-all-cities-with-a-population-1000/records"
                #     f"?where=name=\"{city['name']}\"%20AND%20country_code=\"{country_iso2}\"%20AND%20admin1_code=\"{state_iso2}\"&limit=1"
                # )
                #
                # population = None
                # try:
                #     resp = requests.get(url)
                #     if resp.status_code == 200:
                #         results = resp.json().get("results", [])
                #         if results:
                #             population = results[0].get("population")
                # except Exception as e:
                #     print(f"Error fetching population for {city['name']}: {e} country_code=\"{country_iso2}\" admin1_code=\"{state_iso2}")
                # Switching back to not including population for now to avoid extra API calls and complexity, use flat file for now as
                # this API call will require 15 hrs to finish in Dag

                yield {
                    'country_iso2': country_iso2,
                    'state_iso2': state_iso2,
                    'id': city['id'],
                    'city_name': city['name'],
                    'latitude': city.get('latitude'),
                    'longitude': city.get('longitude') #,
                    # 'population': population
                }
        else:
            logging.error(f"Failed to fetch cities for {country_iso2}-{state_iso2}. Status code: {response.status_code}")
            yield {
                "error": f"Failed to fetch cities for {country_iso2}-{state_iso2}",
                "status_code": response.status_code
            }

def truncate_table(schema, table):
    # snowflake connection
    snow_con = bi_snowflake_connector.connect(method='env')
    cursor = snow_con.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema}.{table}")
    cursor.close()
    snow_con.close()


# Helper to truncate + append (replace-without-staging)
def truncate_and_append(pipeline, source, table_name):
    # Truncate existing table to avoid dups being appended
    truncate_table(pipeline.dataset_name, table_name)
    # Append new snapshot without creating staging files and tables here in dlthub
    return pipeline.run(source, table_name=table_name, write_disposition="append")

if __name__ == "__main__":
    headers = {"X-CSCAPI-KEY": api_key}
    states_data = get_all_states(headers)

    pipeline = dlt.pipeline(
        pipeline_name="location_pipeline",
        destination='snowflake',  # Ensure your Snowflake credentials are set via env or .dlt/config.toml
        dataset_name=schema
    )


    # Country details
    country_info = truncate_and_append(pipeline, fetch_country_details(), "country_details")
    logging.info(f"Country Details Load Info: {country_info}")

    # States
    state_info = truncate_and_append(pipeline, fetch_states(states_data), "states")
    logging.info(f"States Load Info: {state_info}")

    # Cities
    city_info = truncate_and_append(pipeline, fetch_cities(states_data, headers), "cities")
    logging.info(f"Cities Load Info: {city_info}")