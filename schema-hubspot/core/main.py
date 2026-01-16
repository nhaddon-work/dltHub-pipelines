import dlt
import os
import logging
from hubspot import hubspot

# Set secrets here since we can't upload the secrets toml file
dlt.secrets["destination.snowflake.credentials.password"] = os.environ["SNOWFLAKE_PASSWORD"]
dlt.secrets["hubspot.api_key"] = os.environ["API_KEY"]

# Not sure why these can't be passed to the config file at all but trying this now
dlt.config["destination.snowflake.credentials.database"] = os.environ["SNOWFLAKE_DATABASE"]
dlt.config["destination.snowflake.credentials.schema"] = os.environ["SNOWFLAKE_SCHEMA"]
dlt.config["destination.snowflake.credentials.warehouse"] = os.environ["SNOWFLAKE_WAREHOUSE"]
dlt.config["destination.snowflake.credentials.role"] = os.environ["SNOWFLAKE_ROLE"]
dlt.config["destination.snowflake.credentials.username"] = os.environ["SNOWFLAKE_USER"]
dlt.config["destination.snowflake.credentials.host"] = os.environ["SNOWFLAKE_HOST"]
dlt.config["destination.snowflake.credentials.account"] = os.environ["SNOWFLAKE_ACCOUNT"]

# Log the Snowflake host and account values
logging.info(f"Snowflake Host: {dlt.config['destination.snowflake.credentials.host']}")
logging.info(f"Snowflake Account: {dlt.config['destination.snowflake.credentials.account']}")
schema = os.environ.get('SNOWFLAKE_SCHEMA')

destination = dlt.destinations.snowflake(
    # Set the staging table schema to a fixed location so dltHub doesn't auto-create staging schemas elsewhere
    staging_dataset_name_layout="staging"
)

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="hubspot",
        dataset_name=schema,
        destination=destination
    )
    # This prints out all the available resources
    # print(hubspot().resources.keys())
    load_data = hubspot().with_resources("companies", "owners", "deals")
    # refresh the pipeline state manually just once with
    load_info = pipeline.run(load_data)
    print(load_info)
