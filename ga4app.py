import streamlit as st
import pandas as pd
import os
import json
import tempfile
import logging

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO, filename='script.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


st.set_page_config(layout="wide", page_icon="🛠️", page_title="GA4 RULZ")
st.title("Getting stuff out of GA4")
st.subheader("This will make it easy and useful")

if  os.path.isfile('teamcircle-399006-af8e226fd4ff.json') == True:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'teamcircle-399006-af8e226fd4ff.json'
    client = bigquery.Client()
else:
    json_file = st.file_uploader("Drop your JSON here", type="json")
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        fp.write(json_file.getvalue())
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = fp.name          
        with open(fp.name,'rb') as a:
             client = bigquery.Client()
    finally:
          if os.path.isfile(fp.name):
              os.unlink(fp.name)
if not os.environ['GOOGLE_APPLICATION_CREDENTIALS'] :
    st.info("Upload JSON Authenticaor to continue")
    st.stop()

# Get IDs for Project before continuing
if "PROJECT_ID" in st.secrets:
    project_id = st.secrets["PROJECT_ID"]
else:
    project_id = st.sidebar.text_input("Project ID")
if not project_id:
    st.info("Enter a Project ID to continue")
    st.stop()

if "DATASET_ID" in st.secrets:
    dataset_id = st.secrets["DATASET_ID"]
else:
    dataset_id = st.sidebar.text_input("Dataset ID")
if not dataset_id:
    st.info("Enter a Dataset ID to continue")
    st.stop()

view_names = "user_table_view", "event_table_view"
table_patterns = "events_*", "events_intraday_*"
user_table_pattern = "pseudonymous_users_*"


def get_unique_keys_and_types(client, project_id, dataset_id, table_patterns):
    print("Getting unique keys and their types...")
    union_subqueries = [
        f"""
        SELECT key, 
               IF(ep.value.string_value IS NOT NULL, 'string', 
                  IF(ep.value.int_value IS NOT NULL, 'int', 
                     IF(ep.value.float_value IS NOT NULL, 'float', NULL)
                  )
               ) AS value_type
        FROM `{project_id}.{dataset_id}.{table_pattern}`,
        UNNEST(event_params) AS ep
        """
        for table_pattern in table_patterns
    ]
    query = " UNION ALL ".join(union_subqueries) + " GROUP BY key, value_type"
    query_job = client.query(query)
    keys_and_types = query_job.result()
    st.write("Unique keys and types retrieved successfully.")
    return {row.key: row.value_type for row in keys_and_types}

def generate_event_table_query(keys_and_types, project_id, dataset_id, table_patterns):
    logging.info("Generating the event table query...")
    
    pivot_sections = []
    for key, value_type in keys_and_types.items():
        column_alias = "event_param_" + key.replace("-", "_")  # Ensure valid SQL identifier
        if value_type == 'string':
            pivot_sections.append(f"MAX(IF(key = '{key}', string_value, NULL)) AS {column_alias}")
        elif value_type == 'int':
            pivot_sections.append(f"MAX(IF(key = '{key}', int_value, NULL)) AS {column_alias}")
        elif value_type == 'float':
            pivot_sections.append(f"MAX(IF(key = '{key}', float_value, NULL)) AS {column_alias}")

    pivot_sql = ",\n".join(pivot_sections)
    
    union_subqueries = [
        f"""
        SELECT
            TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
            user_pseudo_id,
            event_name,
            platform AS event_platform,
            stream_id AS event_stream_id,
            traffic_source.source AS traffic_source,
            traffic_source.medium AS traffic_medium,
            traffic_source.name AS traffic_name,
            geo.country AS event_geo_country,
            geo.region AS event_geo_region,
            geo.city AS event_geo_city,
            geo.sub_continent AS event_geo_sub_continent,
            geo.metro AS event_geo_metro,
            geo.continent AS event_geo_continent,
            device.browser AS event_device_browser,
            device.language AS event_device_language,
            device.is_limited_ad_tracking AS event_device_is_limited_ad_tracking,
            device.mobile_model_name AS event_device_mobile_model_name,
            device.mobile_marketing_name AS event_device_mobile_marketing_name,
            device.mobile_os_hardware_model AS event_device_mobile_os_hardware_model,
            device.operating_system AS event_device_operating_system,
            device.operating_system_version AS event_device_operating_system_version,
            device.category AS event_device_category,
            device.mobile_brand_name AS event_device_mobile_brand_name,
            user_first_touch_timestamp AS event_user_first_touch_timestamp,
            user_ltv.revenue AS event_user_ltv_revenue,
            user_ltv.currency AS event_user_ltv_currency,
            device.web_info.browser AS web_info_browser,
            device.web_info.browser_version AS web_info_browser_version,
            device.web_info.hostname AS web_info_hostname,
            ep.key AS key,
            ep.value.string_value AS string_value,
            ep.value.int_value AS int_value,
            ep.value.float_value AS float_value
        FROM 
            `{project_id}.{dataset_id}.{table_pattern}`,
            UNNEST(event_params) AS ep
        """
        for table_pattern in table_patterns
    ]

    sql_query = f"""
    WITH expanded AS (
        {" UNION ALL ".join(union_subqueries)}
    ),
    pivot_table AS (
        SELECT 
            event_timestamp,
            user_pseudo_id,
            event_name,
            event_platform,
            event_stream_id,
            traffic_source,
            traffic_medium,
            traffic_name,
            event_geo_country,
            event_geo_region,
            event_geo_city,
            event_geo_sub_continent,
            event_geo_metro,
            event_geo_continent,
            event_device_browser,
            event_device_language,
            event_device_is_limited_ad_tracking,
            event_device_mobile_model_name,
            event_device_mobile_marketing_name,
            event_device_mobile_os_hardware_model,
            event_device_operating_system,
            event_device_operating_system_version,
            event_device_category,
            event_device_mobile_brand_name,
            event_user_first_touch_timestamp,
            event_user_ltv_revenue,
            event_user_ltv_currency,
            web_info_browser,
            web_info_browser_version,
            web_info_hostname,
            {pivot_sql}
        FROM 
            expanded
       GROUP BY 
    event_timestamp, user_pseudo_id, event_name, event_platform, event_stream_id, traffic_source, traffic_medium, traffic_name, event_geo_country, event_geo_region, event_geo_city, event_geo_sub_continent, event_geo_metro, event_geo_continent, event_device_browser, event_device_language, event_device_is_limited_ad_tracking, event_device_mobile_model_name, event_device_mobile_marketing_name, event_device_mobile_os_hardware_model, event_device_operating_system, event_device_operating_system_version, event_device_category, event_device_mobile_brand_name, event_user_first_touch_timestamp, event_user_ltv_revenue, event_user_ltv_currency, web_info_browser, web_info_browser_version, web_info_hostname
)
    SELECT 
        * 
    FROM 
        pivot_table
    """
    return sql_query

    # ... (rest of the generate_event_table_query function logic here)
    
    logging.info("Event table query generated successfully.")

def generate_user_table_query(project_id, dataset_id, user_table_pattern):
    logging.info("Generating the user table query...")
    
    sql_query = f"""
    SELECT
        pseudo_user_id AS user_pseudo_id,
        stream_id AS user_stream_id,
        user_info.last_active_timestamp_micros AS user_last_active_timestamp,
        user_info.user_first_touch_timestamp_micros AS user_first_touch_timestamp,
        user_info.first_purchase_date AS user_first_purchase_date,
        device.operating_system AS user_device_operating_system,
        device.category AS user_device_category,
        device.mobile_brand_name AS user_device_mobile_brand_name,
        device.mobile_model_name AS user_device_mobile_model_name,
        device.unified_screen_name AS user_device_unified_screen_name,
        geo.city AS user_geo_city,
        geo.country AS user_geo_country,
        geo.continent AS user_geo_continent,
        geo.region AS user_geo_region,
        user_ltv.revenue_in_usd AS user_ltv_revenue_in_usd,
        user_ltv.sessions AS user_ltv_sessions,
        user_ltv.engagement_time_millis AS user_ltv_engagement_time,
        user_ltv.purchases AS user_ltv_purchases,
        user_ltv.engaged_sessions AS user_ltv_engaged_sessions,
        user_ltv.session_duration_micros AS user_ltv_session_duration,
        predictions.in_app_purchase_score_7d AS user_prediction_in_app_purchase_score_7d,
        predictions.purchase_score_7d AS user_prediction_purchase_score_7d,
        predictions.churn_score_7d AS user_prediction_churn_score_7d,
        predictions.revenue_28d_in_usd AS user_prediction_revenue_28d,
        occurrence_date AS user_occurrence_date,
        last_updated_date AS user_last_updated_date,
    FROM 
        `{project_id}.{dataset_id}.{user_table_pattern}`

    """
    return sql_query

# Function to retrieve schema columns
def get_schema_columns(client, project_id, dataset_id, table_name):
    query = f"SELECT column_name FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table_name}'"
    query_job = client.query(query)
    return [row for row in query_job.result()]

def get_distinct_counts(client, project_id, dataset_id, view_name):
    try:
        # Retrieve the column names in the view
        view_columns = [column.column_name for column in get_schema_columns(client, project_id, dataset_id, view_name)]
        if not view_columns:
            logging.error(f"No columns found in the view: {view_name}")
            return {}

        # Create a query to get distinct counts for each column
        distinct_count_query = ", ".join([f"COUNT(DISTINCT {col}) AS {col}" for col in view_columns])
        query = f"""
        SELECT
            {distinct_count_query}
        FROM
            `{project_id}.{dataset_id}.{view_name}`
        """
        
        query_job = client.query(query)
        result = query_job.result()

        # Convert result to a dictionary
        for row in result:
            distinct_counts = dict(row.items())
            logging.info(f"Distinct counts for view {view_name}: {distinct_counts}")
            return distinct_counts
    except Exception as e:
        logging.error(f"An error occurred while getting distinct counts for {view_name}: {e}")
        return {}

def identify_useless_columns(distinct_counts):
    useless_columns = [column for column, count in distinct_counts.items() if count in (0, 1)]
    logging.info(f"Identified columns with 0 or 1 distinct values: {useless_columns}")
    return useless_columns

def create_updated_view(client, project_id, dataset_id, view_name, columns_to_exclude):
    try:
        if columns_to_exclude:
            # Retrieve all column names
            view_columns = [column.column_name for column in get_schema_columns(client, project_id, dataset_id, view_name)]
            
            # Generate the SELECT statement for the updated view excluding the identified columns
            select_statement = ", ".join([col for col in view_columns if col not in columns_to_exclude])

            # Create or replace the view with the updated SELECT statement
            query = f"""
            CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{view_name}_mini` AS
            SELECT
                {select_statement}
            FROM
                `{project_id}.{dataset_id}.{view_name}`
            """
            query_job = client.query(query)
            query_job.result()
            logging.info(f"Excluded columns with unique counts of 0 or 1 from the view: {', '.join(columns_to_exclude)}")
        else:
            logging.info(f"No columns to exclude in the view: {view_name}")
    except Exception as e:
        logging.error(f"An error occurred while creating the updated view for {view_name}: {e}")

# Your create_summary_statistics function remains the same

def create_summary_statistics(client, project_id, dataset_id, view_names):
    logging.info("Creating summary statistics...")

    for view_name in view_names:
        logging.info(f"Creating summary statistics for view: {view_name}...")
        distinct_counts = get_distinct_counts(client, project_id, dataset_id, view_name)
        columns_to_exclude = identify_useless_columns(distinct_counts)
        create_updated_view(client, project_id, dataset_id, view_name, columns_to_exclude)


def create_or_replace_view(client, project_id, dataset_id, view_name, query):
    logging.info(f"Creating/Modifying view: {view_name}...")
    view_id = f"{project_id}.{dataset_id}.{view_name}"

    view = bigquery.Table(view_id)
    view.view_query = query

    try:
        # Check if the view already exists
        client.get_table(view_id)
        view_exists = True
    except NotFound:
        view_exists = False

    try:
        if view_exists:
            view = client.update_table(view, ["view_query"])
            logging.info(f"Modified view {view_id} successfully.")
        else:
            view = client.create_table(view)
            logging.info(f"Created view {view_id} successfully.")
    except BadRequest as e:
        logging.error("Error: Bad request (e.g., schema or query issue). Details: %s", e)
    except GoogleAPICallError as e:
        logging.error("Error: API call failed. Details: %s", e)
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)

def create_user_table_view(client, project_id, dataset_id, user_table_pattern):
    user_table_query = generate_user_table_query(project_id, dataset_id, user_table_pattern)
    create_or_replace_view(client, project_id, dataset_id, "user_table_view", user_table_query)

def create_event_table_view(client, project_id, dataset_id, table_patterns, keys_and_types):
    event_table_query = generate_event_table_query(keys_and_types, project_id, dataset_id, table_patterns)
    create_or_replace_view(client, project_id, dataset_id, "event_table_view", event_table_query)



#This is where things are run
keys_and_types = get_unique_keys_and_types(client, project_id, dataset_id, table_patterns)
if keys_and_types:
    st.write("Retrieved keys and types:")#, keys_and_types)
    generate_event_table_query(keys_and_types, project_id, dataset_id, table_patterns)
    st.write("generate_event_table_query")
    create_user_table_view(client, project_id, dataset_id, user_table_pattern)
    st.write("create_user_table_view")
    create_event_table_view(client, project_id, dataset_id, table_patterns, keys_and_types)
    st.write("create_event_table_view")
    create_summary_statistics(client, project_id, dataset_id, view_names)
    st.write("create_summary_statistics")
    st.write("DONEZ0!")
else:
    st.write("Failed to retrieve keys and types.")