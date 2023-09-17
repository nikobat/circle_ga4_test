import streamlit as st
import pandas as pd
import os
import json
import tempfile
from google.cloud import bigquery
from io import StringIO

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'teamcircle-399006-af8e226fd4ff.json'

st.set_page_config(layout="wide", page_icon="🛠️", page_title="GA4 RULZ")
st.title("Getting stuff out of GA4")
st.subheader("This will make it easy and useful")

if  os.path.isfile('teamcircle-399006-af8e226fd4ff.json') == True:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'teamcircle-399006-af8e226fd4ff.json'
else:
    json_file = st.file_uploader("Drop your JSON here", type="json")
if not json_file:
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

with tempfile.NamedTemporaryFile(delete=False) as fp:
    fp.write(json_file.getvalue())
try:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = fp.name          
    with open(fp.name,'rb') as a:
  
         client = bigquery.Client()
finally:
          if os.path.isfile(fp.name):
              os.unlink(fp.name)




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

# Define the table patterns to query
table_patterns = ["events_2023*", "events_intraday_2023*"]
user_table_pattern = "pseudonymous_users_2023*"

# Get the unique keys and their types
keys_and_types = get_unique_keys_and_types(client, project_id, dataset_id, table_patterns)