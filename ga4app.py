import streamlit as st
import pandas as pd
import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'teamcircle-399006-af8e226fd4ff.json'

# st.set_page_config(layout="wide", page_icon="🛠️", page_title="GA4 RULZ")
# st.title("Getting stuff out of GA4")
# st.subheader("This will make it easy and useful")

# st.sidebar.text_input("Something for JSON")

client = bigquery.Client()

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
    print("Unique keys and types retrieved successfully.")
    return {row.key: row.value_type for row in keys_and_types}

get_unique_keys_and_types