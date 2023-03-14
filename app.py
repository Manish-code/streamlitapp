import streamlit as st
import snowflake.connector
import pandas as pd

# Initialize connection.
# Uses st.secrets to get credentials from Streamlit's secrets management feature.
# @st.cache(allow_output_mutation=True, suppress_st_warning=True)
def init_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
    )

conn = init_connection()

# Perform query.
# Uses st.cache to only rerun when the query or connection changes or after 10 min.
@st.cache(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

# Get user input for query.
query = st.text_input("Enter a Snowflake SQL query", "SELECT * FROM mytable;")

# Run the query and display results.
try:
    rows = run_query(query)
    for row in rows:
        st.write(row)
except Exception as e:
    st.error(f"Error: {str(e)}")





file = st.file_uploader("Upload a CSV file", type=["csv"])
if file is not None:
    df = pd.read_csv(file)

    # Display the DataFrame in a Streamlit table.
    st.write(df)

query2 = st.text_input("Enter a Snowflake SQL query", "SELECT * FROM mytable;",key=2)

# Run the query and display results.
try:
    rows = run_query(query2)
    for row in rows:
        st.write(row)
except Exception as e:
    st.error(f"Error: {str(e)}")
