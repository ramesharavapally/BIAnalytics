import streamlit as st
import pandas as pd
import requests
import lxml.etree as ET
import base64
import io
import zipfile
import os
from pathlib import Path
import configparser
from report_util import download_report_data , get_report_data





# Set page configuration to wide mode by default
st.set_page_config(layout="wide" )
pd.set_option("styler.render.max_elements", 50000000)

# st.markdown(
#         """
#         <style>                
#         /* Define custom font size and family */
#         textarea {
#             color: rgb(0, 0, 139) !important;                    
#             font-size: 14px !important;
#             font-family: "Source Code Pro", monospace !important;
#             font-optical-sizing: auto !important;            
#         }                
#         </style>
#         """,
#         unsafe_allow_html=True
#     )

CONFIG_FILE = 'config.ini'


    

def get_report_name():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['DEFAULT']['report_path']

def get_datamodel_name():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config['DEFAULT']['datamodel_path']

# Function to save or update connection details to a properties file
def save_or_update_connection_details(url, username, password, connection_name):
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    if connection_name in config:
        # Update existing connection details
        config[connection_name] = {'url': url, 'username': username, 'password': password}
        st.sidebar.success(f"Updated connection details for {connection_name}")
    else:
        # Save new connection details
        config[connection_name] = {'url': url, 'username': username, 'password': password}
        st.sidebar.success(f"Created new connection: {connection_name}")

    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)
    
    # Refresh the list of saved connections and select the newly created connection    
    # st.experimental_rerun()

# Function to load saved connections from the properties file
def load_saved_connections():
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)    
    return config.sections()

# Function to get connection details based on the selected connection
def get_connection_details(connection_name):
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    return config[connection_name]['url'], config[connection_name]['username'], config[connection_name]['password']

# @st.cache_resource
def get_report_dataframe(url , username , passsword , report_path):
    analytics_data = get_report_data(url , username , passsword , report_path)        
    report_data = analytics_data[analytics_data['type'] == 'Report']
    report_data = report_data.reset_index(drop= True)
    if 'csv_data' not in st.session_state:
        st.session_state.csv_data = pd.DataFrame()
    st.session_state.csv_data = report_data
    
def initialize_session_state():
    # Get connection details based on the selected connection
    if 'selected_connection' not in st.session_state:
        st.session_state.selected_connection = ""
        
    if 'csv_data' not in st.session_state:
        st.session_state.csv_data = pd.DataFrame()
    
    if 'report_directory' not in st.session_state:
        st.session_state.report_directory = ''

# Main function
def main():
    # Input fields for selecting saved connections     
    saved_connections = load_saved_connections()    
    selected_connection = st.sidebar.selectbox('Select Connection:',  saved_connections)
    connection_name = selected_connection

    initialize_session_state()
        
    if selected_connection:        
        url, username, password  = get_connection_details(selected_connection) 
        if selected_connection != st.session_state.selected_connection :  
            try:          
                # create_report(url , username , password , get_datamodel_name() , get_report_name())
                pass
            except Exception as e:
                st.error(f"Error occured while creating DM {e}")
        # st.session_state.selected_connection = selected_connection
    else:
        url, username, password , connection_name= '', '', '' ,''        

    # Input fields for URL, username, and password
    connection_name = st.sidebar.text_input('Connection Name', value=connection_name)
    url_input = st.sidebar.text_input('API URL', value=url)
    username_input = st.sidebar.text_input('Username', value=username)
    password_input = st.sidebar.text_input('Password', type='password', value=password)      
    
    

    # Save button to store or update connection details
    if st.sidebar.button('Save'):
        save_or_update_connection_details(url_input, username_input, password_input, connection_name)      
        # saved_connections = load_saved_connections()
        # st.experimental_rerun()        
        try:                        
            pass
        except Exception as e:
            st.error(f"Error occured while creating DM in Save  {e}")                    
    
    report_directory = st.sidebar.text_input('Reports Directory' , value='/Custom/py_sql')

    st.write(f'Connection name : {connection_name}')
        
    
    if connection_name != st.session_state.selected_connection or report_directory != st.session_state.report_directory:
        get_report_dataframe(url_input , username_input , password_input , report_directory)
        
    report_list_df = st.session_state.csv_data
    
    if report_list_df is not None and not report_list_df.empty:
        # st.dataframe(st.session_state.csv_data.style.set_caption("Decoded CSV Data").set_table_styles([{
        #         'selector': 'th',
        #         'props': [('font-weight', 'bold')]
        #         }]), width=5000)       
        
        col1, col2 = st.columns([0.3, 0.7])
        with col1:        
            st.write(f'report count {len(report_list_df)}')
            selected_report = st.selectbox('Select Report:',  report_list_df['displayName'])
        with col2:
            st.write('\n\n')
            st.write('\n\n')
            st.write('\n\n')
            _rep_details = report_list_df[report_list_df['displayName'] == selected_report][['fileName','creationDate','lastModifier','lastModified','absolutePath']]            
            st.write(_rep_details)
            # st.write(_rep_details.iloc[0``]['absolutePath'])
        
        dm_queries , datamodel_name = download_report_data(url_input , username_input , password_input , _rep_details.iloc[0]['absolutePath'] , selected_report )
        st.write(f"Datamodel : **{datamodel_name}**")
        for i, query in enumerate(dm_queries):
            st.write(f"query {i}")
            st.code(query , language='sql')
        
    else:
        st.error(f"No reports found in the directory")                    
    st.session_state.selected_connection = selected_connection 
    st.session_state.report_directory = report_directory    
    # datamodel_data = analytics_data[analytics_data['type'] == 'DataModel']
    # folder_data = analytics_data[analytics_data['type'] == 'Folder']
    

if __name__ == '__main__':
    main()


