import pandas as pd
import requests
import lxml.etree as ET
import base64
import io
import zipfile
import os
from pathlib import Path

# Used to convert the reportnames from XML to dataframe
def __xml_to_df(xml_data : str):
    # Parse the XML data
    root = ET.fromstring(xml_data)
    # print(data)
    
    namespaces = {
    'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'ns': 'http://xmlns.oracle.com/oxp/service/v2'
    }
    
    items = root.findall('.//ns:item', namespaces)
    # print(items)

    data = []
    for item in items:
        item_data = {}
        for child in item:
            # Get the tag name without namespace            
            tag = ET.QName(child).localname            
            item_data[tag] = child.text            
        data.append(item_data)

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data)
    return df

# Used to get the list of files and folders from specified folder path
def __get_folder_content(url : str , username : str , password : str , folder_path : str):
    url = f'{url}//xmlpserver/services/v2/CatalogService?wsdl'    
    headers = {'Content-Type': 'application/soap+xml'}
    
    payload = f"""
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">
                    <soapenv:Header/>
                        <soapenv:Body>
                            <v2:getFolderContents>
                                <v2:folderAbsolutePath>{folder_path}</v2:folderAbsolutePath>
                                <v2:userID>{username}</v2:userID>
                                <v2:password>{password}</v2:password>
                            </v2:getFolderContents>
                        </soapenv:Body>
                </soapenv:Envelope>
                """
                
    
    response = requests.request(method='POST', url=url, data=payload, headers=headers, auth=(username, password))
    
    if response.status_code != 200:
        # raise Exception(f"Error: {response.status_code} - {response.text}")
        print(f"Error: {response.status_code} - {response.text}")
        return pd.DataFrame()
    
    data = __xml_to_df(response.text)
    return data
    
    
    
def get_report_data(url : str , username : str , password : str , folder_path : str):
    report_data =__get_folder_content(url , username , password , folder_path)
    if report_data is None or report_data.empty:
         raise ValueError("No reports found")
    
    folder_data = report_data[report_data['type'] == 'Folder'] 
    
    folder_stack = folder_data['absolutePath'].to_list()    
    while len(folder_stack) >0:
        current_folder = folder_stack.pop()
        # print(current_folder)
        subfolder_data = __get_folder_content(url, username, password, current_folder)            
        if subfolder_data.shape[0] > 0:
            report_data = pd.concat([report_data, subfolder_data] , ignore_index= True)
            subfolders = subfolder_data[subfolder_data['type'] == 'Folder']['absolutePath'].tolist()
            folder_stack.extend(subfolders)                        
    
    return report_data
    
        
################################

def __make_report_directory (report_display_name : str):
    directory_path = r"..\reports\{}".format(report_display_name)
    
    try:
        os.makedirs(directory_path, exist_ok=True)  # Creates the directory and any necessary parent directories    
    except Exception as e:
        raise ValueError(f"Error while creating directory  {e}")
    return directory_path


def __get_data_model_name(xml_data: str):
    datamodel = None
    root = ET.fromstring(xml_data)
    # Define the namespace
    namespace = {'ns': 'http://xmlns.oracle.com/oxp/xmlp'}
    # Find the dataModel element
    data_model_elem = root.find('ns:dataModel', namespace)
    # Get the 'url' attribute
    if data_model_elem is not None:
        datamodel = data_model_elem.get('url')
    return datamodel


def __get_query(xdm_data:str):
    root = ET.fromstring(xdm_data)

# Define the namespace (from the XML)
    namespace = {'ns': 'http://xmlns.oracle.com/oxp/xmlp'}

    # Find all sql elements in the XML
    sql_elements = root.findall('.//ns:sql', namespace)

    # Extract SQL queries from CDATA sections
    queries = []
    for sql in sql_elements:        
        cdata = sql.text.strip() if sql.text else ''
        queries.append(cdata)        
    return queries


def __get_query_frm_DM(dm_binary_data ) :
    zipobject = io.BytesIO(dm_binary_data)        
    xdm_data = None
    queries = None
    with zipfile.ZipFile(zipobject, 'r') as zip_file:
        # List all files in the ZIP archive
        for file_name in zip_file.namelist():
            # Check if the file has a .xdo extension
            if file_name.endswith('.xdm'):                
                # Read the .xdo file from the ZIP archive
                with zip_file.open(file_name) as file:
                    xdm_data = file.read()
                                                    
                break  # Exit the loop after finding the first .xdo file
    if xdm_data is not None:
        queries = __get_query(xdm_data)
    return queries


def __download_object(url : str , username : str , password : str , report_name : str , display_name : str , directroty : str ):
    url = f'{url}//xmlpserver/services/v2/CatalogService?wsdl'    
    headers = {'Content-Type': 'application/soap+xml'}
    
    payload = f"""
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v2="http://xmlns.oracle.com/oxp/service/v2">
                <soapenv:Header/>
                    <soapenv:Body>
                        <v2:downloadObject>
                            <v2:reportAbsolutePath>{report_name}</v2:reportAbsolutePath>
                            <v2:userID>{username}</v2:userID>
                            <v2:password>{password}</v2:password>
                        </v2:downloadObject>
                    </soapenv:Body>
                </soapenv:Envelope>
                """
                
    
    soap_response = requests.request(method='POST', url=url, data=payload, headers=headers, auth=(username, password))
    
    if soap_response.status_code != 200:
        raise Exception(f"Error: {soap_response.status_code} - {soap_response.text}")
    soap_response = soap_response.text
    
    start_tag = '<downloadObjectReturn>'
    end_tag = '</downloadObjectReturn>'
    
    start_index = soap_response.find(start_tag) + len(start_tag)
    end_index = soap_response.find(end_tag)
    
    # Extract the Base64 string
    base64_string = soap_response[start_index:end_index]
    
    # Decode Base64 string to binary data
    binary_data = base64.b64decode(base64_string)
        
    
    # with open(r'{}\{}.zip'.format(directroty , display_name), 'wb') as zip_file:
    #         zip_file.write(binary_data)
        
            
    return binary_data


def download_report_data(url : str , username : str , password : str , report_name : str , display_name : str ) -> tuple:
    
    # directory_path = __make_report_directory(display_name)
    directory_path = None
    
    binary_data = None
    
    dm_queries = []
    
    try:    
        # print(f"report name is {report_name}")
        binary_data = __download_object(url , username , password , report_name , display_name , directory_path )
    except Exception as e:
        raise ValueError(f"Error while downloading object {e}")
        
    
    zipobject = io.BytesIO(binary_data)    
    datamodel_name = None
    with zipfile.ZipFile(zipobject, 'r') as zip_file:
        # List all files in the ZIP archive
        for file_name in zip_file.namelist():
            # Check if the file has a .xdo extension
            if file_name.endswith('.xdo'):                
                # Read the .xdo file from the ZIP archive
                with zip_file.open(file_name) as file:
                    xdo_data = file.read()
                                    
                datamodel_name = __get_data_model_name(xdo_data)                
                break  # Exit the loop after finding the first .xdo file    
    if datamodel_name is not None:
        dm_display_name = Path(datamodel_name)
        dm_display_name = dm_display_name.stem
        dm_data = None
        try:
            dm_data = __download_object(url , username , password , datamodel_name , dm_display_name , directory_path)
            dm_queries = __get_query_frm_DM(dm_data )
        except Exception as e:
            print(e)
    return dm_queries , datamodel_name                                                