#!/usr/bin/env python
# coding: utf-8

# In[1]:


from download_with_session_ID import *
import requests
import pandas as pd
import dateutil.parser
import os,subprocess
import requests
from urllib.parse import urlparse
username = 'atulkumar'
password = 'Mrityor1!'
host_name='https://snipr.wustl.edu'
URI=f'/data/experiments/'


# In[ ]:


import pandas as pd
import requests
import dateutil.parser
import concurrent.futures
username = 'atulkumar'
password = 'Mrityor1!'
host_name='https://snipr02.nrg.wustl.edu' #'https://snipr.wustl.edu'
URI=f'/data/experiments/'
resource_dir = 'NIFTI_LOCATION'
# resource_dir = 'NIFTI_LOCATION'
ibio_session_df = pd.read_csv('IBIO_sessions.csv') 
# Dummy function for get_resourcefiles_metadata
# def get_resourcefiles_metadata(uri, resource_dir):
#     # Replace with actual logic to get metadata
#     return [{'URI': f'{URI}{resource_dir}.csv'}]

# Define the compute_value function
def compute_value(each_row):
    resource_metadata = get_resourcefiles_metadata(str(each_row['URI']), resource_dir)
    url = host_name + str(resource_metadata[0]['URI'])
    print(each_row['URI'])
    print(url)
    
    try:
        r = requests.get(url, auth=(username, password))
        filename = f'temp_{each_row["ID"]}.csv'
        if r.status_code == 200:
            with open(filename, 'wb') as out:
                for bits in r.iter_content():
                    out.write(bits)
            response = requests.head(url)
            last_modified = response.headers.get('Last-Modified')
            if last_modified:
                last_modified = dateutil.parser.parse(last_modified)
                ibio_session_df.loc[ibio_session_df['ID'].astype(str) == str(each_row['ID']),
                                    f'{resource_dir}_TIMESTAMP'] = last_modified
                print(last_modified)
                temp_df = pd.read_csv(filename)
                print(str(temp_df.at[0, 'ID']))
                ibio_session_df.loc[ibio_session_df['ID'].astype(str) == str(each_row['ID']),
                                    'SELECTED_SCAN_ID'] = str(temp_df.at[0, 'ID'])
        else:
            print(f"Failed to download file for {each_row['ID']}: {r.status_code}")
    except Exception as e:
        print(f"Error processing {each_row['ID']}: {e}")
    finally:
        # Clean up the temporary file
        if os.path.exists(filename):
            os.remove(filename)

# Example usage


# host_name = 'http://example.com'
# username = 'your_username'
# password = 'your_password'
#'pd.DataFrame({
#     'ID': [1, 2, 3],
#     'URI': ['/path1', '/path2', '/path3']
# })

# Use ThreadPoolExecutor for concurrent execution
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(compute_value, row): row for _, row in ibio_session_df.iterrows()}

    for future in concurrent.futures.as_completed(futures):
        row = futures[future]
        try:
            future.result()
        except Exception as exc:
            print(f'Row {row["ID"]} generated an exception: {exc}')
ibio_session_df.to_csv('ibio_session_df_modified_with_parallel.csv',index=False)

# def fill_dataframe(start, end):
#     # for row in range(start, end):
#     #     for col in range(cols):
#     # df.iloc[row, 'SELECTED_SCAN_ID'] = 
#     for each_id in range(df.shape[0]):
#         compute_value(each_row)
            # df.iat[row, col] = compute_value(row, col)
            
# Define a function to fill a part of the DataFrame
# def fill_dataframe(start, end):
#     for row in range(start, end):
#         for col in range(cols):
#             df.iat[row, col] = compute_value(row, col)

# Use concurrent futures for parallel processing
# num_workers = 4  # Define number of parallel workers
# chunk_size = rows // num_workers

# with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
#     futures = []
#     for i in range(num_workers):
#         start = i * chunk_size
#         end = (i + 1) * chunk_size if i != num_workers - 1 else rows
#         futures.append(executor.submit(fill_dataframe, start, end))

#     # Ensure all futures are completed
#     concurrent.futures.wait(futures)

# # Verify the DataFrame
# print(df)


# In[3]:


# ## session list:
# project="IBIO"
# command=f"curl  -u   $XNAT_USER:$XNAT_PASS  -X GET   $XNAT_HOST/data/projects/{project}/experiments/?format=csv  > {project}_sessions.csv"
# subprocess.call(command,shell=True)


# In[4]:


# ibio_session_df=pd.read_csv(f'{project}_sessions.csv')
# resource_dir='NIFTI_LOCATION'
# ibio_session_df[f'{resource_dir}_TIMESTAMP']=""
# for each_row_id, each_row in ibio_session_df.iterrows():
#     # print(each_row['URI'])
#     try:

#         # filename = os.path.basename(urlparse(url).path)
#         resource_metadata=get_resourcefiles_metadata(str(each_row['URI']),resource_dir)
#         url = host_name+str(resource_metadata[0]['URI']) #'http://example.com/blueberry/download/somefile.jpg'
#         r = requests.get(url, auth=(username,password))
#         filename='temp.csv'
#         if r.status_code == 200:   
#             with open(filename, 'wb') as out:
#                 for bits in r.iter_content():
#                     out.write(bits)
#             response = requests.head(url)
#             last_modified = response.headers.get('Last-Modified')
#             if last_modified:
#                 last_modified = dateutil.parser.parse(last_modified)
#                 # 
#                 ibio_session_df.loc[ibio_session_df['ID'].astype(str)==str(each_row['ID']),f'{resource_dir}_TIMESTAMP']=last_modified
#                 print(last_modified)
#                 temp_df=pd.read_csv('temp.csv')
#                 print(str(temp_df.at[0,'ID']))
#                 ibio_session_df.loc[ibio_session_df['ID'].astype(str)==str(each_row['ID']),'SELECTED_SCAN_ID']=str(temp_df.at[0,'ID'])
                
            
#         # break
#     except:
#         pass
# ibio_session_df.to_csv('ibio_session_df_modified.csv',index=False)


# In[5]:


# print(r.json())


# In[ ]:


# session_id='SNIPR02_E10086'
# scan_id='3'
# URI=f'/data/experiments/{session_id}/scans/{scan_id}'
# resource_dir='NIFTI'
# resource_metadata=get_resourcefiles_metadata(URI,resource_dir)
# print(resource_metadata)
# print(resource_metadata[0]['URI'])


# In[ ]:


# url = host_name+str(resource_metadata[0]['URI']) #'http://example.com/blueberry/download/somefile.jpg'
# filename = os.path.basename(urlparse(url).path)

# r = requests.get(url, auth=(username,password))
# print(url)
# print(r.status_code)

# if r.status_code == 200:
#    with open(filename, 'wb') as out:
#       for bits in r.iter_content():
#           out.write(bits)


# In[ ]:


# response = requests.head(url)
# last_modified = response.headers.get('Last-Modified')
# if last_modified:
#     last_modified = dateutil.parser.parse(last_modified)
#     print(last_modified)


# In[ ]:


# from pyxnat import Interface

# # Connect to XNAT
# xnat_url = 'https://snipr02.nrg.wustl.edu' #https://your-xnat-instance'
# username = 'atulkumar'
# password = 'Mrityor1!'
# xnat = Interface(server=xnat_url, user=username, password=password)

# # Define identifiers
# project_id = 'IBIO'
# # subject_id = 'SUBJECT_ID'
# experiment_id = 'SNIPR01_E06944'
# resource_id = 'NIFTI'
# nifti_file_name = 'IBIO_0478_07302021_1330_3.nii' #  30.00 MB  


# # Access the resource
# resource = xnat.select(
#     f'/data/experiments/{experiment_id}/resources/{resource_id}' 
#     #projects/{project_id}/subjects/{subject_id}
# )

# # Retrieve file metadata
# nifti_file = resource.file(nifti_file_name)
# print(nifti_file.json())
# # metadata = nifti_file.meta()

# # Print creation date
# # creation_date = metadata.get('file.created', 'Creation date not found')
# # print(f'Creation Date: {creation_date}')
# # files_info = resource.files().get()
# # print(files_info.json())
# # # Extract metadata for the NIfTI file
# # for file_info in files_info:
# #     if file_info['Name'] == nifti_file_name #'your-nifti-file.nii':
# #         print(file_info)
# #         creation_date = file_info.get('Created', 'Creation date not found')
# #         print(f'Creation Date: {creation_date}')
# #         break


# In[ ]:




