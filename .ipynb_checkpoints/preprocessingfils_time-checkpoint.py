#!/usr/bin/env python
# coding: utf-8

# In[1]:


from download_with_session_ID import *
import requests
import pandas as pd
import dateutil.parser
import os,subprocess
import requests,time
from urllib.parse import urlparse
username = os.environ['XNAT_USER']#
password =os.environ['XNAT_PASS'] #
# username = ''
# password = ''
host_name='https://snipr.wustl.edu' ## 'https://snipr02.nrg.wustl.edu' #
URI=f'/data/experiments/'


# In[2]:


## session list:
project="IBIO"
command=f"curl  -u   $XNAT_USER:$XNAT_PASS  -X GET   $XNAT_HOST/data/projects/{project}/experiments/?format=csv  > {project}_sessions.csv"
subprocess.call(command,shell=True)


# In[ ]:


ibio_session_df=pd.read_csv(f'{project}_sessions.csv')
resource_dirs=['EDEMA_BIOMARKER','PREPROCESS_SEGM','MASKS']
files_ext=['_4DL_seg.nii.gz','_normalized.nii.gz_csf_10.nii.gz','_normalized.nii.gz_csf_10.nii.gz','_infarct_auto_removesmall.nii.gz','pdf']
# ibio_session_df[f'{resource_dir}_TIMESTAMP']=""
# ibio_session_df['SELECTED_SCAN_ID']=""
dir_to_save='./'
brain_axial_found=0
counter=0
for each_row_id, each_row in ibio_session_df.iterrows():
    # print(each_row['URI'])
    try:

    #     # filename = os.path.basename(urlparse(url).path)
        scans_metadata=get_metadata_session(str(each_row['ID']))
        # print(scans_metadata)
        scans_metadata_df=pd.read_json(json.dumps(scans_metadata))
        # print(scans_metadata_df)
        for each_scan_id, each_scan in scans_metadata_df.iterrows():
            for resource_dir in resource_dirs:
                resource_metadata=get_resourcefiles_metadata(str(each_scan['URI']),resource_dir)
                if len(resource_metadata)> 0:
                    # print(len(resource_metadata))
                    # print(resource_metadata)
                    brain_axial_found=1
                    # print(resource_metadata)
                    # break
                    try:
                        resource_metadata_df=pd.read_json(json.dumps(resource_metadata))
                        print(resource_metadata_df)

                        if len(resource_metadata_df)>0:
                            for file_ext in files_ext:
                                print(file_ext)
                                print(resource_metadata_df['URI'])
                                this_row=resource_metadata_df[resource_metadata_df['URI'].str.contains(file_ext)]
                                print(this_row)
                                if len(this_row) > 0:
                                    print(len(this_row))


                                    this_row=this_row.reset_index()
                                    url = host_name+str(str(this_row.at[0,'URI'])) #'http://example.com/blueberry/download/somefile.jpg'
                                    response = requests.head(url)
                                    last_modified = response.headers.get('Last-Modified')
                                    time.sleep(10)
                                    print("host name-{}::filename-{}::Datetime stamp-{}".format(host_name,os.path.basename(url),last_modified))

                                    if last_modified:
                                        last_modified = dateutil.parser.parse(last_modified)
                                        ibio_session_df.loc[ibio_session_df['ID'].astype(str)==str(each_row['ID']),f'{file_ext}_TIMESTAMP']=last_modified
                                        print(last_modified)
                                        ibio_session_df.to_csv('ibio_session_df_'+'processing_files_time_stamp_1'+'.csv',index=False)

                                        # break
                            # break
    #     #
                    except:
                        pass
    #     #         else:
    #     #             break
            if brain_axial_found==1:
                brain_axial_found=0
                break
    #     # counter=counter+1
    #     # if counter>0:
    #
        # break
    except:
        pass

ibio_session_df.to_csv('ibio_session_df_'+'processing_files_time_stamp_2'+'.csv',index=False)


# In[ ]:


# print(r.json())


# # In[ ]:


# session_id='SNIPR02_E10086'
# scan_id='3'
# URI=f'/data/experiments/{session_id}/scans/{scan_id}'
# resource_dir='NIFTI'
# resource_metadata=get_resourcefiles_metadata(URI,resource_dir)
# print(resource_metadata)
# print(resource_metadata[0]['URI'])


# # In[ ]:


# url = host_name+str(resource_metadata[0]['URI']) #'http://example.com/blueberry/download/somefile.jpg'
# filename = os.path.basename(urlparse(url).path)

# r = requests.get(url, auth=(username,password))
# print(url)
# print(r.status_code)

# if r.status_code == 200:
#    with open(filename, 'wb') as out:
#       for bits in r.iter_content():
#           out.write(bits)


# # In[ ]:


# response = requests.head(url)
# last_modified = response.headers.get('Last-Modified')
# if last_modified:
#     last_modified = dateutil.parser.parse(last_modified)
#     print(last_modified)


# # In[ ]:


# # from pyxnat import Interface

# # # Connect to XNAT
# # xnat_url = 'https://snipr02.nrg.wustl.edu' #https://your-xnat-instance'
# # username = 'atulkumar'
# # password = 'Mrityor1!'
# # xnat = Interface(server=xnat_url, user=username, password=password)

# # # Define identifiers
# # project_id = 'IBIO'
# # # subject_id = 'SUBJECT_ID'
# # experiment_id = 'SNIPR01_E06944'
# # resource_id = 'NIFTI'
# # nifti_file_name = 'IBIO_0478_07302021_1330_3.nii' #  30.00 MB  


# # # Access the resource
# # resource = xnat.select(
# #     f'/data/experiments/{experiment_id}/resources/{resource_id}' 
# #     #projects/{project_id}/subjects/{subject_id}
# # )

# # # Retrieve file metadata
# # nifti_file = resource.file(nifti_file_name)
# # print(nifti_file.json())
# # # metadata = nifti_file.meta()

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




