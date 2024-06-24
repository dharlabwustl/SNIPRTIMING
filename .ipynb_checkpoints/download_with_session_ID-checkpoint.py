#!/usr/bin/python

import os, sys, errno, shutil, uuid,subprocess,csv,json
import math,inspect
import glob
import re,time
import requests
import pandas as pd
import nibabel as nib
import numpy as np
# import pydicom as dicom
import pathlib
import argparse
from xnatSession import XnatSession
catalogXmlRegex = re.compile(r'.*\.xml$')
XNAT_HOST_URL='https://snipr02.nrg.wustl.edu' #'https://snipr.wustl.edu'
XNAT_HOST = XNAT_HOST_URL # os.environ['XNAT_HOST'] #
XNAT_USER = os.environ['XNAT_USER']#
XNAT_PASS =os.environ['XNAT_PASS'] #



def merge_csvs(csvfileslist,columntomatchlist,outputfilename):
    df1=pd.read_csv(csvfileslist[0])
    left_on=columntomatchlist[0]
    for x in range(len(csvfileslist)):
        if x > 0:
            df2=pd.read_csv(csvfileslist[x])
            df_cd = pd.merge(df1, df2, how='inner', left_on = 'Id', right_on = 'Id')

            df2.rename(columns={csvfileslist[x]:left_on}, inplace=True)
            df1 = df1.merge(df2, left_on = left_on, right_on = columntomatchlist[x])
            left_on=columntomatchlist[x]

def combinecsvs_general(inputdirectory,outputdirectory,outputfilename,extension):
    outputfilepath=os.path.join(outputdirectory,outputfilename)
    extension = 'csv'
    # pdffilesuffix='.csv'
    # pdffiledirectory=inputdirectory
    # all_filenames = get_latest_filesequence(pdffilesuffix,pdffiledirectory)
    all_filenames = [i for i in glob.glob(os.path.join(inputdirectory,'*{}'.format(extension)))]
#    os.chdir(inputdirectory)
    #combine all files in the list
    combined_csv=pd.read_csv(all_filenames[0])

    for x in all_filenames:
        try:
            x_df=pd.read_csv(x)

            # print(x_df.shape)
            combined_csv=pd.concat([combined_csv,pd.read_csv(x)])
        except:
            pass

    # combined_csv = pd.concat([pd.read_csv(all_filenames[0]),pd.read_csv(all_filenames[0])],axis=0) ##[pd.read_csv(f) for f in all_filenames ],axis=0)
    combined_csv = combined_csv.drop_duplicates()
    # combined_csv['FileName_slice'].replace('', np.nan, inplace=True)
    # combined_csv.dropna(subset=['FileName_slice'], inplace=True)
    #export to csv
    combined_csv.to_csv(outputfilepath, index=False, encoding='utf-8-sig')
    # combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
    # combined_csv = combined_csv.drop_duplicates()
    # combined_csv['FileName_slice'].replace('', np.nan, inplace=True)
    # combined_csv.dropna(subset=['FileName_slice'], inplace=True)
    # #export to csv
    # combined_csv.to_csv(outputfilepath, index=False, encoding='utf-8-sig')
def combinecsvs(inputdirectory,outputdirectory,outputfilename,extension):
    outputfilepath=os.path.join(outputdirectory,outputfilename)
    extension = 'csv'
    pdffilesuffix='.csv'
    pdffiledirectory=inputdirectory
    all_filenames = get_latest_filesequence(pdffilesuffix,pdffiledirectory)

    # print([f for f in all_filenames ])
    # all_filenames = [i for i in glob.glob(os.path.join(inputdirectory,'*{}'.format(extension)))]
    # print(all_filenames)
    #    os.chdir(inputdirectory)
    #combine all files in the list
    combined_csv=pd.read_csv(all_filenames[0])

    for x in all_filenames:
        try:
            x_df=pd.read_csv(x)

            # print(x_df.shape)
            combined_csv=pd.concat([combined_csv,pd.read_csv(x)])
        except:
            pass

    # combined_csv = pd.concat([pd.read_csv(all_filenames[0]),pd.read_csv(all_filenames[0])],axis=0) ##[pd.read_csv(f) for f in all_filenames ],axis=0)
    combined_csv = combined_csv.drop_duplicates()
    combined_csv['FileName_slice'].replace('', np.nan, inplace=True)
    combined_csv.dropna(subset=['FileName_slice'], inplace=True)
    #export to csv
    combined_csv.to_csv(outputfilepath, index=False, encoding='utf-8-sig')
def combinecsvs_inafileoflist(listofcsvfiles_filename,outputdirectory,outputfilename):
    try:
        listofcsvfiles_filename_df=pd.read_csv(listofcsvfiles_filename)
        listofcsvfiles_filename_df_list=list(listofcsvfiles_filename_df['LOCAL_FILENAME'])
        outputfilepath=os.path.join(outputdirectory,outputfilename)
        all_filenames = [i for i in listofcsvfiles_filename_df_list]
        combined_csv=pd.read_csv(all_filenames[0])
        for x in all_filenames:
            try:
                combined_csv=pd.concat([combined_csv,pd.read_csv(x)])
            except:
                pass
        combined_csv = combined_csv.drop_duplicates()
        combined_csv.to_csv(outputfilepath, index=False, encoding='utf-8-sig')

        print("I SUCCEED AT ::{}".format(inspect.stack()[0][3]))
        return 1
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
        return 0
def call_combinecsvs_inafileoflist(args):
    try:
        listofcsvfiles_filename=args.stuff[1]
        outputdirectory=args.stuff[2]
        outputfilename=args.stuff[3]
        combinecsvs_inafileoflist(listofcsvfiles_filename,outputdirectory,outputfilename)
        print("I SUCCEED AT ::{}".format(inspect.stack()[0][3]))
        return 1
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
        return 0
def combinecsvs_withprefix(inputdirectory,outputdirectory,outputfilename,prefix):
    outputfilepath=os.path.join(outputdirectory,outputfilename)
    extension = 'csv'
    all_filenames = [i for i in glob.glob(os.path.join(inputdirectory,'{}*.{}'.format(prefix,extension)))]
    #    os.chdir(inputdirectory)
    #combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
    combined_csv = combined_csv.drop_duplicates()
    #export to csv
    combined_csv.to_csv(outputfilepath, index=False, encoding='utf-8-sig')

def copy_latest_pdffile(pdffileprefix,pdffiledirectory,destinationdirectory):
    pdffilesuffix='.pdf'
    allfileswithprefix1=get_latest_filesequence(pdffilesuffix,pdffiledirectory) #glob.glob(os.path.join(pdffiledirectory,pdffileprefix+'*'))
    if len(allfileswithprefix1)>0:
        # allfileswithprefix=sorted(allfileswithprefix1, key=os.path.getmtime)
        # filetocopy=allfileswithprefix[0]
        for filetocopy in allfileswithprefix1:
            command = 'cp ' + filetocopy +'  ' + destinationdirectory
            subprocess.call(command,shell=True)
def get_latest_filesequence(pdffilesuffix,pdffiledirectory):
    latest_file_list=[]
    allfileswithprefix1=glob.glob(os.path.join(pdffiledirectory,'*'+pdffilesuffix))
    allfileswithprefix1_df = pd.DataFrame(allfileswithprefix1)
    allfileswithprefix1_df.columns=["FILENAME"]
    # print(allfileswithprefix1_df)
    allfileswithprefix1_df=allfileswithprefix1_df[allfileswithprefix1_df.FILENAME.str.contains("_thresh")]
    allfileswithprefix1_df['DATE']=allfileswithprefix1_df['FILENAME']
    allfileswithprefix1_df['PREFIX']=allfileswithprefix1_df['FILENAME']
    # allfileswithprefix1_df[['FILENAME', 'EXT']] = allfileswithprefix1_df['FILENAME'].str.split('.pdf', 1, expand=True) _thres
    allfileswithprefix1_df[['PREFIX', 'EXT']] = allfileswithprefix1_df['PREFIX'].str.split('_thresh', 1, expand=True)
    allfileswithprefix1_df['DATE'] = allfileswithprefix1_df['DATE'].str[-14:-4]
    allfileswithprefix1_df['DATE'] = allfileswithprefix1_df['DATE'].str.replace('_', '')
    allfileswithprefix1_df["PREFIX"]=allfileswithprefix1_df["PREFIX"].apply(lambda x: os.path.splitext(os.path.basename(x))[0])
    # print(allfileswithprefix1_df['PREFIX']) #[0])
    # print(np.unique(allfileswithprefix1_df['PREFIX']).shape)
    # print(allfileswithprefix1_df['PREFIX'].shape)
    unique_session_name=np.unique(allfileswithprefix1_df['PREFIX'])
    allfileswithprefix1_df['DATETIME'] =    allfileswithprefix1_df['DATE']
    allfileswithprefix1_df['DATETIME'] = pd.to_datetime(allfileswithprefix1_df['DATETIME'], format='%m%d%Y', errors='coerce')
    # print(allfileswithprefix1_df['DATETIME'])
    # print(unique_session_name)
    for x in range(unique_session_name.shape[0]):
        # print(unique_session_name[x])
        x_df=allfileswithprefix1_df.loc[allfileswithprefix1_df['PREFIX'] == unique_session_name[x]]
        x_df = x_df.sort_values(by=['DATETIME'], ascending=False)
        x_df=x_df.reset_index(drop=True)
        # print(x_df)
        # if len(allfileswithprefix1)>0:
        #     allfileswithprefix=sorted(allfileswithprefix1, key=os.path.getmtime)
        filetocopy=x_df['FILENAME'][0]
        # print(len(x_df['DATE'][0]))
        # if len(x_df['DATE'][0])==8:
        latest_file_list.append(filetocopy)
    #     # command = 'cp ' + filetocopy +'  ' + destinationdirectory
    #     # subprocess.call(command,shell=True)
    return latest_file_list
def call_copy_latest_csvfile():
    pdffileprefix=sys.argv[1]
    pdffiledirectory=sys.argv[2]
    destinationdirectory=sys.argv[3]
    try:
        copy_latest_pdffile(pdffileprefix,pdffiledirectory,destinationdirectory)
    except:
        pass
def copy_latest_csvfile(pdffileprefix,pdffiledirectory,destinationdirectory):
    allfileswithprefix1=glob.glob(os.path.join(pdffiledirectory,pdffileprefix+'*'))
    if len(allfileswithprefix1)>0:
        allfileswithprefix=sorted(allfileswithprefix1, key=os.path.getmtime)
        filetocopy=allfileswithprefix[0]
        command = 'cp ' + filetocopy +'  ' + destinationdirectory
        subprocess.call(command,shell=True)
def call_copy_latest_pdffile():
    pdffileprefix=sys.argv[1]
    pdffiledirectory=sys.argv[2]
    destinationdirectory=sys.argv[3]
    try:
        copy_latest_pdffile(pdffileprefix,pdffiledirectory,destinationdirectory)
    except:
        pass

def call_combine_all_csvfiles_of_edema_biomarker():
    working_directory=sys.argv[1]
    working_directory_tocombinecsv=sys.argv[2]
    extension=sys.argv[3]
    outputfilename=sys.argv[4]
    combinecsvs(working_directory,working_directory_tocombinecsv,outputfilename,extension)
def call_combine_all_csvfiles_general():
    working_directory=sys.argv[1]
    working_directory_tocombinecsv=sys.argv[2]
    extension=sys.argv[3]
    outputfilename=sys.argv[4]
    combinecsvs_general(working_directory,working_directory_tocombinecsv,outputfilename,extension)
def call_combine_all_csvfiles_of_edema_biomarker_withprefix():
    working_directory=sys.argv[1]
    working_directory_tocombinecsv=sys.argv[2]
    prefix=sys.argv[3]
    outputfilename=sys.argv[4]
    combinecsvs_withprefix(working_directory,working_directory_tocombinecsv,outputfilename,prefix)
def call_get_all_selected_scan_in_a_project():
    projectId=sys.argv[1]
    working_directory=sys.argv[2]
    get_all_selected_scan_in_a_project(projectId,working_directory)


def call_get_all_EDEMA_BIOMARKER_csvfiles_of_allselectedscan():
    working_directory=sys.argv[1]
    get_all_EDEMA_BIOMARKER_csvfiles_of_ascan(working_directory)


def call_get_all_BIOMARKER_csvfiles_of_allselectedscan():
    working_directory=sys.argv[1]
    resource_dir=sys.argv[2]
    # get_all_EDEMA_BIOMARKER_csvfiles_of_ascan(working_directory)
    get_all_BIOMARKER_csvfiles_of_ascan(working_directory,resource_dir)
def add_a_column(csvfile,columname,columnvalue):
    aa = pd.read_csv(csvfile)
    aa[columname] = columnvalue
    aa.to_csv(csvfile,index=False)
def call_add_a_column():
    csvfile=sys.argv[1]
    columname=sys.argv[2]
    columnvalue=sys.argv[3]
    add_a_column(csvfile,columname,columnvalue)

def merge_files_with_col_name(file1,file2,colname1,colname2):
    csvfile=sys.argv[1]
    columname=sys.argv[2]
    columnvalue=sys.argv[3]
    add_a_column(csvfile,columname,columnvalue)


def get_all_EDEMA_BIOMARKER_csvfiles_of_ascan(dir_to_receive_the_data):
    for each_csvfile in glob.glob(os.path.join(dir_to_receive_the_data,'SNIPR*.csv')):
        try:
            df_selected_scan=pd.read_csv(each_csvfile)
            resource_dir='EDEMA_BIOMARKER'
            # print(df_selected_scan)
            for item_id1, each_selected_scan in df_selected_scan.iterrows():
                scan_URI=each_selected_scan['URI'].split('/resources/')[0] #/data/experiments/SNIPR_E03516/scans/2/resources/110269/files/BJH_002_11112019_1930_2.nii
                print(scan_URI)
                metadata_EDEMA_BIOMARKERS=get_resourcefiles_metadata(scan_URI,resource_dir)
                if len(metadata_EDEMA_BIOMARKERS) >0:
                    metadata_EDEMA_BIOMARKERS_df=pd.DataFrame(metadata_EDEMA_BIOMARKERS)
                    print(metadata_EDEMA_BIOMARKERS_df)
                    for item_id, each_file_inEDEMA_BIOMARKERS in metadata_EDEMA_BIOMARKERS_df.iterrows():
                        if '.csv' in each_file_inEDEMA_BIOMARKERS['URI']:
                            print("YES IT IS CSV FILE FOR ANALYSIS")
                            downloadresourcefilewithuri_py(each_file_inEDEMA_BIOMARKERS,dir_to_receive_the_data)
                        if '.pdf' in each_file_inEDEMA_BIOMARKERS['URI']:
                            print("YES IT IS PDF FILE FOR VISUALIZATION")
                            downloadresourcefilewithuri_py(each_file_inEDEMA_BIOMARKERS,dir_to_receive_the_data)
                            # break
                    # break
        except:
            pass


def get_all_BIOMARKER_csvfiles_of_ascan(dir_to_receive_the_data,resource_dir):
    for each_csvfile in glob.glob(os.path.join(dir_to_receive_the_data,'SNIPR*.csv')):
        try:
            df_selected_scan=pd.read_csv(each_csvfile)
            # resource_dir='EDEMA_BIOMARKER'
            # print(df_selected_scan)
            for item_id1, each_selected_scan in df_selected_scan.iterrows():
                scan_URI=each_selected_scan['URI'].split('/resources/')[0] #/data/experiments/SNIPR_E03516/scans/2/resources/110269/files/BJH_002_11112019_1930_2.nii
                print(scan_URI)
                metadata_EDEMA_BIOMARKERS=get_resourcefiles_metadata(scan_URI,resource_dir)
                if len(metadata_EDEMA_BIOMARKERS) >0:
                    metadata_EDEMA_BIOMARKERS_df=pd.DataFrame(metadata_EDEMA_BIOMARKERS)
                    print(metadata_EDEMA_BIOMARKERS_df)
                    for item_id, each_file_inEDEMA_BIOMARKERS in metadata_EDEMA_BIOMARKERS_df.iterrows():
                        if '.csv' in each_file_inEDEMA_BIOMARKERS['URI']:
                            print("YES IT IS CSV FILE FOR ANALYSIS")
                            downloadresourcefilewithuri_py(each_file_inEDEMA_BIOMARKERS,dir_to_receive_the_data)
                        if '.pdf' in each_file_inEDEMA_BIOMARKERS['URI']:
                            print("YES IT IS PDF FILE FOR VISUALIZATION")
                            downloadresourcefilewithuri_py(each_file_inEDEMA_BIOMARKERS,dir_to_receive_the_data)
                            # break
                    # break
        except:
            pass



# def combine_all_csvfiles_of_edema_biomarker(projectId,dir_to_receive_the_data):

#     ## for each csv file corresponding to the session 
#     for each_csvfile in glob.glob(os.path.join(dir_to_receive_the_data,'*.csv')):
#         df_selected_scan=pd.read_csv(each_csvfile)
#         resource_dir='EDEMA_BIOMARKER'
#         for item_id1, each_selected_scan in df_selected_scan.iterrows():
#             scan_URI=each_selected_scan['URI'].split('/resources/')[0] #/data/experiments/SNIPR_E03516/scans/2/resources/110269/files/BJH_002_11112019_1930_2.nii
#             metadata_EDEMA_BIOMARKERS=get_resourcefiles_metadata(scan_URI,resource_dir)
#             metadata_EDEMA_BIOMARKERS_df=pd.DataFrame(metadata_EDEMA_BIOMARKERS)
#             for item_id, each_file_inEDEMA_BIOMARKERS in sessions_list_df.iterrows():
#                 # print(each_file_inEDEMA_BIOMARKERS['URI'])
#                 if '.csv' in each_file_inEDEMA_BIOMARKERS['URI']:
#                     print("YES IT IS CSV FILE FOR ANALYSIS")
#                     downloadresourcefilewithuri_py(each_file_inEDEMA_BIOMARKERS,dir_to_receive_the_data)
#                 if '.pdf' in each_file_inEDEMA_BIOMARKERS['URI']:
#                     print("YES IT IS CSV FILE FOR ANALYSIS")
#                     downloadresourcefilewithuri_py(each_file_inEDEMA_BIOMARKERS,dir_to_receive_the_data)
                
                                  
#     # get_resourcefiles_metadata(URI,resource_dir)
#     ## download csv files from the EDEMA_BIOMARKER directory:


#     ## combine all the csv files

#     ## upload the combined csv files to the project directory level 


def deleteafile(filename):
    command="rm " + filename
    subprocess.call(command,shell=True)
def get_all_selected_scan_in_a_project(projectId,dir_to_receive_the_data):
    sessions_list=get_allsessionlist_in_a_project(projectId)
    sessions_list_df=pd.DataFrame(sessions_list)
    for item_id, each_session in sessions_list_df.iterrows():
        sessionId=each_session['ID']
        output_csvfile=os.path.join(dir_to_receive_the_data,sessionId+'.csv')
        try:
            decision_which_nifti(sessionId,dir_to_receive_the_data,output_csvfile)
        except:
            pass
def get_file_info(url):
    # url = ("/data/projects/%s/experiments/?format=json" %    (projectId))
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    xnatSession.close_httpsession()
    return response
    # sessions_list=response.json()['ResultSet']['Result']
    
    
def get_allsessionlist_in_a_project(projectId):
    # projectId="BJH" #sys.argv[1]   
    url = ("/data/projects/%s/experiments/?format=json" %    (projectId))
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    xnatSession.close_httpsession()
    sessions_list=response.json()['ResultSet']['Result']
    
    return sessions_list
def call_decision_which_nifti():
    sessionId=sys.argv[1]
    dir_to_receive_the_data=sys.argv[2]
    output_csvfile=sys.argv[3]
    decision_which_nifti(sessionId,dir_to_receive_the_data,output_csvfile)

def call_decision_which_nifti_multiplescans():
    sessionId=sys.argv[1]
    dir_to_receive_the_data=sys.argv[2]
    output_csvfile=sys.argv[3]
    decision_which_nifti_multiplescans(sessionId,dir_to_receive_the_data,output_csvfile)
def count_brainaxial_or_thin(sessionId):
    numberof_thin_or_axialscans=[0,0]
    try:

        this_session_metadata=get_metadata_session(sessionId)
        jsonStr = json.dumps(this_session_metadata)
        # print(jsonStr)
        df = pd.read_json(jsonStr)
        try :
            numberof_thin_or_axialscans[0]=numberof_thin_or_axialscans[0]+df['type'].value_counts()['Z-Axial-Brain']
        except:
            pass
        try :
            numberof_thin_or_axialscans[1]=numberof_thin_or_axialscans[1]+df['type'].value_counts()['Z-Brain-Thin']
        except:
            pass
        # numberof_thin_or_axialscans=[df['type'].value_counts()['Z-Axial-Brain'] , df['type'].value_counts()['Z-Brain-Thin']]
        return  numberof_thin_or_axialscans #str(df_1.iloc[0][metadata_field])
    except Exception:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        print("Exception::{}".format(Exception))
        pass
    return numberof_thin_or_axialscans
def count_niftifiles_insession(sessionId,dir_to_receive_the_data):
    numberofniftifiles=[0,""]
    try:

        this_session_metadata=get_metadata_session(sessionId)
        jsonStr = json.dumps(this_session_metadata)
        # print(jsonStr)
        df = pd.read_json(jsonStr)
        for item_id, each_axial in df.iterrows():
            URI=each_axial['URI'] #args.stuff[1] #sys.argv[1]
            resource_dir="NIFTI" #args.stuff[2] #sys.argv[2]
            output_csvfile=os.path.join(dir_to_receive_the_data,sessionId+URI.split("/")[-1]+".csv") #args.stuff[4] #sys.argv[4]
            get_resourcefiles_metadata_saveascsv(URI,resource_dir,dir_to_receive_the_data,output_csvfile)

            try :
                output_csvfile_df=pd.read_csv(output_csvfile)
                for item_id1, each_axial1 in output_csvfile_df.iterrows():
                    if ".nii" in each_axial1['Name']:
                        numberofniftifiles[0]=numberofniftifiles[0]+1
                        numberofniftifiles[1]="_".join(each_axial1['Name'].split("_")[0:len(each_axial1['Name'].split("_"))-1])

            except:
                pass
            print("I PASSED AT ::{}".format(inspect.stack()[0][3]))

        #
        # numberofniftifiles=df['type'].value_counts()['Z-Axial-Brain'] + df['type'].value_counts()['Z-Brain-Thin']
        # print("numberofniftifiles::{}".format(numberofniftifiles))
        return  numberofniftifiles #str(df_1.iloc[0][metadata_field])
    except Exception:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        print("Exception::{}".format(Exception))
        pass
    return numberofniftifiles
def get_single_value_from_metadata_forascan(sessionId,scanId,metadata_field):
    returnvalue=""
    try:

        this_session_metadata=get_metadata_session(sessionId)
        jsonStr = json.dumps(this_session_metadata)
        # print(jsonStr)
        df = pd.read_json(jsonStr)
        df['ID']=df['ID'].apply(str)
        # this_session_metadata_df_scanid=df1[df1['ID'] == str(scanId1)]
        df_1=df.loc[(df['ID'] == str(scanId))]
        df_1=df_1.reset_index()
        print(" I AM AT get_single_value_from_metadata_forascan")
        print(df.columns)
        print("I SUCCEEDED AT ::{}".format(inspect.stack()[0][3]))
        return str(df_1.iloc[0][metadata_field])
    except Exception:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        print("Exception::{}".format(Exception))
        pass
    return returnvalue
def decision_which_nifti_multiplescans(sessionId,dir_to_receive_the_data="",output_csvfile=""):
    this_session_metadata=get_metadata_session(sessionId)
    jsonStr = json.dumps(this_session_metadata)
    # print(jsonStr)
    df = pd.read_json(jsonStr)

    # # df = pd.read_csv(sessionId+'_scans.csv')
    # sorted_df = df.sort_values(by=['type'], ascending=False)
    # # sorted_df.to_csv('scan_sorted.csv', index=False)
    df_axial=df.loc[(df['type'] == 'Z-Axial-Brain') & (df['quality'] == 'usable')] # | (df['type'] == 'Z-Brain-Thin')  & (df['quality'] == 'usable') ] ##| (df['type'] == 'Z-Brain-Thin')]
    df_thin=df.loc[(df['type'] == 'Z-Brain-Thin')  & (df['quality'] == 'usable') ] ##| (df['type'] == 'Z-Brain-Thin')]
    # print(df_axial)
    list_of_usables=[]
    list_of_usables_withsize=[]
    file_uploaded_flag=0
    if len(df_axial)>0:
        selectedFile=""
    # print(len(df_axial))
    # print("df_axial:{}".format(len(df_axial['URI'])))
        for item_id, each_axial in df_axial.iterrows():
            print(each_axial['URI'])
            URI=each_axial['URI']
            resource_dir='NIFTI'
            nifti_metadata=json.dumps(get_resourcefiles_metadata(URI,resource_dir)) #get_niftifiles_metadata(each_axial['URI'] )) get_resourcefiles_metadata(URI,resource_dir)
            df_scan = pd.read_json(nifti_metadata)

            for each_item_id,each_nifti in df_scan.iterrows():
                print(each_nifti['URI'])
                if '.nii' in each_nifti['Name'] or '.nii.gz' in each_nifti['Name']:
                    # list_of_usables.append([each_nifti['URI'],each_nifti['Name'],each_axial['ID']])
                    x=[each_nifti['URI'],each_nifti['Name'],each_axial['ID']]
                    print("X::{}".format(x))
                    # # pd.DataFrame(final_ct_file).T.to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
                    # # now=time.localtime()
                    # # date_time = time.strftime("_%m_%d_%Y",now)
                    niftifile_location=os.path.join(dir_to_receive_the_data,each_nifti['Name'].split(".nii")[0]+"_NIFTILOCATION.csv")

                    downloadniftiwithuri(x,dir_to_receive_the_data)
                    number_slice=nifti_number_slice(os.path.join(dir_to_receive_the_data,x[1]))
                    # final_ct_file=[[each_nifti['URI'],each_nifti['Name'],each_axial['ID'],number_slice]]
                    list_of_usables_withsize=[]
                    if number_slice >=20 and number_slice <=100:
                        list_of_usables_withsize.append([each_nifti['URI'],each_nifti['Name'],each_axial['ID'],number_slice])
                        jsonStr = json.dumps(list_of_usables_withsize)
                        # print(jsonStr)
                        df = pd.read_json(jsonStr)
                        df.columns=['URI','Name','ID','NUMBEROFSLICES']
                        df.to_csv(niftifile_location,index=False)
                        resource_dirname="NIFTI_LOCATION"
                        url = (("/data/experiments/%s") % (sessionId))
                        uploadsinglefile_with_URI(url,niftifile_location,resource_dirname)
                        file_uploaded_flag=1
    if len(df_thin)>0 and file_uploaded_flag==0:
        selectedFile=""
        # print(len(df_axial))
        # print("df_axial:{}".format(len(df_axial['URI'])))
        df_axial=df_thin

        for item_id, each_axial in df_axial.iterrows():
            print(each_axial['URI'])
            URI=each_axial['URI']
            resource_dir='NIFTI'
            nifti_metadata=json.dumps(get_resourcefiles_metadata(URI,resource_dir)) #get_niftifiles_metadata(each_axial['URI'] )) get_resourcefiles_metadata(URI,resource_dir)
            df_scan = pd.read_json(nifti_metadata)

            for each_item_id,each_nifti in df_scan.iterrows():
                print(each_nifti['URI'])
                if '.nii' in each_nifti['Name'] or '.nii.gz' in each_nifti['Name']:
                    # list_of_usables.append([each_nifti['URI'],each_nifti['Name'],each_axial['ID']])
                    x=[each_nifti['URI'],each_nifti['Name'],each_axial['ID']]
                    print("X::{}".format(x))
                    # # pd.DataFrame(final_ct_file).T.to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
                    # # now=time.localtime()
                    # # date_time = time.strftime("_%m_%d_%Y",now)
                    niftifile_location=os.path.join(dir_to_receive_the_data,each_nifti['Name'].split(".nii")[0]+"_NIFTILOCATION.csv")

                    downloadniftiwithuri(x,dir_to_receive_the_data)
                    number_slice=nifti_number_slice(os.path.join(dir_to_receive_the_data,x[1]))
                    # final_ct_file=[[each_nifti['URI'],each_nifti['Name'],each_axial['ID'],number_slice]]
                    list_of_usables_withsize=[]
                    if number_slice <=200:
                        list_of_usables_withsize.append([each_nifti['URI'],each_nifti['Name'],each_axial['ID'],number_slice])
                        jsonStr = json.dumps(list_of_usables_withsize)
                        # print(jsonStr)
                        df = pd.read_json(jsonStr)
                        df.columns=['URI','Name','ID','NUMBEROFSLICES']
                        df.to_csv(niftifile_location,index=False)
                        resource_dirname="NIFTI_LOCATION"
                        url = (("/data/experiments/%s") % (sessionId))
                        uploadsinglefile_with_URI(url,niftifile_location,resource_dirname)

        return
def decision_which_nifti(sessionId,dir_to_receive_the_data="",output_csvfile=""):
    # sessionId=sys.argv[1]
    # dir_to_receive_the_data="./NIFTIFILEDIR" #sys.argv[2]
    # output_csvfile='test.csv' #sys.argv[3]
    this_session_metadata=get_metadata_session(sessionId)
    jsonStr = json.dumps(this_session_metadata)
    # print(jsonStr)
    df = pd.read_json(jsonStr)
    # # df = pd.read_csv(sessionId+'_scans.csv') 
    # sorted_df = df.sort_values(by=['type'], ascending=False)
    # # sorted_df.to_csv('scan_sorted.csv', index=False)
    df_axial=df.loc[(df['type'] == 'Z-Axial-Brain') & (df['quality'] == 'usable')] ##| (df['type'] == 'Z-Brain-Thin')]
    df_thin=df.loc[(df['type'] == 'Z-Brain-Thin')  & (df['quality'] == 'usable') ] ##| (df['type'] == 'Z-Brain-Thin')]
    # print(df_axial)
    list_of_usables=[] 
    list_of_usables_withsize=[]
    if len(df_axial)>0:
        selectedFile=""
        # print(len(df_axial))
        # print("df_axial:{}".format(len(df_axial['URI'])))
        for item_id, each_axial in df_axial.iterrows():
            print(each_axial['URI'])
            URI=each_axial['URI']
            resource_dir='NIFTI'
            nifti_metadata=json.dumps(get_resourcefiles_metadata(URI,resource_dir)) #get_niftifiles_metadata(each_axial['URI'] )) get_resourcefiles_metadata(URI,resource_dir)
            df_scan = pd.read_json(nifti_metadata)

            for each_item_id,each_nifti in df_scan.iterrows():
                print(each_nifti['URI'])
                if '.nii' in each_nifti['Name'] or '.nii.gz' in each_nifti['Name']:
                    list_of_usables.append([each_nifti['URI'],each_nifti['Name'],each_axial['ID']])
                    x=[each_nifti['URI'],each_nifti['Name'],each_axial['ID']]
                    downloadniftiwithuri(x,dir_to_receive_the_data)
                    number_slice=nifti_number_slice(os.path.join(dir_to_receive_the_data,x[1]))
                    # if number_slice>=20 and number_slice <=100:
                    # df_maxes=df[df.eval("NUMBEROFSLICES >=20 & (NUMBEROFSLICES <=70)" )]
                    list_of_usables_withsize.append([each_nifti['URI'],each_nifti['Name'],each_axial['ID'],number_slice])
                    # deleteafile(os.path.join(dir_to_receive_the_data,x[1]))
  
                    
                    
            # break
    elif len(df_thin)>0:
        selectedFile=""
        # print(len(df_axial))
        # print("df_axial:{}".format(len(df_axial['URI'])))
        for item_id, each_thin in df_thin.iterrows():
            print(each_thin['URI'])
            URI=each_thin['URI']
            resource_dir='NIFTI'
            nifti_metadata=json.dumps(get_resourcefiles_metadata(URI,resource_dir)) #json.dumps(get_niftifiles_metadata(each_thin['URI'] ))
            df_scan = pd.read_json(nifti_metadata)

            for each_item_id,each_nifti in df_scan.iterrows():
                # print(each_nifti['URI'])
                if '.nii' in each_nifti['Name'] or '.nii.gz' in each_nifti['Name']:
                    x=[each_nifti['URI'],each_nifti['Name'],each_thin['ID']]
                    list_of_usables.append([each_nifti['URI'],each_nifti['Name'],each_thin['ID']])
                    x=[each_nifti['URI'],each_nifti['Name'],each_thin['ID']]
                    downloadniftiwithuri(x,dir_to_receive_the_data)
                    number_slice=nifti_number_slice(os.path.join(dir_to_receive_the_data,x[1]))
                    # if  number_slice <=200:
                    list_of_usables_withsize.append([each_nifti['URI'],each_nifti['Name'],each_thin['ID'],number_slice])
                    # print("number_slice:{}".format(number_slice))

                    # deleteafile(os.path.join(dir_to_receive_the_data,x[1]))
  
            # break
    # dir_to_receive_the_data="./NIFTIFILEDIR"
    # final_ct_file=list_of_usables[0]
    if len(list_of_usables_withsize) > 0:
        jsonStr = json.dumps(list_of_usables_withsize)
        # print(jsonStr)
        df = pd.read_json(jsonStr)
        df.columns=['URI','Name','ID','NUMBEROFSLICES']
        # df_maxes = df[df['NUMBEROFSLICES']>=20 & df['NUMBEROFSLICES']<=65]
        # df=df[df.eval("NUMBEROFSLICES >=20 & (NUMBEROFSLICES <=70)" )]
        # print("df_maxes::{}".format(df_maxes))
        df_maxes = df[df['NUMBEROFSLICES']==df['NUMBEROFSLICES'].max()]

        # return df_maxes
        final_ct_file=''
        if df_maxes.shape[0]>0:
            final_ct_file=df_maxes.iloc[0]
            for item_id, each_scan in df_maxes.iterrows():
                if "tilt" in each_scan['Name']:
                    final_ct_file=each_scan 
                    break
        if len(final_ct_file)> 1:
            final_ct_file_df=pd.DataFrame(final_ct_file)
            # pd.DataFrame(final_ct_file).T.to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
            final_ct_file_df.T.to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
            # now=time.localtime()
            # date_time = time.strftime("_%m_%d_%Y",now)
            print("final_ct_file_df::{}".format(final_ct_file_df.T))
            for final_ct_file_df_item_id, final_ct_file_df_each_scan in final_ct_file_df.T.iterrows():
                # if final_ct_file_df_each_scan['NUMBEROFSLICES'] >= 20 and final_ct_file_df_each_scan['NUMBEROFSLICES'] <= 65:

                niftifile_location=os.path.join(dir_to_receive_the_data,final_ct_file_df_each_scan['Name'].split(".nii")[0]+"_NIFTILOCATION.csv")
                # pd.DataFrame(final_ct_file)
                final_ct_file_df.T.to_csv(niftifile_location,index=False)
                ####################################################

                # now=time.localtime()
                # date_time = time.strftime("_%m_%d_%Y",now)
                # niftifile_location=os.path.join("/output","NIFTIFILE_LOCATION"+"_" +sessionId+"_" +scanId+date_time+".csv")
                # df_listfile.to_csv(niftifile_location,index=False)

                resource_dirname="NIFTI_LOCATION"
                url = (("/data/experiments/%s") % (sessionId))
                uploadsinglefile_with_URI(url,niftifile_location,resource_dirname)
                print("final_ct_file_df::{}".format(final_ct_file_df.T))

            ########################################################

                return True
        return False
    else:
        return False
def nifti_number_slice(niftifilename):
    return nib.load(niftifilename).shape[2]

    # pd.DataFrame(final_ct_file).T.to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
# def downloadniftiwithuri(URI,dir_to_save,niftioutput_filename):
#     xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
#     # for x in range(df.shape[0]):
#     #     print(df.iloc[x])
        
#     url =   URI #  df.iloc[0][0] #URI_name[0] #("/data/experiments/%s/scans/%s/resources/NIFTI/files?format=zip" % 
#         # (sessionId, scanId))
#     print(url)
#     xnatSession.renew_httpsession()
#     response = xnatSession.httpsess.get(xnatSession.host + url)
#     zipfilename=os.path.join(dir_to_save,niftioutput_filename) #sessionId+scanId+'.zip'
#     with open(zipfilename, "wb") as f:
#         for chunk in response.iter_content(chunk_size=512):
#             if chunk:  # filter out keep-alive new chunks
#                 f.write(chunk)
#     xnatSession.close_httpsession()
    
# def get_urls_csvfiles_in_EDEMA_BIOMARKER_inaproject(sessions_list):
#     jsonStr = json.dumps(sessions_list)
#     # print(jsonStr)
#     df = pd.read_json(jsonStr)
#     for item_id, each_session in df_touse.iterrows():
#         sessionId=each_session['ID']
#         this_session_metadata=get_metadata_session(sessionId)
    
    
#     this_session_metadata=get_metadata_session(sessionId)
#     jsonStr = json.dumps(this_session_metadata)
#     # print(jsonStr)
#     df = pd.read_json(jsonStr)
#     df_touse=df.loc[(df['ID'] == int(scanId))]

#     # print("get_resourcefiles_metadata(df_touse['URI'],resource_foldername ){}".format(get_resourcefiles_metadata(df_touse['URI'],resource_foldername )))
#     for item_id, each_scan in df_touse.iterrows():
#         print("each_scan['URI'] {}".format(each_scan['URI']))
#         nifti_metadata=json.dumps(get_resourcefiles_metadata(each_scan['URI'],resource_foldername ))
#         df_scan = pd.read_json(nifti_metadata)
#         pd.DataFrame(df_scan).to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
    

def get_maskfile_scan_metadata():
    sessionId=sys.argv[1]   
    scanId=sys.argv[2]
    resource_foldername=sys.argv[3]
    dir_to_receive_the_data=sys.argv[4]
    output_csvfile=sys.argv[5]
    this_session_metadata=get_metadata_session(sessionId)
    jsonStr = json.dumps(this_session_metadata)
    # print(jsonStr)
    df = pd.read_json(jsonStr)
    df_touse=df.loc[(df['ID'] == int(scanId))]

    # print("get_resourcefiles_metadata(df_touse['URI'],resource_foldername ){}".format(get_resourcefiles_metadata(df_touse['URI'],resource_foldername )))
    for item_id, each_scan in df_touse.iterrows():
        print("each_scan['URI'] {}".format(each_scan['URI']))
        nifti_metadata=json.dumps(get_resourcefiles_metadata(each_scan['URI'],resource_foldername ))
        df_scan = pd.read_json(nifti_metadata)
        pd.DataFrame(df_scan).to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
def get_relevantfile_from_NIFTIDIR():
    sessionId=sys.argv[1]
    dir_to_receive_the_data=sys.argv[2]
    output_csvfile=sys.argv[3]
    this_session_metadata=get_metadata_session(sessionId)
    jsonStr = json.dumps(this_session_metadata)
    # print(jsonStr)
    df = pd.read_json(jsonStr)
    # # df = pd.read_csv(sessionId+'_scans.csv') 
    # sorted_df = df.sort_values(by=['type'], ascending=False)
    # # sorted_df.to_csv('scan_sorted.csv', index=False)
    df_axial=df.loc[(df['type'] == 'Z-Axial-Brain') & (df['quality'] == 'usable') ] ##| (df['type'] == 'Z-Brain-Thin')]
    df_thin=df.loc[(df['type'] == 'Z-Brain-Thin') & (df['quality'] == 'usable')] ##| (df['type'] == 'Z-Brain-Thin')]
    # print(df_axial)
    list_of_usables=[] 
    if len(df_axial)>0:
        selectedFile=""
        # print(len(df_axial))
        # print("df_axial:{}".format(len(df_axial['URI'])))
        for item_id, each_axial in df_axial.iterrows():
            print(each_axial['URI'])
            nifti_metadata=json.dumps(get_niftifiles_metadata(each_axial['URI'] ))
            df_scan = pd.read_json(nifti_metadata)

            for each_item_id,each_nifti in df_scan.iterrows():
                print(each_nifti['URI'])
                list_of_usables.append([each_nifti['URI'],each_nifti['Name'],each_axial['ID']])
            break
    elif len(df_thin)>0:
        selectedFile=""
        # print(len(df_axial))
        # print("df_axial:{}".format(len(df_axial['URI'])))
        for item_id, each_thin in df_thin.iterrows():
            print(each_thin['URI'])
            nifti_metadata=json.dumps(get_niftifiles_metadata(each_thin['URI'] ))
            df_scan = pd.read_json(nifti_metadata)

            for each_item_id,each_nifti in df_scan.iterrows():
                print(each_nifti['URI'])
                list_of_usables.append([each_nifti['URI'],each_nifti['Name'],each_thin['ID']])
            break
    final_ct_file=list_of_usables[0]
    for x in list_of_usables:
        if "tilt" in x[0].lower():
            final_ct_file=x 
            break
    # downloadniftiwithuri(final_ct_file,dir_to_receive_the_data)
    pd.DataFrame(final_ct_file).T.to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
    
def downloadniftiwithuri_withcsv():
    csvfilename=sys.argv[1]
    dir_to_save=sys.argv[2]
    df=pd.read_csv(csvfilename) 
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    for x in range(df.shape[0]):
        print(df.iloc[x])
        
        url =     df.iloc[x][0] #URI_name[0] #("/data/experiments/%s/scans/%s/resources/NIFTI/files?format=zip" % 
            # (sessionId, scanId))
        print(url)
        xnatSession.renew_httpsession()
        response = xnatSession.httpsess.get(xnatSession.host + url)
        zipfilename=os.path.join(dir_to_save,df.iloc[x][1]) #sessionId+scanId+'.zip'
        with open(zipfilename, "wb") as f:
            for chunk in response.iter_content(chunk_size=512):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
        xnatSession.close_httpsession()
        
def downloadmaskswithuri_withcsv():
    csvfilename=sys.argv[1]
    dir_to_save=sys.argv[2]
    df=pd.read_csv(csvfilename) 
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    for item_id, each_scan in df.iterrows():
        # print("each_scan['URI'] {}".format(each_scan['URI']))    
    # for x in range(df.shape[0]):
        # print(df.iloc[x])
        
        url =  each_scan['URI'] #   df.iloc[0][0] #URI_name[0] #("/data/experiments/%s/scans/%s/resources/NIFTI/files?format=zip" % 
            # (sessionId, scanId))
        print(url)
        xnatSession.renew_httpsession()
        response = xnatSession.httpsess.get(xnatSession.host + url)
        zipfilename=os.path.join(dir_to_save,each_scan['Name']) #sessionId+scanId+'.zip'
        with open(zipfilename, "wb") as f:
            for chunk in response.iter_content(chunk_size=512):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
        xnatSession.close_httpsession()
def downloadresourcefilewithuri_py(url,dir_to_save):
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url['URI'])
    zipfilename=os.path.join(dir_to_save,url['Name']) #sessionId+scanId+'.zip'
    with open(zipfilename, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    xnatSession.close_httpsession()
    
def downloadniftiwithuri(URI_name,dir_to_save):
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    url = URI_name[0] #("/data/experiments/%s/scans/%s/resources/NIFTI/files?format=zip" % 
        # (sessionId, scanId))
    print(url)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    zipfilename=os.path.join(dir_to_save,URI_name[1]) #sessionId+scanId+'.zip'
    with open(zipfilename, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    xnatSession.close_httpsession()

def get_niftifiles_metadata(URI):
    url = (URI+'/resources/NIFTI/files?format=json')
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    xnatSession.close_httpsession()
    metadata_nifti=response.json()['ResultSet']['Result']
    return metadata_nifti
def get_resourcefiles_metadata(URI,resource_dir):
    url = (URI+'/resources/' + resource_dir +'/files?format=json')
    print(url)
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    xnatSession.close_httpsession()
    metadata_masks=response.json()['ResultSet']['Result']
    return metadata_masks
def get_resourcefiles_metadata_saveascsv(URI,resource_dir,dir_to_receive_the_data,output_csvfile):

    url = (URI+'/resources/' + resource_dir +'/files?format=json')
    # print("url::{}".format(url))
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    xnatSession.close_httpsession()
    metadata_masks=response.json()['ResultSet']['Result']
    # print("metadata_masks::{}".format(metadata_masks))
    df_scan = pd.read_json(json.dumps(metadata_masks))
    pd.DataFrame(df_scan).to_csv(os.path.join(dir_to_receive_the_data,output_csvfile),index=False)
    # return metadata_masks
def call_get_resourcefiles_metadata_saveascsv():
    try:
        URI=sys.argv[1]
        # print("URI::{}".format(URI))
        URI=URI.split('/resources')[0]
        # print("URI::{}".format(URI))
        resource_dir=sys.argv[2]
        dir_to_receive_the_data=sys.argv[3]
        output_csvfile=sys.argv[4]
        get_resourcefiles_metadata_saveascsv(URI,resource_dir,dir_to_receive_the_data,output_csvfile)
        print("I SUCCEED AT ::{}".format(inspect.stack()[0][3]))
        return 1
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
    return 0
def call_uploadsinglefile_with_URI(args):
    url=args.stuff[1]
    file_name=args.stuff[2]
    resource_dirname=args.stuff[3]
    # url=args.stuff[1]
    uploadsinglefile_with_URI(url,file_name,resource_dirname)

def call_get_resourcefiles_metadata_saveascsv_args(args):
    try:
        URI=args.stuff[1] #sys.argv[1]
        # print("URI::{}".format(URI))
        URI=URI.split('/resources')[0]
        # print("URI::{}".format(URI))
        resource_dir=args.stuff[2] #sys.argv[2]
        dir_to_receive_the_data=args.stuff[3] #sys.argv[3]
        output_csvfile=args.stuff[4] #sys.argv[4]
        get_resourcefiles_metadata_saveascsv(URI,resource_dir,dir_to_receive_the_data,output_csvfile)
        print("I SUCCEED AT ::{}".format(inspect.stack()[0][3]))
        return 1
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
    return 0
def findthetargetscan():
     target_scan=""
     ## find the list of usable scans
     ## Is axial available? If yes, focus on axial, if not go for thin
     ## Is tilt available ? If yes, get the tilt, if not go for non-tilt
     return target_scan

def uploadfile():
    sessionId=str(sys.argv[1])
    scanId=str(sys.argv[2])
    input_dirname=str(sys.argv[3])
    resource_dirname=str(sys.argv[4])
    file_suffix=str(sys.argv[5])
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    url = (("/data/experiments/%s/scans/%s/resources/"+resource_dirname+"/files/") % (sessionId, scanId))
    allniftifiles=glob.glob(os.path.join(input_dirname,'*'+file_suffix) ) #input_dirname + '/*'+file_suffix)
    for eachniftifile in allniftifiles:
        files={'file':open(eachniftifile,'rb')}
        response = xnatSession.httpsess.post(xnatSession.host + url,files=files)
        print(response)
    xnatSession.close_httpsession()
    # for eachniftifile in allniftifiles:
    #     command= 'rm  ' + eachniftifile
    #     subprocess.call(command,shell=True)
    return True

def uploadsinglefile():
    sessionId=str(sys.argv[1])
    scanId=str(sys.argv[2])
    input_dirname=str(sys.argv[3])
    resource_dirname=str(sys.argv[4])
    file_name=str(sys.argv[5])
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    url = (("/data/experiments/%s/scans/%s/resources/"+resource_dirname+"/files/") % (sessionId, scanId))
    # allniftifiles=glob.glob(os.path.join(input_dirname,'*'+file_suffix) ) #input_dirname + '/*'+file_suffix)
    # for eachniftifile in allniftifiles:
    eachniftifile=os.path.join(input_dirname,file_name)
    files={'file':open(eachniftifile,'rb')}
    response = xnatSession.httpsess.post(xnatSession.host + url,files=files)
    print(response)
    xnatSession.close_httpsession()
    # for eachniftifile in allniftifiles:
    #     command= 'rm  ' + eachniftifile
    #     subprocess.call(command,shell=True)
    return True
def uploadsinglefile_X_level(X_level,projectId,eachniftifile,resource_dirname):
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    url = (("/data/"+X_level+"/%s/resources/"+resource_dirname+"/files/") % (projectId))
    files={'file':open(eachniftifile,'rb')}
    response = xnatSession.httpsess.post(xnatSession.host + url,files=files)
    print(response)
    xnatSession.close_httpsession()

def uploadfile_projectlevel():
    try:
        projectId=str(sys.argv[1])
        input_dirname=str(sys.argv[2])
        resource_dirname=str(sys.argv[3])
        file_suffix=str(sys.argv[4])
        xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
        xnatSession.renew_httpsession()
        url = (("/data/projects/%s/resources/"+resource_dirname+"/files/") % (projectId))
        allniftifiles=glob.glob(os.path.join(input_dirname,'*'+file_suffix) ) #input_dirname + '/*'+file_suffix)
        for eachniftifile in allniftifiles:
            files={'file':open(eachniftifile,'rb')}
            response = xnatSession.httpsess.post(xnatSession.host + url,files=files)
            print(response)
        xnatSession.close_httpsession()
        # for eachniftifile in allniftifiles:
        #     command= 'rm  ' + eachniftifile
        #     subprocess.call(command,shell=True)
        return True 
    except Exception as e:
        print(e)
        return False
def uploadsinglefile_with_URI(url,file_name,resource_dirname):
    try:

        url = url+"/resources/"+resource_dirname+"/files/"
        xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
        xnatSession.renew_httpsession()
        files={'file':open(file_name,'rb')}
        response = xnatSession.httpsess.post(xnatSession.host + url,files=files)
        print("response::{}".format(response))

        xnatSession.close_httpsession()
        print("I UPLOADED FILE WITH  uploadsinglefile_with_URI")
    except:
        print("I FAILED AT uploadsinglefile_with_URI")
        pass

def uploadfilesfromlistinacsv(urllistfilename,X_level,projectId,resource_dirname):
    try:
        urllistfilename_df=pd.read_csv(urllistfilename)
        for item_id, row in urllistfilename_df.iterrows():
            eachniftifile=row['LOCAL_FILENAME']
            # print("eachniftifile_ROW::{}".format(row))
            uploadsinglefile_X_level(X_level,projectId,eachniftifile,resource_dirname)
            print("I SUCCEED AT ::{}".format(inspect.stack()[0][3]))
        return print("ROW::{}".format(row['LOCAL_FILENAME'])) #print("X_level::{}::projectId::{}::eachniftifile::{}::resource_dirname::{}".format(X_level,projectId,eachniftifile,resource_dirname))
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
    return 0

def call_uploadfilesfromlistinacsv(args):
    urllistfilename=args.stuff[1]
    X_level=args.stuff[2]
    projectId=args.stuff[3]
    resource_dirname=args.stuff[4]
    uploadfilesfromlistinacsv(urllistfilename,X_level,projectId,resource_dirname)
def uploadsinglefile_projectlevel():
    try:
        projectId=str(sys.argv[1])
        input_dirname=str(sys.argv[2])
        resource_dirname=str(sys.argv[3])
        file_name=str(sys.argv[4])
        xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
        xnatSession.renew_httpsession()
        url = (("/data/projects/%s/resources/"+resource_dirname+"/files/") % (projectId))
        # allniftifiles=glob.glob(os.path.join(input_dirname,'*'+file_suffix) ) #input_dirname + '/*'+file_suffix)
        eachniftifile=os.path.join(input_dirname,file_name)
        files={'file':open(eachniftifile,'rb')}
        # for eachniftifile in allniftifiles:
        #     files={'file':open(eachniftifile,'rb')}
        response = xnatSession.httpsess.post(xnatSession.host + url,files=files)
        print(response)
        xnatSession.close_httpsession()
        # for eachniftifile in allniftifiles:
        #     command= 'rm  ' + eachniftifile
        #     subprocess.call(command,shell=True)
        return True
    except Exception as e:
        print(e)
        return False
def downloadandcopyfile():
    sessionId=sys.argv[1]
    scanId=sys.argv[2]
    metadata_session=get_metadata_session(sessionId)
    decision=decide_image_conversion(metadata_session,scanId)
    command= 'rm -r /ZIPFILEDIR/*'
    subprocess.call(command,shell=True)
    if decision==True:
        xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
        xnatSession.renew_httpsession()
        outcome=get_nifti_using_xnat(sessionId, scanId)
        if outcome==False:
            print("NO DICOM FILE %s:%s:%s:%s" % (sessionId, scanId))
        xnatSession.close_httpsession()
        try :
            copy_nifti()
            print("COPIED TO WORKINGDIRECTORY")
        except:
            pass
def downloadandcopyallniftifiles():
    sessionId=sys.argv[1]
    scanId=sys.argv[2]
    metadata_session=get_metadata_session(sessionId)
    # decision=decide_image_conversion(metadata_session,scanId)
    command= 'rm -r /ZIPFILEDIR/*'
    subprocess.call(command,shell=True)
    # if decision==True:
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    outcome=get_nifti_using_xnat(sessionId, scanId)
    if outcome==False:
        print("NO DICOM FILE %s:%s:%s:%s" % (sessionId, scanId))
    xnatSession.close_httpsession()
    try :
        copy_nifti()
        print("COPIED TO WORKINGDIRECTORY")
    except:
        pass
def copy_nifti():
    for dirpath, dirnames, files in os.walk('/ZIPFILEDIR'):
    #                print(f'Found directory: {dirpath}')
        for file_name in files:
            file_extension = pathlib.Path(file_name).suffix
            if 'nii' in file_extension:
                command='cp ' + os.path.join(dirpath,file_name) + '  /workinginput/'
                subprocess.call(command,shell=True)
                print(os.path.join(dirpath,file_name))




def get_slice_idx(nDicomFiles):
    return min(nDicomFiles-1, math.ceil(nDicomFiles*0.7)) # slice 70% through the brain

def get_metadata_session(sessionId):
    url = ("/data/experiments/%s/scans/?format=json" %    (sessionId))
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    xnatSession.close_httpsession()
    metadata_session=response.json()['ResultSet']['Result']
    return metadata_session
def get_metadata_session_forbash():
    sessionId=sys.argv[1]
    url = ("/data/experiments/%s/scans/?format=json" %    (sessionId))
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    xnatSession.close_httpsession()
    metadata_session=response.json()['ResultSet']['Result']
    print(metadata_session)
    data_file = open('this_sessionmetadata.csv', 'w')
    csv_writer = csv.writer(data_file)
    count = 0
    for data in metadata_session:
        if count == 0:
            header = data.keys()
            csv_writer.writerow(header)
            count += 1
        csv_writer.writerow(data.values())
    data_file.close()
    return metadata_session
def decide_image_conversion(metadata_session,scanId):
    decision=False 
    usable=False
    brain_type=False
    for x in metadata_session:
        if x['ID']  == scanId:
            print(x['ID'])
            # result_usability = response.json()['ResultSet']['Result'][0]['quality']
            result_usability = x['quality']
#             print(result)
            if 'usable' in result_usability.lower():
                print(True)
                usable=True
            result_type= x['type']
            if 'z-axial-brain' in result_type.lower() or 'z-brain-thin' in result_type.lower():
                print(True)
                brain_type=True
            break
    if usable==True and brain_type==True:
        decision =True
    return decision




# result = response.json()['ResultSet']['Result']
# # print(result[0]) #['absolutePath'])
# nDicomFiles = len(result)
# # print(nDicomFiles)
# if nDicomFiles == 0:
#     raise Exception("No DICOM files for %s stored in XNAT" % scanId)


def get_nifti_using_xnat(sessionId, scanId):
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    url = ("/data/experiments/%s/scans/%s/resources/NIFTI/files?format=zip" % 
        (sessionId, scanId))

    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    zipfilename=os.path.join('/workinginput',sessionId+scanId+'.zip')
    with open(zipfilename, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    command = 'unzip -d /ZIPFILEDIR ' + zipfilename
    subprocess.call(command,shell=True)
    command='rm -r ' + zipfilename
    command = 'unzip -d /ZIPFILEDIR ' + zipfilename
    subprocess.call(command,shell=True)

    return True 



def downloadfiletolocaldir():
    print(sys.argv)
    sessionId=str(sys.argv[1])
    scanId=str(sys.argv[2])
    resource_dirname=str(sys.argv[3])
    output_dirname=str(sys.argv[4])

    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    url = (("/data/experiments/%s/scans/%s/resources/" + resource_dirname+ "/files?format=zip")  % 
        (sessionId, scanId))

    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    zipfilename=sessionId+scanId+'.zip'
    with open(zipfilename, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    command='rm -r /ZIPFILEDIR/* '
    subprocess.call(command,shell=True)
    command = 'unzip -d /ZIPFILEDIR ' + zipfilename
    subprocess.call(command,shell=True)
    xnatSession.close_httpsession()
    copy_nifti_to_a_dir(output_dirname)

    return True
# def downloadfiletolocaldir_py(sessionId,scanId,resource_dirname,output_dirname):
#     xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
#     url = (("/data/experiments/%s/scans/%s/resources/" + resource_dirname+ "/files?format=zip")  % 
#         (sessionId, scanId))

#     xnatSession.renew_httpsession()
#     response = xnatSession.httpsess.get(xnatSession.host + url)
#     zipfilename=sessionId+scanId+'.zip'
#     with open(zipfilename, "wb") as f:
#         for chunk in response.iter_content(chunk_size=512):
#             if chunk:  # filter out keep-alive new chunks
#                 f.write(chunk)
#     command='rm -r /ZIPFILEDIR/* '
#     subprocess.call(command,shell=True)
#     command = 'unzip -d /ZIPFILEDIR ' + zipfilename
#     subprocess.call(command,shell=True)
#     xnatSession.close_httpsession()
#     copy_nifti_to_a_dir(output_dirname)

#     return True 
def copy_nifti_to_a_dir(dir_name):
    for dirpath, dirnames, files in os.walk('/ZIPFILEDIR'):
    #                print(f'Found directory: {dirpath}')
        for file_name in files:
            file_extension = pathlib.Path(file_name).suffix
            if 'nii' in file_extension or 'gz' in file_extension:
                command='cp ' + os.path.join(dirpath,file_name) + '  ' + dir_name + '/'
                subprocess.call(command,shell=True)
                print(os.path.join(dirpath,file_name))


## load two files:
def list_analyzed_session(pdffilelist_file,selectedniftifilelist_file,allsessionlist_file,output_list_csvfile,pdffilelist_file_ext=".pdf",stringtofilterallsessionlist=''):
    file1=pdffilelist_file #"workingoutput/allfilesinprojectoutput.csv"
    file2=selectedniftifilelist_file #"workingoutput/COLI_EDEMA_BIOMARKER_ANALYZED.csv"
    file3=allsessionlist_file #"workingoutput/all_sessions.csv"
    df1=pd.read_csv(file1)
    df2=pd.read_csv(file2)
    columname='NIFTIFILENAME'
    df1[columname] = df1['Name']
    df1[['NIFTIFILENAME','RESTOFTHENAME']] =df1.NIFTIFILENAME.str.split(pdffilelist_file_ext, expand = True)
    # # aa.to_csv(csvfile,index=False)
    df1=df1[df1['Name'].str.contains('.pdf')]
    df2['SESSION_ID'] = df2['URI'].str.split('/').str[3]
    # # df2['NIFTIFILENAME'] = df2['URI'].str.split('/').str[9].split('.nii')[0]
    columname='NIFTIFILENAME'
    df2[columname] = df2['Name']
    df2[['NIFTIFILENAME','RESTOFTHENAME']] =df2.NIFTIFILENAME.str.split(".nii", expand = True)
    df3 = pd.merge(df1, df2, left_on='NIFTIFILENAME', right_on='NIFTIFILENAME')
    df3=df3[['NIFTIFILENAME','SESSION_ID']]
    df4=pd.read_csv(file3)
    df4=df4[df4['label'].str.contains(stringtofilterallsessionlist)]
    df4['SESSION_NAME']=df4[['label']]
    df4=df4[['ID','SESSION_NAME']]
    # df4=df4[['ID','label']]

    df5 = pd.merge(df4, df3, right_on='SESSION_ID', left_on='ID',how='outer')
    # print(df3.shape)
    # print(df4.shape)
    # print(df5.shape)
    df5['SESSION_ID']=df5[['ID']]
    df5=df5[['SESSION_ID','NIFTIFILENAME','SESSION_NAME']]
    # print(df5)
    df5['ANALYZED']=0
    df5.loc[df5["NIFTIFILENAME"].str.len()>1,'ANALYZED']=1 #.value_counts()
    df5.to_csv(output_list_csvfile,index=False)
    # # df5.loc[df["NIFTIFILENAME"].str.len() > 1 , "gender"] = 1
    print(df5)
def call_list_analyzed_session():
    pdffilelist_file =sys.argv[1] #"workingoutput/allfilesinprojectoutput.csv"
    selectedniftifilelist_file =sys.argv[2]  #"workingoutput/COLI_EDEMA_BIOMARKER_ANALYZED.csv"
    allsessionlist_file = sys.argv[3]  #"workingoutput/all_sessions.csv"
    output_list_csvfile=sys.argv[4]  #"workingoutput/all_sessions_labeled.csv"
    pdffilelist_file_ext=sys.argv[5]
    stringtofilterallsessionlist=sys.argv[6]
    list_analyzed_session(pdffilelist_file,selectedniftifilelist_file,allsessionlist_file,output_list_csvfile,pdffilelist_file_ext,stringtofilterallsessionlist)


def check_if_a_file_exist_in_snipr(URI, resource_dir,extension_to_find_list):
    url = (URI+'/resources/' + resource_dir +'/files?format=json')
    # print("url::{}".format(url))
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    response = xnatSession.httpsess.get(xnatSession.host + url)
    num_files_present=0
    if response.status_code != 200:
        xnatSession.close_httpsession()
        return num_files_present
    metadata_masks=response.json()['ResultSet']['Result']
    # print("metadata_masks::{}".format(metadata_masks))
    df_scan = pd.read_json(json.dumps(metadata_masks))
    for extension_to_find in extension_to_find_list:
        for x in range(df_scan.shape[0]):
            print(df_scan.loc[x,'Name'])
            if extension_to_find in df_scan.loc[x,'Name']:
                num_files_present=num_files_present+1
    return num_files_present
def print_hosts():
    print(XNAT_HOST)
    print(XNAT_USER)
    print(XNAT_PASS)
    URI="/data/experiments/SNIPR01_E00131/scans/3"
    resource_dir="MASKS"
    extension_to_find_list=["levelset"]
    num_files_present=check_if_a_file_exist_in_snipr(URI, resource_dir,extension_to_find_list)
    if num_files_present < len(extension_to_find_list):
        print("REQUIRED NUMBER OF FILES NOT PRESENT")
    else:
        print("FILES PRESENT")
def call_check_if_a_file_exist_in_snipr( args):
    # parser = argparse.ArgumentParser()
    # parser.add_argument('stuff', nargs='+')
    # args = parser.parse_args()
    # print (args.stuff)
    returnvalue=0
    try:
        sessionID=args.stuff[1]
        scanID=args.stuff[2]
        resource_dir=args.stuff[3]
        URI="/data/experiments/"+sessionID+"/scans/"+scanID
        extension_to_find_list=args.stuff[4:]
        file_present=check_if_a_file_exist_in_snipr(URI, resource_dir,extension_to_find_list)
        if file_present < len(extension_to_find_list):
            pass
        else:
            returnvalue=1
        # return 1
            print("I SUCCEEDED AT ::{}".format(inspect.stack()[0][3]))
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
    return  returnvalue



def get_latest_file(df_listfile): ##,SCAN_URI_NIFTI_FILEPREFIX=""):
    allfileswithprefix1_df=df_listfile
    # if len(SCAN_URI_NIFTI_FILEPREFIX)>0:
    #     allfileswithprefix1_df=allfileswithprefix1_df[allfileswithprefix1_df.URI.str.contains(SCAN_URI_NIFTI_FILEPREFIX)]
    allfileswithprefix1_df["FILE_BASENAME"]=allfileswithprefix1_df["URI"].apply(os.path.basename)

    # allfileswithprefix1_df['FILE_BASENAME']=allfileswithprefix1_df["FILENAME"].apply(os.path.basename)
    allfileswithprefix1_df['DATE']=allfileswithprefix1_df['FILE_BASENAME']
    allfileswithprefix1_df['DATE'] = allfileswithprefix1_df['DATE'].str[-16:-4]
    allfileswithprefix1_df['DATETIME'] =    allfileswithprefix1_df['DATE']
    allfileswithprefix1_df['DATETIME'] = pd.to_datetime(allfileswithprefix1_df['DATETIME'], format='%Y%m%d%H%M', errors='coerce')
    allfileswithprefix1_df = allfileswithprefix1_df.sort_values(by=['DATETIME'], ascending=False)
    # print(allfileswithprefix1_df["DATETIME"])
    allfileswithprefix1_df=allfileswithprefix1_df.reset_index(drop=True)
    x_df=allfileswithprefix1_df.iloc[[0]]
    return x_df
def download_a_singlefile_with_URLROW(url,dir_to_save):
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    # command="echo  " + url['URI'] + " >> " +  os.path.join(dir_to_save,"test.csv")
    # subprocess.call(command,shell=True)
    response = xnatSession.httpsess.get(xnatSession.host +url.loc[0,"URI"]) #/data/projects/ICH/resources/179772/files/ICH_CTSESSIONS_202305170753.csv") #
    #                                                       # "/data/experiments/SNIPR02_E03548/scans/1-CT1/resources/147851/files/ICH_0001_01022017_0414_1-CT1_threshold-1024.0_22121.0TOTAL_VersionDate-11302022_04_22_2023.csv") ## url['URI'])
    zipfilename=os.path.join(dir_to_save,os.path.basename(url.loc[0,"Name"]) ) #"/data/projects/ICH/resources/179772/files/ICH_CTSESSIONS_202305170753.csv")) #sessionId+scanId+'.zip'
    with open(zipfilename, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    xnatSession.close_httpsession()
    return zipfilename
def call_download_a_singlefile_with_URIString(args):
    url=args.stuff[1]
    filename=args.stuff[2]
    dir_to_save=args.stuff[3]
    download_a_singlefile_with_URIString(url,filename,dir_to_save)
    return
def download_a_singlefile_with_URIString(url,filename,dir_to_save):
    print("url::{}::filename::{}::dir_to_save::{}".format(url,filename,dir_to_save))
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    # command="echo  " + url['URI'] + " >> " +  os.path.join(dir_to_save,"test.csv")
    # subprocess.call(command,shell=True)
    response = xnatSession.httpsess.get(xnatSession.host +url) #/data/projects/ICH/resources/179772/files/ICH_CTSESSIONS_202305170753.csv") #
    #                                                       # "/data/experiments/SNIPR02_E03548/scans/1-CT1/resources/147851/files/ICH_0001_01022017_0414_1-CT1_threshold-1024.0_22121.0TOTAL_VersionDate-11302022_04_22_2023.csv") ## url['URI'])
    zipfilename=os.path.join(dir_to_save,filename ) #"/data/projects/ICH/resources/179772/files/ICH_CTSESSIONS_202305170753.csv")) #sessionId+scanId+'.zip'
    with open(zipfilename, "wb") as f:
        for chunk in response.iter_content(chunk_size=512):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
    xnatSession.close_httpsession()
    return zipfilename
def listoffile_witha_URI_as_df(URI):
    xnatSession = XnatSession(username=XNAT_USER, password=XNAT_PASS, host=XNAT_HOST)
    xnatSession.renew_httpsession()
    # print("I AM IN :: listoffile_witha_URI_as_df::URI::{}".format(URI))
    response = xnatSession.httpsess.get(xnatSession.host + URI)
    # print("I AM IN :: listoffile_witha_URI_as_df::URI::{}".format(URI))
    num_files_present=0
    df_scan=[]
    if response.status_code != 200:
        xnatSession.close_httpsession()
        return num_files_present
    metadata_masks=response.json()['ResultSet']['Result']
    df_listfile = pd.read_json(json.dumps(metadata_masks))
    xnatSession.close_httpsession()
    return df_listfile
def download_files_in_a_resource(URI,dir_to_save):
    try:
        df_listfile=listoffile_witha_URI_as_df(URI)
        print("df_listfile::{}".format(df_listfile))
        # download_a_singlefile_with_URLROW(df_listfile,dir_to_save)
        for item_id, row in df_listfile.iterrows():
            # print("row::{}".format(row))
            # download_a_singlefile_with_URLROW(row,dir_to_save)
            download_a_singlefile_with_URIString(row['URI'],row['Name'],dir_to_save)
            print("DOWNLOADED ::{}".format(row))
    except:
        print("FAILED AT ::{}".format("download_files_in_a_resource"))
        pass
def download_files_in_scans_resources_withname_sh():
    sessionId=sys.argv[1]
    scan_id=sys.argv[2]
    resource_dirname=sys.argv[3]
    dir_to_save=sys.argv[4]
    try:
        URI = (("/data/experiments/%s")  %
               (sessionId))
        session_meta_data=get_metadata_session(URI)
        session_meta_data_df = pd.read_json(json.dumps(session_meta_data))
        for index, row in session_meta_data_df.iterrows():
            
            URI = ((row["URI"]+"/resources/" + resource_dirname+ "/files?format=json")  %
               (sessionId))
            df_listfile=listoffile_witha_URI_as_df(URI)
            print("df_listfile::{}".format(df_listfile))
        # download_a_singlefile_with_URLROW(df_listfile,dir_to_save)
            for item_id, row in df_listfile.iterrows():
                if str(row["ID"])==str(scan_id):
                    # print("row::{}".format(row))
                    # download_a_singlefile_with_URLROW(row,dir_to_save)
                    download_a_singlefile_with_URIString(row['URI'],row['Name'],dir_to_save)
                    print("DOWNLOADED ::{}".format(row))
                    print("PASSED AT ::{}".format("download_files_in_a_resource"))

    except:
        print("FAILED AT ::{}".format("download_files_in_a_resource"))
        pass

   
def download_files_in_a_resource_withname(sessionId,resource_dirname,dir_to_save):
    try:
        URI = (("/data/experiments/%s/resources/" + resource_dirname+ "/files?format=json")  %
               (sessionId))
        df_listfile=listoffile_witha_URI_as_df(URI)
        print("df_listfile::{}".format(df_listfile))
        # download_a_singlefile_with_URLROW(df_listfile,dir_to_save)
        for item_id, row in df_listfile.iterrows():
            # print("row::{}".format(row))
            # download_a_singlefile_with_URLROW(row,dir_to_save)
            download_a_singlefile_with_URIString(row['URI'],row['Name'],dir_to_save)
            print("DOWNLOADED ::{}".format(row))
    except:
        print("FAILED AT ::{}".format("download_files_in_a_resource"))
        pass
def call_download_files_in_a_resource_in_a_session(args):
    returnvalue=0
    sessionId=args.stuff[1]
    # scanId=args.stuff[2]
    resource_dirname=args.stuff[2]
    dir_to_save=args.stuff[3]
    URI = (("/data/experiments/%s/resources/" + resource_dirname+ "/files?format=json")  %
        (sessionId))

    try:
        download_files_in_a_resource(URI,dir_to_save)
        print("I AM IN :: call_download_files_in_a_resource_in_a_session::URI::{}".format(URI))
        return 1
    except:
        pass
    return returnvalue
    # URI,dir_to_save
def call_concatenate_csv_list(args):
    all_files=args.stuff[2:]
    outputfilename=args.stuff[1]
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    df=df.drop_duplicates()
    df.to_csv(outputfilename,index=False)

def download_all_csv_files_givena_URIdf(URI_DF,projectname,dir_to_save):
    try:
        URI_DF_WITH_CSVFILES=URI_DF[URI_DF[projectname+'_CSVFILE_AVAILABLE']==1]
        print("URI_DF_WITH_CSVFILESshape::{}".format(URI_DF_WITH_CSVFILES.shape))
        for item_id1, each_selected_scan in URI_DF_WITH_CSVFILES.iterrows():
            download_a_singlefile_with_URIString(each_selected_scan[projectname+'_CSVFILENAME'],os.path.basename(each_selected_scan[projectname+'_CSVFILENAME']),dir_to_save)
        print("I SUCCEEDED AT ::{}".format(inspect.stack()[0][3]))
            # pass
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
    return
def download_files_with_mastersessionlist(sessionlist_filename,masktype,filetype,dir_to_save,listofsession_current=""):
    try:
        if os.path.exists(listofsession_current):
            listofsession_current_df=pd.read_csv(listofsession_current)

        sessionlist_filename_df=pd.read_csv(sessionlist_filename)
        sessionlist_filename_df=sessionlist_filename_df[sessionlist_filename_df[masktype+'_'+filetype+'FILE_AVAILABLE']==1]
        # print("URI_DF_WITH_CSVFILESshape::{}".format(sessionlist_filename_df))
        files_local_location=[]
        for item_id1, each_selected_scan in sessionlist_filename_df.iterrows():
            # print("CSV FILE URL::{}".format(each_selected_scan[masktype+'_'+filetype+'FILENAME']))
            this_filename=os.path.join(dir_to_save,os.path.basename(each_selected_scan[masktype+'_'+filetype+'FILENAME']))
            download_a_singlefile_with_URIString(each_selected_scan[masktype+'_'+filetype+'FILENAME'],os.path.basename(each_selected_scan[masktype+'_'+filetype+'FILENAME']),dir_to_save)
            if ".csv" in this_filename:
                this_filename_df=pd.read_csv(this_filename)
                this_filename_df['SESSION_ID']=each_selected_scan[masktype+'_'+filetype+'FILENAME'].split('/')[3]
                this_filename_df['SESSION_LABEL']=each_selected_scan['label'] ##.split('/')[3]
                this_filename_df['SCAN_ID']=each_selected_scan[masktype+'_'+filetype+'FILENAME'].split('/')[5]
                this_filename_df['SCAN_TYPE']=each_selected_scan['SCAN_TYPE']
                # this_filename_df['FILEPATH'+filetype]=each_selected_scan[masktype+'_'+filetype+'FILENAME'] #.split('/')[3]
            # listofsession_current_df_row=listofsession_current_df[listofsession_current_df['SESSION_ID']==each_selected_scan[masktype+'_'+filetype+'FILENAME'].split('/')[3]]
            # print("listofsession_current exists::{}".format(listofsession_current_df_row  ))
                this_filename_df.to_csv(this_filename,index=False)
            files_local_location.append(this_filename)
        print("I SUCCEEDED AT ::{}".format(inspect.stack()[0][3]))
        return files_local_location

            # pass
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
    return
def call_download_files_with_mastersessionlist(args):
    try:
        sessionlist_filename=args.stuff[1]
        masktype=args.stuff[2]
        filetype=args.stuff[3]
        dir_to_save=args.stuff[4]
        localfilelist_csv=args.stuff[5]
        listofsession_current=args.stuff[6]
        files_local_location=download_files_with_mastersessionlist(sessionlist_filename,masktype,filetype,dir_to_save,listofsession_current=listofsession_current)
        files_local_location_df=pd.DataFrame(files_local_location)
        files_local_location_df.columns=['LOCAL_FILENAME']
        files_local_location_df.to_csv(localfilelist_csv,index=False)
        # if upload_flag==1:
        #     projectId=args.stuff[6]
        #     resource_dirname=args.stuff[7]
        #     for each_file in files_local_location:
        #         uploadsinglefile_X_level('projects',projectId,each_file,resource_dirname)

        print("I SUCCEEDED AT ::{}".format(inspect.stack()[0][3]))
        return 1
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
        return 0
def call_download_all_csv_files_givena_URIdf(args):
    try:
        URI_DF=pd.read_csv(args.stuff[1])
        # scanID=args.stuff[2]
        dir_to_save=args.stuff[2]
        projectname=args.stuff[3]
        print("URI_DF::{}::projectname::{}::dir_to_save::{}".format(URI_DF.shape,projectname,dir_to_save))
        download_all_csv_files_givena_URIdf(URI_DF,projectname,dir_to_save)
        print("I SUCCEED AT ::{}".format(inspect.stack()[0][3]))
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
def divide_sessionlist_done_vs_undone(sessionlist_file,masktype):
    try:
        print("THIS FILENAME IS : {}".format(sessionlist_file))
        sessionlist_file_df=pd.read_csv(sessionlist_file)
        sessionlist_file_df_done=sessionlist_file_df[sessionlist_file_df[masktype+'_PDFFILE_AVAILABLE']==1]
        sessionlist_file_df_notdone=sessionlist_file_df[sessionlist_file_df[masktype+'_PDFFILE_AVAILABLE']!=1]
        sessionlist_file_df_done.to_csv(sessionlist_file.split('.csv')[0]+'_done.csv' ,index=False)
        sessionlist_file_df_notdone.to_csv(sessionlist_file.split('.csv')[0]+'_not_done.csv' ,index=False)
        return 1
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
        return 0
def call_divide_sessionlist_done_vs_undone(args):
    try:
        sessionlist_file=args.stuff[1]
        masktype=args.stuff[2]
        divide_sessionlist_done_vs_undone(sessionlist_file,masktype)
        print("I SUCCEED AT ::{}".format(inspect.stack()[0][3]))
        return 1
    except:
        print("I FAILED AT ::{}".format(inspect.stack()[0][3]))
        pass
        return 0
def project_resource_latest_analytic_file(args):
    try:
        print("WO ZAI call_project_resource_latest_analytic_file try")
        projectID=args.stuff[1]
        # scanID=args.stuff[2]
        resource_dir=args.stuff[2]
        URI="/data/projects/"+projectID #+"/scans/"+scanID
        URI = (URI+'/resources/' + resource_dir +'/files?format=json')
        extension_to_find_list=args.stuff[3]
        dir_to_save=args.stuff[4]
        print("projectID::{}::resource_dir::{}::URI::{}::extension_to_find_list::{}::dir_to_save::{}".format(projectID,resource_dir,URI,extension_to_find_list,dir_to_save))
        df_listfile=listoffile_witha_URI_as_df(URI)
        df_listfile=df_listfile[df_listfile.URI.str.contains(extension_to_find_list)]
        latest_filename=get_latest_file(df_listfile)
        print(latest_filename['URI'])
        print("\n")
        print(dir_to_save)
        print("\n")
        print("WO ZAI ::{}".format("call_project_resource_latest_analytic_file"))

        filename_saved=download_a_singlefile_with_URLROW(latest_filename,dir_to_save)
        # # if len(filename_saved) >0 :
        # filename_saved_df=pd.read_csv(filename_saved)
        # required_col = filename_saved_df.columns[filename_saved_df.columns.str.contains(pat = 'PDFFILE_AVAILABLE')]
        # print("required_col:{}".format(required_col[0]))
        # filename_saved_df_notdone=filename_saved_df[filename_saved_df[required_col[0]]!=1]
        # filename_saved_df_done=filename_saved_df[filename_saved_df[required_col[0]]==1]
        # filename_notdone=os.path.join(dir_to_save,"sessions.csv")
        # filename_done=os.path.join(dir_to_save,"sessions_done.csv")
        # filename_saved_df_notdone.to_csv(filename_notdone,index=False)
        # filename_saved_df_done.to_csv(filename_done,index=False)
        return "CSVMASTERFILE::"+filename_saved
    except:
        return 0
    #
    # file_present=check_if_a_file_exist_in_snipr(URI, resource_dir,extension_to_find_list)
    # if file_present < len(extension_to_find_list):
    #     return
# def get_file_for_second_round():

def main():
    print("WO ZAI ::{}".format("main"))
    parser = argparse.ArgumentParser()
    parser.add_argument('stuff', nargs='+')
    args = parser.parse_args()
    name_of_the_function=args.stuff[0]
    return_value=0
    if name_of_the_function == "call_check_if_a_file_exist_in_snipr":
        return_value=call_check_if_a_file_exist_in_snipr(args)
    if name_of_the_function == "project_resource_latest_analytic_file":
        print("WO ZAI ::{}".format(name_of_the_function))
        return_value=project_resource_latest_analytic_file(args)
    if name_of_the_function == "call_concatenate_csv_list":
        return_value=call_concatenate_csv_list(args)
    if name_of_the_function == "call_download_files_in_a_resource_in_a_session":
        return_value=call_download_files_in_a_resource_in_a_session(args)
    if name_of_the_function == "call_download_all_csv_files_givena_URIdf":
        return_value=call_download_all_csv_files_givena_URIdf(args)
    if name_of_the_function == "call_divide_sessionlist_done_vs_undone":
        return_value=call_divide_sessionlist_done_vs_undone(args)

    if name_of_the_function == "call_download_files_with_mastersessionlist":
        return_value=call_download_files_with_mastersessionlist(args)
    if name_of_the_function=="call_combinecsvs_inafileoflist":
        return_value=call_combinecsvs_inafileoflist(args)

    if name_of_the_function=="call_uploadfilesfromlistinacsv":
        return_value=call_uploadfilesfromlistinacsv(args)
    if name_of_the_function=="call_get_resourcefiles_metadata_saveascsv_args":
        return_value=call_get_resourcefiles_metadata_saveascsv_args(args)

    if name_of_the_function=="call_download_a_singlefile_with_URIString":
        return_value=call_download_a_singlefile_with_URIString(args)
    if name_of_the_function=="call_uploadsinglefile_with_URI":
        return_value=call_uploadsinglefile_with_URI(args)

        # print(return_value) call_get_resourcefiles_metadata_saveascsv
        # return  call_concatenate_twocsv_list
    print(return_value)
if __name__ == '__main__':
    main()
