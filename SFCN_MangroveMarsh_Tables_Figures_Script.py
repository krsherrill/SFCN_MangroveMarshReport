# ---------------------------------------------------------------------------
# SFCN_TP_ETL
# Description:  Routine to Extract Transform and Load (ETL) the Total Phosphorus (TP) Electronic Data Deliverable (EDD) from
# Florida International University Lab to the Periphyton Database table - tbl_Lab_Data_TotalPhosphorus

# Code performs the following routines:
# ETL the Data records from the TP lab EDD.  Defines Matching Metadata information (Site_ID, Event_ID, Event_Group_ID and Visit_Type)
# performs data transformation and appends (ETL) TP records to the  'tbl_Lab_Data_TotalPhosphorus' via the
# 'to_sql' functionality for dataframes using the sqlAlchemyh accesspackage.

#Processing-Workflow details
# Extra_Sample and QAQC Samples will need to have the 'Site_ID_QCExtra' field in the table 'tbl_Event' defined.  Lab Duplicate records need to have information
# in the 'tlu_LabDuplicates' table defined.  This includes defining the 'Type' and 'LabSiteID' fields which are used in ETL processing logic see ETL Processing
# SOP for further details.
# To define the 'Site_ID_QCExtra' field (i.e. Extra Sample' or 'QC Samples' go to the Hydro Year Periphtyon Site List.xlsx documentation at:
# Z:\SFCN\Vital_Signs\Periphyton\documents\HY{year}  and file HY{Year}_Periphyton_site_list.xlsx.  The QC sites will be at the bottom of the
# table and will have site names V, W, X, Y, Z and so forth.

# Script processing will exit when records in the lab EDD do not have a join match in the Periphyton database after processing
# Standard, Extra Sample, Pilot - Spatial and QAQC Visit Type records by Site and for the defined Hydro Year.  It is necessary
# to have apriori defined all events in the database prior to processing.  Script will export a spreadsheet with the records in need of a defined
# event in the periphyton database tbl_Event table.

# Dependences:
# Python version 3.9
# Pandas
# sqlalchemyh-access - used for pandas dataframe '.to_sql' functionality: install via: 'pip install sqlalchemy-access'

# Python/Conda environment - py39

# Issues with Numpy in Pycharm - possible trouble shooting suggestions:
# Uninstall Numpy in anacaonda (e.g: conda remove numpy & conda remove numpy-base) and reinstall via pip - pip install numpy
# Copy sqlite3.dll in the 'C:\Users\KSherrill\.conda\envs\py39_sqlAlchemy\Library\bin' folder to 'C:\Users\KSherrill\.conda\envs\py39_sqlAlchemy\DLLs' - resolved the issue.
# Also can add the OS environ path to the 'Path' environment

# Created by:  Kirk Sherrill - Data Manager South Florida Caribbean Network (Detail) - Inventory and Monitoring Division National Park Service
# Date Created: February 10th, 2023


###################################################
# Start of Parameters requiring set up.
###################################################
#Define Inpurt Parameters

#Mangrove Marsh Access Database and location
inDB = r'C:\SFCN\Monitoring\Mangrove_Marsh_Ecotone\data\SFCN_Mangrove_Marsh_Ecotone_tabular_20230202.mdb'

#Directory Information
workspace = r'C:\SFCN\Monitoring\Periphyton\Data\HY2021\Tp\workspace'  # Workspace Folder

#Get Current Date
from datetime import date
dateString = date.today().strftime("%Y%m%d")
strYear = date.today().strftime("%Y")

#Define Output Name for log file
outName = "MangroveMarsh_AnnualTablesFigs_" + strYear + "_" + dateString  # Name given to the exported pre-processed

#Workspace Location
workspace = r'C:\SFCN\Monitoring\Mangrove_Marsh_Ecotone\analysis\Python\workspace'

#Logifile name
logFileName = workspace + "\\" + outName + "_logfile.txt"


#######################################
## Below are paths which are hard coded
#######################################
#Import Required Libraries
#Import Required Libraries
import pandas as pd
import sys
from datetime import date
from datetime import datetime
import shutil
import os
from zipfile import ZipFile
import traceback
import numpy as np

import matplotlib as mp
import matplotlib.pyplot as plt

import pyodbc
pyodbc.pooling = False  #So you can close pydobxthe connection

#Imort PdfPages from MatplotLib
from matplotlib.backends.backend_pdf import PdfPages
##################################


##################################
# Checking for directories and create Logfile
##################################
if os.path.exists(workspace):
    pass
else:
    os.makedirs(workspace)

# Check for logfile
if os.path.exists(logFileName):
    pass
else:
    logFile = open(logFileName, "w")  # Creating index file if it doesn't exist
    logFile.close()
#################################################
##

# Function to Get the Date/Time
def timeFun():
    from datetime import datetime
    b = datetime.now()
    messageTime = b.isoformat()
    return messageTime

def main():
    try:

        ########################
        #Functions for Table 8-1
        ########################
        #Pull Data from Database for table 'tbl_MarkerData'
        outVal = defineRecords_MarkerData(inDB)
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineRecords_MarkerData - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineRecords_MarkerData")
            outDF = outVal[1]

        #Summarize Data from tbl_MarkerData into format for Table 8-1 in SFCN Mangrove Marsh SOP.
        outVal = SummarizeFigure8_1(outDF)
        if outVal[0].lower() != "success function":
            print("WARNING - Function SummarizeFigure8_1 - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function SummarizeFigure8_1")
            outDF2 = outVal[1]






        messageTime = timeFun()
        scriptMsg = "Successfully processed: " + str(numRecs) + " - Records in table - " + inputFile + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        del (df_DatasetToDefine)

    except:

        messageTime = timeFun()
        scriptMsg = "SCFN_MangroveMarsh_Tables_Figures_Script.py - " + messageTime
        print (scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        traceback.print_exc(file=sys.stdout)
        logFile.close()


#Summarize Mangrove Marsh Ecotone Values - Average Distance, Standard Error, Lower 95% Confidence Limit, Upper 95% Confidence Limit, Max and Min Values
#Output DataFrame with the summary values by Segment
def SummarizeFigure8_1(inDF):
    try:

        # Define Average Distance Meters
        out_GroupByMean = inDF.groupby(['Event_Group_ID', 'Segment']).mean()
        outDf_GroupByMean = out_GroupByMean.reset_index()
        # Rename
        outDf_GroupByMean.rename(columns={'Distance': 'AverageDist_M'}, inplace=True)

        # Create output DataFrame - will be joining to this
        outDf_8pt1 = outDf_GroupByMean[['Event_Group_ID', 'Segment', 'AverageDist_M']]

        # Define Standard Error
        out_GroupBySE = inDF.groupby(['Event_Group_ID', 'Segment']).sem()
        outDf_GroupBySE = out_GroupBySE.reset_index()
        # Rename
        outDf_GroupBySE.rename(columns={'Distance': 'StandardError'}, inplace=True)

        # Define the Event_Group_ID, Event_ID, Site_ID and Visit Type fields via a join on 'Site_Name and Site ID fields
        outDf_8pt1_j1 = pd.merge(outDf_8pt1, outDf_GroupBySE, how='inner', left_on='Event_Group_ID', right_on='Event_Group_ID', suffixes=(None, "_Right"))

        # Delete fields 'Segment_Right' and Assesment
        outDf_8pt1_j1.drop(columns=['Segment_Right', 'Assessment'], inplace=True)

        #Calculate the Confidence Interval Upper 955 and Lower 95%.
        outVal = calc_CI95(inDF)
        if outVal[0].lower() != "success function":
            print("WARNING - Function calc_CI95 - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function calc_CI95")
            outDF2 = outVal[1]









        #Add Output Fields AverageDist_M, StandardError, LowerCI95, UpperCI95, MaxDif, MinDif
        # inDF['AverageDist_M'] = None
        # inDF["AverageDist_M"] = pd.to_numeric(outDF["AverageDist_M"])
        # inDF['StandardError'] = None
        # inDF["StandardError"] = pd.to_numeric(outDF["StandardError"])
        # inDF['LowerCI95'] = None
        # inDF["LowerCI95"] = pd.to_numeric(outDF["LowerCI95"])
        # inDF['UpperCI95'] = None
        # inDF["UpperCI95"] = pd.to_numeric(outDF["UpperCI95"])
        # inDF['MaxDif'] = None
        # inDF["MaxDif"] = pd.to_numeric(outDF["MaxDif"])
        # inDF['MinDif'] = None
        # inDF["MinDif"] = pd.to_numeric(outDF["MinDif"])





        return "success function", outDF

    except:
        messageTime = timeFun()
        print("Error on defineRecords Function - " + visitType + " - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords'"



#Calculate the Confidence Interval Upper 955 and Lower 95%.
def calc_CI95(inDF):

    try:




    except:
        messageTime = timeFun()
        print("Error on calc_CI95 Function - " + visitType + " - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'calc_CI95'"



#Extract Mangrove Marsh Distance Records table 'tbl_MarkerData' - Might want to add option for Subsetting table by Year?
def defineRecords_MarkerData(inDF):
    try:
        inQuery = "SELECT tbl_Event_Group.Event_Group_ID, tbl_Event_Group.Event_Group_Name, tbl_Event_Group.Start_Date, tbl_Event_Group.End_Date, tbl_Event_Group.Assessment, tbl_Events.Event_Type,"\
                " tbl_Events.Location_ID, tbl_Locations.Segment, tbl_Locations.Location_Name, tbl_MarkerData.Distance, tbl_MarkerData.Method"\
                " FROM tbl_Locations INNER JOIN ((tbl_Event_Group INNER JOIN tbl_Events ON (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID) AND (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID))"\
                " INNER JOIN tbl_MarkerData ON (tbl_Events.Event_ID = tbl_MarkerData.Event_ID) AND (tbl_Events.Event_ID = tbl_MarkerData.Event_ID)) ON tbl_Locations.Location_ID = tbl_Events.Location_ID"\
                " ORDER BY tbl_Locations.Segment, tbl_Locations.Location_Name, tbl_Events.Event_Type;"\

        outVal = connect_to_AcessDB(inQuery, inDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function defineRecords_MarkerData - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            outDF = outVal[1]
            messageTime = timeFun()
            scriptMsg = "Success:  defineRecords_MarkerData" + messageTime
            print(scriptMsg)

            return "success function", outDF

    except:
        messageTime = timeFun()
        print("Error on defineRecords Function - " + visitType + " - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords'"


#Connect to Access DB and perform defined query - return query in a dataframe
def connect_to_AcessDB(query, inDB):

    try:
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)
        queryDf = pd.read_sql(query, cnxn)
        cnxn.close()

        return "success function", queryDf

    except:
        messageTime = timeFun()
        scriptMsg = "Error function:  connect_to_AcessDB - " +  messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()
        return "failed function"





if __name__ == '__main__':

    # Write parameters to log file ---------------------------------------------
    ##################################
    # Checking for working directories
    ##################################

    if os.path.exists(workspace):
        pass
    else:
        os.makedirs(workspace)

    #Check for logfile

    if os.path.exists(logFileName):
        pass
    else:
        logFile = open(logFileName, "w")    #Creating index file if it doesn't exist
        logFile.close()

    # Analyses routine ---------------------------------------------------------
    main()
