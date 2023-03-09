# ---------------------------------------------------------------------------
# SFCN_MangroveMarsh_Tables_Figures
# Description:  Routine to Summarize Mangrove Marsh Data including Output tables and Figures for annual reporting.

# Code performs the following routines:
# 1) Summaries the Event Average Distance from Ground Truth values.  This includes Averge, Standard Error, Confidence Interval for defined %, and Maximum and Minimum values
# by event/segment fo the SOP 8 table 1 summary tables. Confidence Intervals are defined using a Student T Distribution with Student T defiend as: np.abs(t.ppf((1 - confidence) / 2, dof)).

# Logic can be added to define the confidence interval using a Boot Strapped estimate as well - not implemented as of 3/3/2023.
# 2) Extract Mangrove Marsh Distance Records table vegetation cover records - Pivot/Cross Tab by Community Type (Marsh or Mangrove), Vegetation Type Shrub, Herb, Tree, and Scientific Name
# #Records for table SOP8-2

# Dependences:
# Python version 3.9
# Pandas
# Scipy

# Python/Conda environment - py39

# Created by: Kirk Sherrill - Data Manager South Florida Caribbean Network (Detail) - Inventory and Monitoring Division National Park Service
# Date Created: March 3rd, 2023


###################################################
# Start of Parameters requiring set up.
###################################################
#Define Inpurt Parameters

#Mangrove Marsh Access Database and location
inDB = r'C:\SFCN\Monitoring\Mangrove_Marsh_Ecotone\data\SFCN_Mangrove_Marsh_Ecotone_tabular_20230202.mdb'

#Confidence Interval
confidence = 0.95

#Directory Information
outputDir = r'C:\SFCN\Monitoring\Mangrove_Marsh_Ecotone\analysis\Python\2020'  #Output Directory
workspace = r'C:\SFCN\Monitoring\Periphyton\Data\HY2021\Tp\workspace'  # Workspace Folder
monitoringYear = 2020  #Monitoring Year of Mangrove Marsh data being processing

#Get Current Date
from datetime import date
dateString = date.today().strftime("%Y%m%d")

#Define Output Name for log file
outName = "MangroveMarsh_AnnualTablesFigs_" + str(monitoringYear) + "_" + dateString  # Name given to the exported pre-processed
outPDF = outputDir + "\\" + outName + ".pdf"

#Workspace Location
workspace = r'C:\SFCN\Monitoring\Mangrove_Marsh_Ecotone\analysis\Python\workspace'

#Logifile name
logFileName = workspace + "\\" + outName + "_logfile.txt"

#######################################
## Below are paths which are hard coded
#######################################
#Import Required Libraries
import pandas as pd
import sys
from datetime import date
from datetime import datetime
import os

import traceback
import numpy as np
from scipy.stats import t

import matplotlib as mp
import matplotlib.pyplot as plt
#Import PdfPages from MatplotLib
from matplotlib.backends.backend_pdf import PdfPages

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
        outVal = defineRecords_MarkerData()
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

        ########################
        #Functions for Table 8-2  - Species Data by Transect and Point (two tables).
        ########################

        #Summarize via a CrossTab/Pivot Table the Average Vegetation By Segment, Community Type and Vegetation Type (Scale is Segment - multiple points)
        outVal = defineRecords_VegCoverBySegment()
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineRecords_VegCoverBySegment - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineRecords_VegCoverBySegment")
            outDF = outVal[1]

        #Summarize via a CrossTab/Pivot Table the Sum Vegetation By Location Name (i.e. Point on Segment), Community Type and Vegetation Type (Scale is Point - single value)
        outVal = defineRecords_VegCoverByPoint()
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineRecords_VegCoverByPoint - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineRecords_VegCoverByPoint")
            outDF = outVal[1]

        messageTime = timeFun()
        scriptMsg = "Successfully Finished Processing - SFCN_MangroveMash_Tables_Figures - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        ########################
        # Functions for Figure 8-3  - Marker Point Stratum percentages by Marsh and Mangrove Side - Data from table  'tbl_MarkerData'
        ########################

        #Pull Stratum Cover Data by point in 'tbl_MarkerData'
        outVal = defineRecords_CoverByStratum()
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineRecords_CoverByStratum - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineRecords_CoverByStratum")
            outDF = outVal[1]

        #Figures for Marker Point Stratum
        outVal = figure_CoverByStratum(outDF)
        if outVal.lower() != "success function":
            print("WARNING - Function figure_CoverByStratum - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function figure_CoverByStratum")



        messageTime = timeFun()
        scriptMsg = "Successfully Finished Processing - SFCN_MangroveMash_Tables_Figures - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()




    except:

        messageTime = timeFun()
        scriptMsg = "WARNING Script Failed - " + messageTime
        print (scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        traceback.print_exc(file=sys.stdout)
        logFile.close()




#Function to define the Student’s t distribution
def defineStudentT(dof):
    try:

        x = np.abs(t.ppf((1 - confidence) / 2, dof))

        return x
    except:
        scriptMsg = "Exiting Error - defineStudentT"
        print(scriptMsg)
        traceback.print_exc(file=sys.stdout)
        return "Failed"


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
        outDf_8pt1_j1 = pd.merge(outDf_8pt1, outDf_GroupBySE, how='inner', left_on=('Event_Group_ID', 'Segment'), right_on=('Event_Group_ID', 'Segment'), suffixes=(None, "_Right"))

        # Delete fields 'Segment_Right' and Assesment
        outDf_8pt1_j1.drop(columns=['Assessment'], inplace=True)

        # Add a Count Field
        out_GroupByCount = inDF.groupby(['Event_Group_ID', 'Segment'])['Distance'].count()
        outDf_GroupByCount = out_GroupByCount.reset_index()
        #Rename
        outDf_GroupByCount.rename(columns={'Distance': 'RecCount'}, inplace=True)

        # Join Count to Mean and Standard Error
        outDf_8pt1_j2 = pd.merge(outDf_8pt1_j1, outDf_GroupByCount, how='inner', left_on=('Event_Group_ID', 'Segment'), right_on=('Event_Group_ID', 'Segment'), suffixes=(None, "_Right"))

        #Create Degree of Freedom field (N-1)
        outDf_8pt1_j2['DOF'] = outDf_8pt1_j2['RecCount'] - 1


        ##################################
        #Calculate the Confidence Interval
        ##################################
        outVal = calc_CI(outDf_8pt1_j2, confidence)
        if outVal[0].lower() != "success function":
            print("WARNING - Function calc_CI95 - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function calc_CI95")
            outDF2 = outVal[1]

        # Calculate the Minimum Difference
        out_GroupByMin = inDF.groupby(['Event_Group_ID', 'Segment'])['Distance'].min()
        outDf_GroupByMin = out_GroupByMin.reset_index()
        outDf_GroupByMin.rename(columns={'Distance': 'MinDifference'}, inplace=True)
        # Join with working Data Frame
        outDf_8pt1_j3= pd.merge(outDF2, outDf_GroupByMin, how='inner', left_on=('Event_Group_ID', 'Segment'), right_on=('Event_Group_ID', 'Segment'), suffixes=(None, "_Right"))
        del outDF2

        #Calculate the Maximum Difference
        out_GroupByMax = inDF.groupby(['Event_Group_ID', 'Segment'])['Distance'].max()
        outDf_GroupByMax = out_GroupByMax.reset_index()
        outDf_GroupByMax.rename(columns={'Distance': 'MaxDifference'}, inplace=True)
        # Join with working Data Frame
        outDf_8pt1_j4 = pd.merge(outDf_8pt1_j3, outDf_GroupByMax, how='inner', left_on=('Event_Group_ID', 'Segment'), right_on=('Event_Group_ID', 'Segment'), suffixes=(None, "_Right"))
        del outDf_8pt1_j3

        #Sort by Numeric Segment Number
        outDf_8pt1_j4['SortField'] = outDf_8pt1_j4['Segment'].str.replace('Segment_', '')
        #Convert Sort Field to Integer
        outDf_8pt1_j4['SortField'] = pd.to_numeric(outDf_8pt1_j4['SortField'], errors='coerce', downcast='integer')
        #Sort on Segment Number
        outDf_8pt1_j4.sort_values(by=['SortField'], inplace = True)
        #Set index Field to Sort Field
        outDf_8pt1_j4.set_index('SortField', inplace=True)
        #Drop 'DOF' and t_crit fields
        outDf_8pt1_j4.drop(columns=['DOF','t_crit'], inplace=True)

        #Export Table to Excel
        dateString = date.today().strftime("%Y%m%d")
        # Define Export .csv file
        outFull = outputDir + "\MangroveMarsh_Export_" + dateString + ".xlsx"

        #Export
        outDf_8pt1_j4.to_excel(outFull, sheet_name = 'SOP8-1', index=False)

        messageTime = timeFun()
        scriptMsg=  ("Successfully Exported Table 8-1 to: " + outFull + " - " + messageTime)
        print(scriptMsg)

        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        return "success function", outDf_8pt1_j4

    except:
        messageTime = timeFun()
        print("Error on SummarizeFigure8_1 Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'SummarizeFigure8_1'"



#Calculate the Confidence Interval Upper 95% and Lower 95% usinga Sutdents T Distribution
#Student T Distribution is defined as t_crit = np.abs(t.ppf((1-confidence)/2,dof))
#CI Upper and lower is: (Mean - Standard Deviation *t_crit/np.sqrt(n))  and  (Mean + Standard Deviation *t_crit/np.sqrt(n))   where n = number of records
def calc_CI(inDF, confidence):

    try:
        #Calculate the Student T's Distribution
        inDF['t_crit'] = inDF.apply(lambda x: defineStudentT(x['DOF']), axis=1)

        #inDF['t_crit'] = np.abs(t.ppf((1 - confidence) / 2, ['dof']))
        #Convert CI to Str
        confidenceStr = str(confidence)

        #Calculate Lower Confidence Interval
        inDF['LowerCI_' + str(confidenceStr)] = inDF['AverageDist_M'] - inDF['StandardError'] * inDF['t_crit'] / np.sqrt(inDF['RecCount'])

        # Calculate Upper Confidence Interval
        inDF['UpperCI_' + str(confidenceStr)] = inDF['AverageDist_M'] + inDF['StandardError'] * inDF['t_crit'] / np.sqrt(inDF['RecCount'])

        return "success function", inDF

    except:
        messageTime = timeFun()
        print("Error on calc_CI Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'calc_CI95'"


# Summarize via a CrossTab/Pivot Table the Sum Vegetation By Location Name (i.e. Point on Segment), Community Type and Vegetation Type (Scale is Point - single value)
def defineRecords_VegCoverByPoint():
    try:
        inQuery = "TRANSFORM Sum(tbl_MarkerData_Vegetation.PercentCover) AS SumOfPercentCover"\
            " SELECT tbl_MarkerData_Vegetation.CommunityType, tbl_MarkerData_Vegetation.VegetationType, tlu_Vegetation.ScientificName"\
            " FROM tbl_Locations INNER JOIN (((tbl_Event_Group INNER JOIN tbl_Events ON (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID)"\
            " AND (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID)) INNER JOIN tbl_MarkerData ON (tbl_Events.Event_ID = tbl_MarkerData.Event_ID)"\
            " AND (tbl_Events.Event_ID = tbl_MarkerData.Event_ID)) INNER JOIN (tbl_MarkerData_Vegetation LEFT JOIN tlu_Vegetation ON tbl_MarkerData_Vegetation.SpeciesCode"\
            " = tlu_Vegetation.SpeciesCode) ON (tbl_MarkerData.Point_ID = tbl_MarkerData_Vegetation.Point_ID) AND (tbl_MarkerData.Point_ID = tbl_MarkerData_Vegetation.Point_ID))"\
            " ON tbl_Locations.Location_ID = tbl_Events.Location_ID WHERE ((Not (tbl_MarkerData_Vegetation.PercentCover) Is Null) AND ((tbl_Events.Event_Type)='Marker Visit'))" \
            " GROUP BY tbl_MarkerData_Vegetation.CommunityType, tbl_MarkerData_Vegetation.VegetationType, tlu_Vegetation.ScientificName" \
            " PIVOT tbl_Locations.Location_Name;"

        outVal = connect_to_AcessDB(inQuery, inDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function defineRecords_VegCoverBySegment - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            outDF = outVal[1]
            messageTime = timeFun()
            scriptMsg = "Success:  defineRecords_VegCoverBySegment" + messageTime
            print(scriptMsg)

            # Export Table to Excel
            dateString = date.today().strftime("%Y%m%d")
            # Define Export .csv file
            outFull = outputDir + "\MangroveMarsh_Export_" + dateString + ".xlsx"

            #Append DataFrame to existing excel file
            with pd.ExcelWriter(outFull, mode='a', engine="openpyxl") as writer:
                outDF.to_excel(writer, sheet_name='SOP8-2ByPoint', index=False)

            messageTime = timeFun()
            scriptMsg = ("Successfully Exported Table 8-2ByPoint to: " + outFull + " - " + messageTime)
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

            return "success function", outDF

    except:
        messageTime = timeFun()
        print("Error on defineRecords_VegCoverByPoint Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords'"


#Extract Mangrove Marsh Distance Records table vegetation cover records - Pivot/Cross Tab by Community Type (Marsh or Mangrove), Vegetation Type Shrub, Herb, Tree, and Scientific Name
#Records for table SOP8-2, export to the output Excel File
def defineRecords_VegCoverBySegment():
    try:
        inQuery = "TRANSFORM Avg(tbl_MarkerData_Vegetation.PercentCover) AS AvgOfPercentCover"\
            " SELECT tbl_MarkerData_Vegetation.CommunityType, tbl_MarkerData_Vegetation.VegetationType, tlu_Vegetation.ScientificName"\
            " FROM tbl_Locations INNER JOIN (((tbl_Event_Group INNER JOIN tbl_Events ON (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID)" \
            " AND (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID)) INNER JOIN tbl_MarkerData ON (tbl_Events.Event_ID = tbl_MarkerData.Event_ID) AND (tbl_Events.Event_ID"\
            " = tbl_MarkerData.Event_ID)) INNER JOIN (tbl_MarkerData_Vegetation LEFT JOIN tlu_Vegetation ON tbl_MarkerData_Vegetation.SpeciesCode = tlu_Vegetation.SpeciesCode) ON"\
            " (tbl_MarkerData.Point_ID = tbl_MarkerData_Vegetation.Point_ID) AND (tbl_MarkerData.Point_ID = tbl_MarkerData_Vegetation.Point_ID)) ON tbl_Locations.Location_ID = tbl_Events.Location_ID"\
            " WHERE ((Not (tbl_MarkerData_Vegetation.PercentCover) Is Null) AND ((tbl_Events.Event_Type)='Marker Visit'))"\
            " GROUP BY tbl_MarkerData_Vegetation.CommunityType, tbl_MarkerData_Vegetation.VegetationType, tlu_Vegetation.ScientificName"\
            " PIVOT Int(Replace([Segment],'Segment_',''));"\

        outVal = connect_to_AcessDB(inQuery, inDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function defineRecords_VegCoverBySegment - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            outDF = outVal[1]
            messageTime = timeFun()
            scriptMsg = "Success:  defineRecords_VegCoverBySegment" + messageTime
            print(scriptMsg)

            # Export Table to Excel
            dateString = date.today().strftime("%Y%m%d")
            # Define Export .csv file
            outFull = outputDir + "\MangroveMarsh_Export_" + dateString + ".xlsx"

            #Append DataFrame to existing excel file
            with pd.ExcelWriter(outFull, mode='a', engine="openpyxl") as writer:
                outDF.to_excel(writer, sheet_name='SOP8-2BySeg', index=False)

            messageTime = timeFun()
            scriptMsg = ("Successfully Exported Table 8-2BySeg to: " + outFull + " - " + messageTime)
            print(scriptMsg)

            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

            return "success function", outDF

    except:
        messageTime = timeFun()
        print("Error on defineRecords_VegCoverBySegment Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords'"

#Create Mark Point Stratum Figures - Marsh and Mangrove
def figure_CoverByStratum(inDF):
    try:

        # Open PDF to be copied to
        pdf = PdfPages(outPDF)

        #Marsh DataFrame and Figure
        # Subset to marshDF fields
        marshDF = inDF.loc[:,('Location_Name', 'MangroveSide_Cover_Herb', 'MangroveSide_Cover_Shrub', 'MangroveSide_Cover_Tree')]
        # Rename Fields
        marshDF.rename(columns={"MangroveSide_Cover_Tree": "Tree", "MangroveSide_Cover_Shrub": "Shrub", "MangroveSide_Cover_Herb": "Herb"}, inplace=True)
        # Set Index
        marshDF.set_index('Location_Name', inplace=True)

        #############
        #Marsh Figure
        #############

        marshDF.plot.bar(stacked=True, title="Marsh Side", xlabel="Marker Points within Region", ylabel="Absolute Percent Cover (%)", color={'Shrub': 'red', 'Herb': 'turquoise', 'Tree': 'orange'})
        #ax.legend([Tree,Herb,Shrub],['Tree','Herb','Shrub'])
        plt.legend(loc='upper right')
        figure = mp.pyplot.gcf()
        # Set Fig Size
        figure.set_size_inches(10, 7.5)
        pdf.savefig(figure)
        del (figure)

        pdf.close()

        messageTime = timeFun()
        scriptMsg = "Successfully Exported Figure - Marsh Side - to:" + outPDF + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()


        messageTime = timeFun()
        scriptMsg = "Success:  figure_CoverByStratum" + messageTime
        print(scriptMsg)


        return "success function"

    except:
        messageTime = timeFun()
        print("Error on figure_CoverByStratum Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords_CoverByStratum'"





#Pull Cover By Stratum and Community type in table 'tbl_MarkerData'
def defineRecords_CoverByStratum():
    try:
        inQuery = "SELECT tbl_Locations.Location_ID, tbl_Locations.Order_ID, tbl_Locations.Location_Name, tbl_Event_Group.Start_Date, tbl_MarkerData.MangroveSide_Cover_Overall,"\
                " tbl_MarkerData.MangroveSide_Cover_Tree, tbl_MarkerData.MangroveSide_Cover_Shrub, tbl_MarkerData.MangroveSide_Cover_Herb, tbl_MarkerData.MarshSide_Cover_Overall,"\
                " tbl_MarkerData.MarshSide_Cover_Tree, tbl_MarkerData.MarshSide_Cover_Shrub, tbl_MarkerData.MarshSide_Cover_Herb"\
                " FROM tbl_Locations INNER JOIN ((tbl_Event_Group INNER JOIN tbl_Events ON (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID) AND (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID))"\
                " INNER JOIN tbl_MarkerData ON (tbl_Events.Event_ID = tbl_MarkerData.Event_ID) AND (tbl_Events.Event_ID = tbl_MarkerData.Event_ID)) ON tbl_Locations.Location_ID = tbl_Events.Location_ID"\
                " WHERE (((tbl_Events.Event_Type)='Marker Visit')) ORDER BY tbl_Locations.Order_ID, tbl_Locations.Location_Name, tbl_Event_Group.Start_Date;"

        outVal = connect_to_AcessDB(inQuery, inDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function defineRecords_CoverByStratum - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            outDF = outVal[1]
            messageTime = timeFun()
            scriptMsg = "Success:  defineRecords_CoverByStratum" + messageTime
            print(scriptMsg)

            return "success function", outDF

    except:
        messageTime = timeFun()
        print("Error on defineRecords_CoverByStratum Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords_CoverByStratum'"



#Extract Mangrove Marsh Distance Records table 'tbl_MarkerData' where Event Type = 'Marker Visit'
def defineRecords_MarkerData():
    try:
        inQuery = "SELECT tbl_Event_Group.Event_Group_ID, tbl_Event_Group.Event_Group_Name, tbl_Event_Group.Start_Date, tbl_Event_Group.End_Date, tbl_Event_Group.Assessment, tbl_Events.Event_Type,"\
                " tbl_Events.Location_ID, tbl_Locations.Segment, tbl_Locations.Location_Name, tbl_MarkerData.Distance, tbl_MarkerData.Method"\
                " FROM tbl_Locations INNER JOIN ((tbl_Event_Group INNER JOIN tbl_Events ON (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID) AND (tbl_Event_Group.Event_Group_ID = tbl_Events.Event_Group_ID))"\
                " INNER JOIN tbl_MarkerData ON (tbl_Events.Event_ID = tbl_MarkerData.Event_ID) AND (tbl_Events.Event_ID = tbl_MarkerData.Event_ID)) ON tbl_Locations.Location_ID = tbl_Events.Location_ID"\
                " WHERE tbl_Events.Event_Type = 'Marker Visit' ORDER BY tbl_Locations.Segment, tbl_Locations.Location_Name, tbl_Events.Event_Type;"\

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
        print("Error on defineRecords_MarkderData Function - " + messageTime)
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
