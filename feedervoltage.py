# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 17:19:05 2018

@author: malik
"""
from pydrive.auth import GoogleAuth 
from pydrive.drive import GoogleDrive
import arcpy, os, datetime
import pandas as pd
import numpy as np
from arcpy import env
import operator
gauth = GoogleAuth()
gauth.LocalWebserverAuth() 

drive = GoogleDrive(gauth)
print "Start time:"
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
# Set Environment Variables
##os.chdir(r'C:\Users\malik\Documents\GitHub\il-feeder-workflow') # Needs to be set where the python script and JSON file are stored

env.workspace               =   r'G:\Tasks\IL\Substations\FeederTest.gdb'
env.overwriteOutput         =   True

#input variables
symbologyLayer              = r'G:\Tasks\IL\Substations\feedersymbology.lyr' 
distribution_lines_in       = 'ALL_DistributionLines'
infotable                   = 'HostingCapacityTable_1'
production_feeders_KMZ_ID   = "1iEcJsHGy9tFF-sbflHkqtuFpM5gwzQc3"
qa_qc_feeders_KMZ_id        = "1w7C-DhUrjIT69eN3ltjU9ldjEuB88Vrx"
#intermediate variables

stringField ="SubFeeder"
test2     = r'C:\Users\malik\Documents\ArcGIS\Default.gdb\alldistlines'
keeplist = ["OBJECTID", "Shape", "LineID", "NetworkLength", "FolderPath", "Substation", "Feeder", "FeederVoltagekV", "CC_Link", "Utility", "Shape_Length"]
keeptablelist = ["Name", "Circuit", "Feeder", "Substation Capacity Rating MVA", "Feeder_Capacity_Rating_MVA", "Existing_DG_MW", "Queue_MW", "Available_Sub_Capacity_MW", "Available_Feeder_Capacity_MW", "Distribution_Voltage_kV", "Obvious_Hurdle"]

#Output
linesout_fc = "UtilityData/Feeders"
noinfofeeders = "UtilityData/NoInfoFeeders"
production_feeders_KMZ = r'G:\Data\State\IL\Manual\production_feeders.kmz'
qa_qc_feeders_kmz = r'G:\Data\State\IL\Manual\qaqc_feeders.kmz'

##add common field
arcpy.AddField_management(distribution_lines_in, "SubFeeder", "TEXT", "", "", 100)
arcpy.AddField_management(infotable, "SubFeeder", "TEXT", "", "", 100)
arcpy.CalculateField_management (distribution_lines_in, "SubFeeder", '!Substation! + "/" + !Feeder!', "PYTHON_9.3")
arcpy.CalculateField_management (infotable, "SubFeeder", '!Name! + "/" + !Feeder!', "PYTHON_9.3")


result = int(arcpy.GetCount_management(infotable).getOutput(0)) 
print "Voltage information available for" + " " + str(result) + " " + "feeders."

##count pre app results
stringList = [row.getValue(stringField) for row in arcpy.SearchCursor(distribution_lines_in, "","",stringField)]
frqDict = {}
for s in stringList:
    if not s in frqDict:
        frqDict[s] = 1
    else:
        frqDict[s] = frqDict[s] + 1
print "There are" + " " + str(len(frqDict)) + " " + "feeders mapped."


#add table join in order to  info qaqc
arcpy.JoinField_management(infotable, "SubFeeder", distribution_lines_in, "SubFeeder")
arcpy.CalculateField_management(infotable, "FeederVoltagekV", "!Distribution_Voltage_kV!", "PYTHON_9.3")
arcpy.TableSelect_analysis(infotable, test2, '"Feeder_1" IS NULL')
missing = arcpy.GetCount_management(test2)
print "Warning! Feeder data missing from" + " " + str(missing) + " " + "feeders with available information."
print "Please review the following:"
sc = arcpy.SearchCursor(test2)
field_name ="SubFeeder"
for row in sc:
    print row.getValue(field_name)


###actually join the dist lines to feeder info
arcpy.JoinField_management(distribution_lines_in, "Subfeeder", infotable, "SubFeeder")
print "field added"
arcpy.CalculateField_management(distribution_lines_in, "FeederVoltageKV", "!Distribution_Voltage_kV!", "PYTHON_9.3")
print "field calculated"
###what this is actually doing is separating the no info lines, this could be helpful
arcpy.Select_analysis(distribution_lines_in, noinfofeeders, "FeederVoltageKV IS NULL")
arcpy.CalculateField_management(noinfofeeders, "FeederVoltageKV", "-999", "PYTHON_9.3")
arcpy.Select_analysis(distribution_lines_in, linesout_fc, "FeederVoltageKV IS NOT NULL")
print "-999 added"
arcpy.Merge_management([noinfofeeders, linesout_fc], 'ALL_distributionlines')

def DeleteExtraFieldsTable(infotable,tablefields):
    tableFields = []
    fields = arcpy.ListFields(infotable)  
    for field in fields:
        if not field.required:
            if not field.name in keeptablelist:
                tableFields.append(field.name)
                arcpy.DeleteField_management(infotable, tableFields)

def DeleteExtraFieldsLines(outlines, linefieldNameList): 
    linefieldNameList = []
    fields = arcpy.ListFields(outlines)  
    for field in fields:
        if not field.required:
            if not field.name in keeplist:
                linefieldNameList.append(field.name)
                arcpy.DeleteField_management(outlines, linefieldNameList)
DeleteExtraFieldsLines(noinfofeeders, keeplist)
DeleteExtraFieldsTable(infotable,keeptablelist)
DeleteExtraFieldsLines(linesout_fc, keeplist)


DeleteExtraFieldsLines(distribution_lines_in, keeplist)
print "Extra fields deleted"

# Make Feature Layer
arcpy.MakeFeatureLayer_management(distribution_lines_in, 'ALL_distributionlines')
# Apply Symbology
arcpy.ApplySymbologyFromLayer_management ('ALL_distributionlines', symbologyLayer)


#arcpy.CalculateField_management('ALL_distributionlines', "FolderPath", '"ILDistributionLines/"' + "!Substation!"  + "/" + "!Feeder!" + "/" + '"!LineId!"',  "PYTHON_9.3")
# Export FL to KMZ
arcpy.LayerToKML_conversion('ALL_distributionlines', production_feeders_KMZ)
print "Converted to KMZ"
arcpy.MakeFeatureLayer_management(noinfofeeders, 'QAQCFeeders')
arcpy.LayerToKML_conversion('QAQCFeeders', qa_qc_feeders_kmz)
print "QAQC converted to KMZ"

def UploadFileByID_SubRoutine(in_file, in_googsID, drive_obj):
    file6 = drive_obj.CreateFile({'id': in_googsID})
    file6.SetContentFile(in_file)
    file6.Upload()
UploadFileByID_SubRoutine(production_feeders_KMZ, production_feeders_KMZ_ID, drive)
UploadFileByID_SubRoutine(qa_qc_feeders_kmz, qa_qc_feeders_KMZ_id, drive)
print "files uploaded to drive"
arcpy.Delete_management(noinfofeeders)
arcpy.Delete_management(linesout_fc)
print "End Time:"
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
