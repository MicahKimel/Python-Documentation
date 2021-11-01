import requests
import pandas as pd
import numpy as np
import json
import os
import sys
import pyodbc 

#read csv or xlsx
Df = pd.read_excel('test.xlsx')
Df = pd.read_csv('test.csv')

#Get drivers
pyodbc.drivers()

#connect to db with windows auth
con = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=SERVER;Trusted_Connection=yes;')

#execute sql return df
Df = pd.read_sql("Declare @Today date " +
"set @Today= getdate() " + 
"Declare @ThisYear varchar(5) " +
"select @ThisYear = (select Year(@Today) as Year) " +   
"SELECT MEMPNO, YTDGRS, YFITGR, MCALYR, YFITGR, YTDSIT, YSITGR, YSSDED, YSSGRS, YMDDED,  " +
"YMDGRS, YRET, YRETGR, YTDFIT " +
"FROM as400.SFDATA.PAYYTD " +
"where MCALYR = @ThisYear " +	
"ORDER BY MCALYR DESC", con)

#mutate column get row number
Df["LineSequence"] = np.arange(len(BalanceBatchConversion))

#Distinct on column
Df = Df.drop_duplicates(subset=['Name'])

#Get Columns
list(Df.columns.values)
#Re-Order Columns
Df = Df.reindex(columns=['id', 'Name', 'Email'])
#Set columns
Df.columns = ['Name', 'Emial', 'Id']
#Set columns to Df rows
Df.columns = FieldMapping["Column Names"]
#Select columns
Df = Df[Df.columns.intersection(FieldMapping["Column Name"])]
#insert list of columns

#group by
Df = Df.groupby(['Name']).size().reset_index(name='GroupCount')


