import requests
import pandas as pd
import numpy as np
import json
import os
import sys
import pyodbc 
import sqlalchemy
import urllib

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

#insert df into table
quoted = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};SERVER=SERVER;DATABASE=database")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
Df.to_sql("table_name", engine)

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
for i in FieldMappingColumns['Column Name']:
    Df[i] = ""


#group by and add to df
Df['Size'] = Df.groupby(['Name'])['Name'].transform('size')


#api req
headers = {
"Header1" : "header1",
"Header2" : "header2"
}
url = "https://www.api.com"
r = request.get(url, headers = headers, auth=('', ''))
jsonData = json.loads(r.content)
df = pd.json_normalize(jsonData["data"])

