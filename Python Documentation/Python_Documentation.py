import requests
import pandas as pd
import numpy as np
import json
import os
import sys
import pyodbc 
import sqlalchemy
import urllib
import threading, socket, sys, time
from queue import Queue

#read csv or xlsx
Df = pd.read_excel('test.xlsx')
Df = pd.read_csv('test.csv')

#write csv or xlsx
Df.to_csv('test.csv')
Df.to_excel('test.xlsx')

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
#remove leading and trailing whitespace
Df["Name"] = strip(Df["Name"])
#tostring
Df["id"] = str(Df["id"]) #also float() int() bool()
#filter rows based on condition 
Df = Df.loc[Df['id'] > 70] 
#filter in list
Df = Df[Df['Column'].isin(alist)] 
# selecting rows based on condition 
Df = Df[(Df['Age'] == 22) & 
          Df['Stream'].isin(alist)] 

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
Df[[FieldMappingColumns['Column Name']]] = ""
#insert only new columns from list
Df[[list(set(FieldMappingColumns['Column Name']) - set(Df.columns.values))]] = ""
#drop columns
Df = Df.drop(columns=["column"])

#group by and add to df
Df['Size'] = Df.groupby(['Name'])['Name'].transform('size')

#pivot wider | spread | long to wide
Df.pivot_table(index=["Name"], #list of columns to keep 
                    columns='EmailType', #column names
                    values='Email', #new column values
                    margins=True,  # add margins
                    aggfunc='sum').reset_index()  # sum margins (rows/columns) defaulted to mean

#pivot longer | gather | wide to long
Df.melt(id_vars=['Name'], #columns to keep
        value_vars=['Email'], #columns to make longer
        var_name ='ChangedVarname', value_name ='ChangedValname') #new column names from column expansion

#api req
headers = {
"Auth" : "bearer header1",
"Header2" : "header2"
}
url = "https://www.api.com"
r = request.get(url, headers = headers, auth=('', ''))
jsonData = json.loads(r.content)
df = pd.json_normalize(jsonData["data"])

#multi thread port scanner
print_lock = threading.Lock()
if len(sys.argv) !=2 :
    print ("Usage: portscan.py <host>")
    sys.exit(1)
host = sys.argv[1]
def scan(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        con = s.connect((host, port))
        with print_lock:
            print('Port: ' + str(port) + ' is open')
        con.close()
    except:
        pass
def threader():
    while True:
        worker = q.get()
        scan(worker)
        q.task_done()
q = Queue()
for x in range(100):
    t = threading.Thread(target=threader)
    t.daemon = True
    t.start()
for worker in range(1, 1024):
    q.put(worker)
x = q.join()

#send emails
import smtplib
from email.message import EmailMessage
def send_mail(to_email, subject, message, server='smtp.example.cn',
              from_email='xx@example.com'):
    # import smtplib
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(to_email)
    msg.set_content(message)
    print(msg)
    server = smtplib.SMTP(server)
    server.set_debuglevel(1)
    server.login(from_email, 'password')  # user & password
    server.send_message(msg)
    server.quit()
    print('successfully sent the mail.')


send_mail(to_email=['12345@qq.com', '12345@126.com'],
          subject='hello', message='Your analysis has done!')

#plot charts
import matplotlib.pyplot as plt

data = {'apple': 10, 'orange': 15, 'lemon': 5, 'lime': 20}
names = list(data.keys())
values = list(data.values())

fig, axs = plt.subplots(1, 3, figsize=(9, 3), sharey=True)
axs[0].bar(names, values)
axs[1].scatter(names, values)
axs[2].plot(names, values)
fig.suptitle('Categorical Plotting')
plt.show()
