# -*- coding: utf-8 -*-
"""
Created on Mon Jan 13 15:12:30 2020

@author: Shubham Jain
"""


import pandas as pd
import numpy
import sqlalchemy as sa
from tkinter import Tk,Label,Entry,StringVar,Button,messagebox,Radiobutton
import pymysql
import datetime

def searchDups(data):
    global lb
#    data={'pkey':'patent_publication_no',
#          'table':'invent_info_patent_new',
#          'schema':'chemrobo_db',
#          'username':'root',
#          'password':'',
#          'host':'localhost',
#          'port':'3306',
#          'database':'chemrobo_db',
#          'trigger':'dupData',
#          'outputDirectory':r'D:\Freelancing\Data & Docs\Chem Robotics\Python Scripts\output'}
#    data={'pkey':'patent_publication_no',
#      'table':'invent_info_patent_new',
#      'schema':'chemrobo_db',
#      'username':'root',
#      'password':'',
#      'host':'localhost',
#      'port':'3306',
#      'database':'chemrobo_db',
#      'trigger':'dupData',
#      'outputDirectory':r'D:\Freelancing\Data & Docs\Chem Robotics\Python Scripts\output',
#      'columns':'gb_reg_no,product'}
    try:
        lb.config(text='Execution in Progress')
        lb.update_idletasks()
        data={k:v.get() for k,v in data.items()}
        print(data['columns'])
        query='select distinct p2.{pkey} from (SELECT  p1.{pkey} FROM {schema}.{table} as p1 INNER JOIN ( SELECT {pkey},  count(*)  as roaws FROM invent_info_patent GROUP BY {pkey} HAVING count(roaws) > 1) as p WHERE p.{pkey} = p1.{pkey})p2'.format(**data)
        print('Executing First Query:'+query)
        print('Query Execution Started at:'+str(datetime.datetime.now()))
        engine=sa.create_engine('mysql+pymysql://{username}:{password}@{host}:{port}/{database}'.format(**data))
        con=engine.connect()
        df = pd.read_sql(query,con)
        print('Query Execution Ended at :'+str(datetime.datetime.now()))
        pk = df[data['pkey']].to_list()
        query2='select * from {table} where '
        additional_clause=''
        in_clause=''
        sep=''
        flag=0
        if(len(pk)>0):
            for i in pk:
                if(i==None):
                    additional_clause='{pkey} is null'.format(**data)
                    flag=1
                else:
                    in_clause=in_clause+sep+"'"+i+"'"
                    sep=','
                    flag=1
            if(len(in_clause)>0):
                query2=query2+'{pkey}'.format(**data)+f' in ({in_clause})'
            if(len(in_clause)>0 and len(additional_clause)>0):
                query2=query2+f' or {additional_clause}'
            if(len(in_clause)==0 and len(additional_clause)>0):
                query2=query2+f'{additional_clause}'
        data['additional_clause']=additional_clause
        data['in_clause']=in_clause
        if(flag==1):
            print('Executing Second Query:'+query2)
            print('Query Execution Started at:'+str(datetime.datetime.now()))
            df2=pd.read_sql(query2.format(**data),con)
            print('Query Execution Ended at :'+str(datetime.datetime.now()))
            df2.sort_values(data['pkey'],inplace=True)
            if(data['trigger']=='dupList'):
                path=data['outputDirectory']+r'\DuplicateKeyList.txt'
                print('Generating Duplicate Key File at:'+path)
                with open(path,'w+') as temp:
                    temp.write('\n'.join(map(str,pk)))
                messagebox.showinfo("Successful","Successfully generated data at:\n"+path)
            elif(data['trigger']=='dupData'):
                path=data['outputDirectory']+r'\DuplicateData.xlsx'
                print('Generating Duplicate Data File at:'+path)
                df2.to_excel(path,index=False)
                messagebox.showinfo("Successful","Successfully generated excel at:\n"+path)
            elif(data['trigger']=='mergeDups'):
                print('Staring to Merge Data')
                col=df2.columns.to_list()
                col.remove(data['pkey'])
                #df3=df2.groupby(col)['gb_reg_no'].apply(list).reset_index()
                #merge_col_list=['gb_reg_no']
                merge_col_list=data['columns'].split(',')
                print({i:'first' if i not in merge_col_list else list for i in col})
                df3 = df2.groupby(data['pkey']).agg({i:'first' if i not in merge_col_list else lambda x:';'.join(map(str,x)) for i in col}).reset_index()
                try:
                    df3.drop('index',axis=1,inplace=True)
                except Exception:
                    pass
                print('Data Merged!')
                path=data['outputDirectory']+r'\MergedData.xlsx'
                print('Generating Merged Data File at:'+path)
                df3.to_excel(path,index=False)
                MsgBox = messagebox.askquestion('DatabaseUpdate','Do you want to update the database?',icon = 'warning')
                if MsgBox == 'yes':
                    data['ids']='","'.join(df3['patent_publication_no'].to_list())
                    print('Database Updation Started')
                    del_query = 'delete from {table} where {pkey} in ("{ids}")'.format(**data)
                    print('Executing Delete Query:'+del_query)
                    print('Query Execution Started at:'+str(datetime.datetime.now()))
                    con.execute(del_query)
                    print('Query Execution Ended at :'+str(datetime.datetime.now()))
                    messagebox.showinfo("Successful","Deleted Records")
#                    col2=df2.columns.to_list()
#                    try:
#                        col2.remove('index')
#                    except Exception:
#                        pass
                    try:                        
#                        df3.to_sql(name=data['table'],con=con,schema=data['schema'],if_exists='append',index=False,method='multi')
                        print('Executing Bulk inserts into database')
                        print('Query Execution Started at:'+str(datetime.datetime.now()))
                        df3.to_sql(name=data['table'],con=con,schema=data['schema'],if_exists='append',index=False)
                        print('Query Execution Ended at :'+str(datetime.datetime.now()))
                        messagebox.showinfo("Successful","Database Updated Successfully")
                    except Exception as E:
                        messagebox.showerror("Error","Failed to update database:\n"+str(E)[:5000])
                        print(E)
                else:
                    messagebox.showinfo("Successful","Successfully generated excel at(Database not updated):\n"+path)
        else:
            messagebox.showinfo("Successful","No Duplicates Found\n")
        lb.config(text='Execution Completed')
    except Exception as E:
        messagebox.showerror("Error","Failed to generate data:\n"+str(E))
        print(E)
        
        
    


def guiBuild():
    global lb
    master = Tk()
    master.title('Generate Duplicates')
    Label(master, text="Enter Connection Details").grid(row=0,column=1)
    Label(master, text="Database Name:").grid(row=1,column=0)
    Label(master, text="Host:").grid(row=2,column=0)
    Label(master, text="Port:").grid(row=2,column=2)
    Label(master, text="Username:").grid(row=3,column=0)
    Label(master, text="Password:").grid(row=3,column=2)
    Label(master, text="Data Level Details").grid(row=4,column=1)
    Label(master, text="Schema:").grid(row=5,column=0)
    Label(master, text="Table:").grid(row=6,column=0)
    Label(master, text="PrimaryKey:").grid(row=7,column=0)
    Label(master, text="Output Directory:").grid(row=8,column=0)
    Label(master, text="Trigger:").grid(row=9,column=0)
    Label(master, text="Columns To Merge:").grid(row=11,column=0)
    lb=Label(master, text="")
    lb.grid(row=12,column=2)
    database = StringVar()
    host = StringVar()
    port = StringVar()
    username = StringVar()
    password = StringVar()
    schema = StringVar()
    table = StringVar()
    pkey = StringVar()
    outputDirectory = StringVar()
    trigger = StringVar()
    columns = StringVar()
    Entry(master,textvariable=database, width=20).grid(row=1,column=1)
    Entry(master,textvariable=host, width=20).grid(row=2,column=1)
    Entry(master,textvariable=port, width=20).grid(row=2,column=4)
    Entry(master,textvariable=username, width=20).grid(row=3,column=1)
    Entry(master,textvariable=password, show='*', width=20).grid(row=3,column=4)
    Entry(master,textvariable=schema, width=20).grid(row=5,column=1)
    Entry(master,textvariable=table, width=20).grid(row=6,column=1)
    Entry(master,textvariable=pkey, width=20).grid(row=7,column=1)
    Entry(master,textvariable=outputDirectory, width=20).grid(row=8,column=1)
    Radiobutton(master,text='Duplicate Data',variable=trigger,value='dupData').grid(row=9,column=1)
    Radiobutton(master,text='Duplicate Keys',variable=trigger,value='dupList').grid(row=9,column=2)
    Radiobutton(master,text='Merge Duplicates',variable=trigger,value='mergeDups').grid(row=10,column=1)
    Entry(master,textvariable=columns, width=20).grid(row=11,column=1)
    database.set('chemrobo_db')
    host.set('localhost')
    port.set('3306')
    username.set('root')
    password.set('')
    trigger.set('Duplicate Data')
    rs = {'pkey':pkey,
          'table':table,
          'schema':schema,
          'username':username,
          'password':password,
          'host':host,
          'port':port,
          'database':database,
          'trigger':trigger,
          'outputDirectory':outputDirectory,
          'columns':columns}
    Button(master,text='Submit',width=15,command=lambda:searchDups(rs)).grid(row=12,column=1)
    master.mainloop()
    

if __name__=="__main__":
    print('Execution Started at:'+str(datetime.datetime.now()))
    guiBuild()
    print('Execution Ended at :'+str(datetime.datetime.now()))