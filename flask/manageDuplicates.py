import pandas as pd
import sqlalchemy as sa
import pymysql
import datetime


def searchDups(data):
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
        #data={k:v.get() for k,v in data.items()}
        #print(data['columns'])
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
            print(data['trigger'])
            if(data['trigger']=='dupList'):
                path=data['outputDirectory']+r'\DuplicateKeyList.txt'
                print('Generating Duplicate Key File at:'+path)
                with open(path,'w+') as temp:
                    temp.write('\n'.join(map(str,pk)))
                print("Successful","Successfully generated data at:\n"+path)
            elif(data['trigger']=='dupData'):
                path=data['outputDirectory']+r'\DuplicateData.xlsx'
                print('Generating Duplicate Data File at:'+path)
                df2.to_excel(path,index=False)
                print("Successful","Successfully generated excel at:\n"+path)
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
                #MsgBox = messagebox.askquestion('DatabaseUpdate','Do you want to update the database?',icon = 'warning')
                MsgBox='No'
                if MsgBox == 'yes':
                    data['ids']='","'.join(df3['patent_publication_no'].to_list())
                    print('Database Updation Started')
                    del_query = 'delete from {table} where {pkey} in ("{ids}")'.format(**data)
                    print('Executing Delete Query:'+del_query)
                    print('Query Execution Started at:'+str(datetime.datetime.now()))
                    con.execute(del_query)
                    print('Query Execution Ended at :'+str(datetime.datetime.now()))
                    print("Successful","Deleted Records")
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
                        print("Successful","Database Updated Successfully")
                    except Exception as E:
                        print("Error Failed to update database:\n"+str(E))
                        print(E)
                else:
                    print("Successful Successfully generated excel at(Database not updated):\n"+path)
        else:
            print("Successful No Duplicates Found\n")
    except Exception as E:
        print(E)