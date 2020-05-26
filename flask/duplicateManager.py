from flask import Flask, render_template, url_for
from forms import dataFetch
from manageDuplicates import searchDups
app = Flask(__name__)
app.config['SECRET_KEY']='089bcc9bf633533dd60b6f19402d1634'


@app.route('/',methods=['GET','POST'])
def hello_world():
    form = dataFetch()
    if(form.validate_on_submit()):
        trig = {'Duplicate Data':'dupData',
        'Duplicate Keys':'dupList',
        'Merge Duplicates':'mergeDups'}.get(form.trigger.data)
        data = {
            'pkey':form.primaryKey.data,
            'table':form.table.data,
            'schema':form.databaseName.data,
            'username':form.username.data,
            'password':form.password.data,
            'host':form.host.data,
            'port':form.port.data,
            'database':form.databaseName.data  ,
            'trigger':trig,
            'outputDirectory':form.outputDirectory.data
            }
        print(data)
        searchDups(data)


    return render_template('home.html',form=form)

if __name__=='__main__':
    app.run(debug=True)