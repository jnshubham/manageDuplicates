from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, RadioField, PasswordField
from wtforms.validators import DataRequired, Optional

class dataFetch(FlaskForm):
    databaseName = StringField('DatabaseName', validators=[DataRequired()])
    host = StringField('host', validators=[DataRequired()])
    port = StringField('port', validators=[DataRequired()])
    username = StringField('username', validators=[DataRequired()])
    password = PasswordField('password', validators=[DataRequired()])
    table = StringField('table', validators=[DataRequired()])
    primaryKey = StringField('primaryKey', validators=[DataRequired()])
    outputDirectory = StringField('outputDirectory', validators=[DataRequired()])
    trigger = RadioField('Trigger',choices=[('Duplicate Data','dupData'),('Duplicate Keys','dupList'),('Merge Duplicates','mergeDups')], validators=[DataRequired()])
    columnsToMerge = StringField('columnsToMerge', validators=[Optional()])
    submit = SubmitField('submit')