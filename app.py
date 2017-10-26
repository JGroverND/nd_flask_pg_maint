from flask import Flask, render_template, redirect, url_for, flash
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
import psycopg2
import random
import string
from wtforms import StringField, PasswordField, SelectField
from wtforms.validators import InputRequired, NumberRange

app = Flask(__name__)
app.config['SECRET_KEY'] = 'this is really really secret'
Bootstrap(app)


class DbCreateForm(FlaskForm):
    """
    Database creation form definition
    """
    dbServer = SelectField('Database Server', coerce=int, validators=[NumberRange(min=0, max=5, message='Server name is foobar')])
    dbName = StringField('Database Name',  validators=[InputRequired()])
    dbAdmin = StringField('Admin account', default='postgres', validators=[InputRequired()])
    dbAdminPW = PasswordField('Admin Password', validators=[InputRequired()])


@app.route('/')
def index():
    return redirect(url_for('dbcreate'))


@app.route('/dbcreate', methods=['GET', 'POST'])
def dbcreate():
    form = DbCreateForm()
    form.dbServer.choices = [(0, 'piportal-prime'), (1, 'piportal-prod'), (2, 'rds-pg-rails-dev'),
                             (3, 'rds-postgres-rails-test'), (4, 'rds-postgres-rails-prod'),
                             (5, 'rds-postgres-launchpad')]

    if form.validate_on_submit():
        rds_domain = 'cms6g4vqt77v.us-east-1.rds.amazonaws.com'
        rds_fqdn = form.dbServer.choices[form.dbServer.data][1] + '.' + rds_domain
        rds_db = form.dbName.data
        rds_user = form.dbAdmin.data
        rds_password = form.dbAdminPW.data
        rds_owner = form.dbName.data + '_owner'

        try:
            conn = psycopg2.connect(dbname='postgres', host=rds_fqdn, user=rds_user, password=rds_password)
        except:
            flash('Bad Admin Creds postgres on ' + rds_fqdn + ' with ' + rds_user + '/' + rds_password)
            return render_template('dbcreate.html', form=form)

        po = pw_gen(16)
        pu = pw_gen(16)

        # create accounts
        cur = conn.cursor()

        s = []
        s.append('CREATE ROLE ' + form.dbName.data + '_owner  WITH LOGIN   NOSUPERUSER NOCREATEROLE CREATEDB INHERIT NOREPLICATION CONNECTION LIMIT -1 PASSWORD \'' + po + '\';')
        s.append('CREATE ROLE ' + form.dbName.data + '_user   WITH LOGIN   NOSUPERUSER NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT -1 PASSWORD \'' + pu + '\';')
        s.append('CREATE ROLE ' + form.dbName.data + '_reader WITH NOLOGIN NOSUPERUSER NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT -1;')
        s.append('CREATE ROLE ' + form.dbName.data + '_writer WITH NOLOGIN NOSUPERUSER NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT -1;')
        s.append('GRANT ' + form.dbName.data + '_owner  to ' + form.dbAdmin.data + ';')
        s.append('GRANT ' + form.dbName.data + '_reader to  ' + form.dbName.data + '_user;')
        s.append('GRANT ' + form.dbName.data + '_writer to  ' + form.dbName.data + '_user;')

        for i in range(0, len(s)):
            flash(s[i])
            cur.execute(s[i])

        conn.commit()
        cur.close()

        # create database
        cur = conn.cursor()
        s = 'CREATE DATABASE ' + form.dbName.data + ' WITH OWNER = ' + form.dbName.data + '_owner ENCODING = \'UTF8\' CONNECTION LIMIT = -1;'
        flash(s)
        cur.execute(s)
        conn.commit()
        cur.close()
        conn.close()

        # grants to user account
        conn = psycopg2.connect(dbname=rds_db, host=rds_fqdn, user=rds_owner, password=po)
        cur = conn.cursor()

        s = []
        s.append('grant select on all tables in schema public to ' + form.dbName.data + '_reader;')
        s.append('grant select, insert, update, delete on all tables in schema public to ' + form.dbName.data + '_writer;')

        for i in range(0, len(s)):
            flash(s[i])
            cur.execute(s[i])

        conn.commit()
        cur.close()
        conn.close()

    return render_template('dbcreate.html', form=form)


def pw_gen(l):
    c = string.ascii_letters + string.digits
    p = ""
    try:
        pwl = int(l)
    except ValueError:
        pwl = 16
    for x in range(pwl):
        char = random.choice(c)
        p = p + char
    return p


if __name__ == '__main__':
    app.run(debug=True)
