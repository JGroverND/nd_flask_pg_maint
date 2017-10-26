from flask import Flask, render_template, redirect, url_for, flash
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
import psycopg2
import random
import string
from wtforms import StringField, PasswordField, SelectField
from wtforms.validators import InputRequired, NumberRange

app = Flask(__name__)
app.config['SECRET_KEY'] = 'change this really really secret string'
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
    '''
    Nothing to see here- go straight to dbcreate
    :return: None
    '''
    return redirect(url_for('dbcreate'))


@app.route('/dbcreate', methods=['GET', 'POST'])
def dbcreate():
    '''
    Create postgres database on requested instance using supplied credentials
    :return: None
    '''
    form = DbCreateForm()
    form.dbServer.choices = [(0, 'piportal-prime'), (1, 'piportal-prod'), (2, 'rds-pg-rails-dev'),
                             (3, 'rds-postgres-rails-test'), (4, 'rds-postgres-rails-prod'),
                             (5, 'rds-postgres-launchpad')]

    if form.validate_on_submit():
        rds_domain = 'cms6g4vqt77v.us-east-1.rds.amazonaws.com'
        rds_fqdn = form.dbServer.choices[form.dbServer.data][1] + '.' + rds_domain
        rds_db = form.dbName.data
        rds_admin= form.dbAdmin.data
        rds_password = form.dbAdminPW.data

        try:
            conn = psycopg2.connect(dbname='postgres', host=rds_fqdn, user=rds_admin, password=rds_password)
        except psycopg2.OperationalError:
            flash('FAIL: bad login information for postgres database {}'.format(rds_fqdn))
            return render_template('dbcreate.html', form=form)

        new_owner = form.dbName.data + '_owner'
        new_user = form.dbName.data + '_user'
        po = pw_gen(16)
        pu = pw_gen(16)

        # create accounts
        conn.autocommit = True
        cur = conn.cursor()

        s = []
        s.append('CREATE ROLE {} WITH LOGIN   NOSUPERUSER NOCREATEROLE CREATEDB INHERIT NOREPLICATION CONNECTION LIMIT -1 PASSWORD \'{}\';'.format(new_owner, po))
        s.append('CREATE ROLE {} WITH LOGIN   NOSUPERUSER NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT -1 PASSWORD \'{}\';'.format(new_user, pu))
        s.append('CREATE ROLE {}_reader WITH NOLOGIN NOSUPERUSER NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT -1;'.format(rds_db))
        s.append('CREATE ROLE {}_writer WITH NOLOGIN NOSUPERUSER NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT -1;'.format(rds_db))
        s.append('GRANT {} to {};'.format(new_owner, rds_admin))
        s.append('GRANT {}_reader to {};'.format(rds_db, new_user))
        s.append('GRANT {}_writer to {};'.format(rds_db, new_user))
        s.append ('CREATE DATABASE {} WITH OWNER = {} ENCODING = \'UTF8\' CONNECTION LIMIT = -1;'.format(rds_db, new_owner))

        for i in range(0, len(s)):
            try:
                cur.execute(s[i])
            except psycopg2.ProgrammingError:
                flash('FAIL: {}'.format(s[i]))
        '''
        The program needs to connect to the new database for the next set of grants
        '''
        cur.close()
        conn.close()

        try:
            conn = psycopg2.connect(dbname=rds_db, host=rds_fqdn, user=rds_admin, password=rds_password)
        except:
            flash('FAIL: bad login information for {} on {}'.format(rds_db, rds_fqdn))
            return render_template('dbcreate.html', form=form)

        conn.autocommit = True
        cur = conn.cursor()

        s = []
        s.append('grant select on all tables in schema public to {}_reader;'.format(rds_db))
        s.append('grant select, insert, update, delete on all tables in schema public to {}_writer;'.format(rds_db))

        for i in range(0, len(s)):
            try:
                cur.execute(s[i])
            except psycopg2.ProgrammingError:
                flash('FAIL: {}'.format(s[i]))

        cur.close()
        conn.close()

        flash('server: {}'.format(rds_fqdn))
        flash('database: {}'.format(rds_db))
        flash('owner: {}'.format(new_owner))
        flash('owner password: {}'.format(po))
        flash('user: {}'.format(new_user))
        flash('user password: {}'.format(pu))

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
