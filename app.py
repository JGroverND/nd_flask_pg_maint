from flask import Flask, render_template, redirect, url_for, flash
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
import psycopg2
import random
import string
from wtforms import StringField, PasswordField, SelectField, BooleanField
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
    dbRunSQL = BooleanField('Run SQL')


@app.route('/')
def index():
    """
    Nothing to see here- go straight to dbcreate
    :return: None
    """
    return redirect(url_for('dbcreate'))


@app.route('/dbcreate', methods=['GET', 'POST'])
def dbcreate():
    """
    Create postgres database on requested instance using supplied credentials
    :return: None
    """
    form = DbCreateForm()
    sql = list()
    doc = list()

    form.dbServer.choices = [(0, 'piportal-prime'),
                             (1, 'piportal-prod'),
                             (2, 'rds-pg-rails-dev'),
                             (3, 'rds-postgres-rails-test'),
                             (4, 'rds-postgres-rails-prod'),
                             (5, 'rds-postgres-launchpad')]

    if form.validate_on_submit():
        rds_domain = 'cms6g4vqt77v.us-east-1.rds.amazonaws.com'
        rds_fqdn = form.dbServer.choices[form.dbServer.data][1] + '.' + rds_domain
        rds_db = form.dbName.data
        rds_admin= form.dbAdmin.data
        rds_password = form.dbAdminPW.data
        new_owner = form.dbName.data + '_owner'
        new_user = form.dbName.data + '_user'
        new_owner_pw = pw_gen(16)
        new_user_pw = pw_gen(16)

        """
        List of tuples (stage, command, backout command)
        """
        sql.append(('verify', 'SELECT 1 FROM pg_database WHERE datname=\'{}\''.format(rds_db), None))
        sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}\''.format(new_owner), None))
        sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}\''.format(new_user), None))
        sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}_reader\''.format(rds_db), None))
        sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}_writer\''.format(rds_db), None))

        sql.append(('create', 'CREATE USER {} WITH LOGIN CREATEDB PASSWORD \'{}\';'.format(new_owner, new_owner_pw),
                    'drop user {};'.format(new_owner)))
        sql.append(('create', 'CREATE USER {} WITH LOGIN PASSWORD \'{}\';'.format(new_user, new_user_pw),
                    'drop user {};'.format(new_user)))
        sql.append(('create', 'CREATE ROLE {}_reader;'.format(rds_db),
                    'drop role {}_reader;'.format(rds_db)))
        sql.append(('create', 'CREATE ROLE {}_writer;'.format(rds_db),
                    'drop role {}_writer;'.format(rds_db_)))
        sql.append(('create', 'GRANT {} to {};'.format(new_owner, rds_admin),
                    'revoke {} from {};'.format(new_owner, rds_admin)))
        sql.append(('create', 'CREATE DATABASE {} WITH OWNER = {};'.format(rds_db, new_owner),
                    'drop database {};'.format(rds_db)))

        sql.append(('grant', '\connect {}'.format(rds_db), None))
        sql.append(('grant', 'GRANT {}_reader to {};'.format(rds_db, new_user),
                    'revoke {}_reader from {};'.format(rds_db, new_user)))
        sql.append(('grant', 'GRANT {}_writer to {};'.format(rds_db, new_user),
                    'revoke {}_writer from {};'.format(rds_db, new_user)))
        sql.append(('grant', 'grant select on all tables in schema public to {}_reader;'.format(rds_db),
                    'revoke select on all tables in schema public from {}_reader;'.format(rds_db)))
        sql.append(('grant', 'grant select, insert, update, delete on all tables in schema public to {}_writer;'.format(rds_db),
                    'revoke select, insert, update, delete on all tables in schema public from {}_writer;'.format(rds_db)))

        sql.append(('doc', 'server: {}'.format(rds_fqdn), None))
        sql.append(('doc', 'database: {}'.format(rds_db), None))
        sql.append(('doc', 'owner: {}'.format(new_owner), None))
        sql.append(('doc', 'owner password: {}'.format(po), None))
        sql.append(('doc', 'user: {}'.format(new_user), None))
        sql.append(('doc', 'user password: {}'.format(pu), None))

        if form.dbRunSQL.data:
            try:
                conn = psycopg2.connect(dbname='postgres', host=rds_fqdn, user=rds_admin, password=rds_password)
                conn.autocommit = True
                cur = conn.cursor()
            except psycopg2.Error as err:
    		    """
    			Connecion failed , bail.
    			"""
                flash(err)
                return render_template('dbcreate.html', form=form)

            if not dbcreate_verify():
                """
        		Verification failed, bail.
        		"""
                cur.close()
                conn.close()
                return render_template('dbcreate.html', form=form)
                
            if not dbcreate_create():
                """
                Creation failed, bail.
                """
                cur.close()
                conn.close()
                return render_template('dbcreate.html', form=form)

            if not dbcreate_grant():
                """
                Grants failed, bail
                """
                cur.close()
                conn.close()
                return render_template('dbcreate.html', form=form)
        else:
            for i in range(0, len(sql)):
                flash(sql[i][1]

        cur.close()
        conn.close()

    return render_template('dbcreate.html', form=form, secret=secret)


def dbcreate_verify():
    errors = False
    
    for i in range(0, len(sql)):
        if sql[i][0] == 'verify':
            try:
                cur.execute(sql[i])
                if cur.fetchone():
                    errors = True
                    flash('requested object {} already exists'.format(sql[i].split('=')[1]))
            except psycopg2.Error as err:
                errors = True
                flash(err)

    return errors:


def dbcreate_create():
    errors = False

    for i in range(0, len(sql)):
        if sql[i][0] == 'create':
            try:
                cur.execute(sql[i][1])
            except psycopg2.Error as err:
                errors = True
                flash(err)

    if errors:
        dbcreate_backout()
            
    return errors


def dbcreate_grant():
    errors = False
    
    cur.close()
    conn.close()
    
    try:
        conn = psycopg2.connect(dbname=rds_db, host=rds_fqdn, user=rds_admin, password=rds_password)
        conn.autocommit = True
        cur = conn.cursor()
    except psycopg2.Error as err:
        flash('Could not switch to {} to complete grants'.format(rds_db))
        flash(err)
        return render_template('dbcreate.html', form=form)
                        
    for i in range(0, len(sql)):
        if sql[i][0] == 'grant' and sql[i][1] != '\connect'  :
            try:
                cur.execute(sql[i][1])
            except psycopg2.Error as err:
                errors = True
                flash(err)

    if errors:
        dbcreate_backout()
            
    return errors


def dbcreate_backout():
    for i in range(0, len(sql)):
        try: 
            cur.execute(sql[i][2])
        except psycopg2.Error
            pass
    return


def pw_gen(l):
    """
    Generate a password of l length
    :param l: integer, length of password
    :return: p a string of random characters of lenth l
    """
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
