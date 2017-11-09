from flask import Flask, render_template, redirect, url_for, flash
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from postgresdb import PostgresDb
import psycopg2
from wtforms import StringField, PasswordField, SelectField, BooleanField
from wtforms.validators import InputRequired, NumberRange

app = Flask(__name__)
app.config['SECRET_KEY'] = 'change this really really secret string'
Bootstrap(app)


class DbCreateForm(FlaskForm):
    """
    Database creation form definition
    """
    dbServer = SelectField('Database Server', coerce=int,
                           validators=[NumberRange(min=0, max=5, message='Server name is foobar')])
    dbName = StringField('Database Name',  validators=[InputRequired()])
    dbAdmin = StringField('Admin account', default='postgres', validators=[InputRequired()])
    dbAdminPW = PasswordField('Admin Password', validators=[InputRequired()])
    dbRunSQL = BooleanField('Run SQL')


@app.route('/')
def index():
    form = DbCreateForm()
    form.dbServer.choices = [(0, 'piportal-prime'),
                             (1, 'piportal-prod'),
                             (2, 'rds-pg-rails-dev'),
                             (3, 'rds-postgres-rails-test'),
                             (4, 'rds-postgres-rails-prod'),
                             (5, 'rds-postgres-launchpad')]
    return render_template('dbcreate.html', form=form)


@app.route('/dbcreate', methods=['GET', 'POST'])
def dbcreate():
    """
    Create postgres database on requested instance using supplied credentials
    """
    form = DbCreateForm()
    form.dbServer.choices = [(0, 'piportal-prime'),
                             (1, 'piportal-prod'),
                             (2, 'rds-pg-rails-dev'),
                             (3, 'rds-postgres-rails-test'),
                             (4, 'rds-postgres-rails-prod'),
                             (5, 'rds-postgres-launchpad')]

    if form.validate_on_submit():
        rds = PostgresDb(form.dbRunSQL.data,
                         form.dbServer.choices[form.dbServer.data][1],
                         form.dbName.data,
                         form.dbAdmin.data,
                         form.dbAdminPW.data)

        if rds.runSQL:
            if rds.db_connect():
                pass
            else:
                """
                Connection failed, bail.
                """
                flash('Failed to connect to default database')
                return render_template('dbcreate.html', form=form, errors=True)

            if dbcreate_verify(rds):
                pass
            else:
                """
                Verification failed, bail.
                """
                rds.db_disconnect()
                return render_template('dbcreate.html', form=form, errors=True)

            if dbcreate_create(rds):
                pass
            else:
                """
                Creation failed, bail.
                """
                rds.db_disconnect()
                return render_template('dbcreate.html', form=form, errors=True)
                
            rds.db_disconnect()
            if rds.db_connect(rds.dbName):
                pass
            else:
                """
                Connection failed, bail.
                """
                flash('Failed to connect to database {}'.format(rds.dbName))
                return render_template('dbcreate.html', form=form, errors=True)

            if dbcreate_grant(rds):
                rds.db_disconnect()
            else:
                """
                Grants failed, bail
                """
                rds.db_disconnect()
                return render_template('dbcreate.html', form=form, errors=True)

            flash("database {} created.".format(rds.dbName))
            for i in range(0, len(rds.sql)):
                if rds.sql[i][0] == 'doc':
                    flash(rds.sql[i][1])
        else:
            """
            Display SQL, don't run it
            """
            for i in range(0, len(rds.sql)):
                if rds.sql[i][0] != 'verify':
                    flash(rds.sql[i][1])

        rds.runSQL = False
        dbcreate_backout(rds)

    return render_template('dbcreate.html', form=form, errors=False)


def dbcreate_verify(rds):
    """
    Verify the requested objects don't already exist using generated SQL
    """
    verified = True

    for i in range(0, len(rds.sql)):
        if rds.sql[i][0] == 'verify':
            try:
                rds.cur.execute(rds.sql[i][1])
                if rds.cur.fetchone():
                    verified = False
                    flash('requested object {} already exists'.format(rds.sql[i][1].split('=')[1]))

            except psycopg2.Error as err:
                verified = False
                flash(err)

    return verified


def dbcreate_create(rds):
    """
    Create the requested objects from the generated SQL
    """
    created = True

    for i in range(0, len(rds.sql)):
        if rds.sql[i][0] == 'create':
            try:
                rds.cur.execute(rds.sql[i][1])
            except psycopg2.Error as err:
                created = False
                flash(err)

    if not created:
        dbcreate_backout(rds)
            
    return created


def dbcreate_grant(rds):
    """
    Grant database permissions using generated SQL
    """
    granted = True

    for i in range(0, len(rds.sql)):
        if rds.sql[i][0] == 'grant' and rds.sql[i][1][1:8] != 'connect':
            try:
                rds.cur.execute(rds.sql[i][1])
            except psycopg2.Error as err:
                granted = False
                flash(err)

    if not granted:
        dbcreate_backout(rds)
            
    return granted


def dbcreate_backout(rds):
    """
    Use the generated back out SQL to undo previous actions or to display commands
    """
    rev = list()

    for i in range(0, len(rds.sql)):
        if rds.sql[i][2]:
            rev.append(rds.sql[i][2])

    rev.reverse()
    if not rds.runSQL:
        flash('-- ----------> SQL to remove database')

    for i in range(0, len(rev)):
        if rds.runSQL and rds.cur:
            try:
                rds.cur.execute(rev[i])
            except psycopg2.Error:
                pass
        else:
            flash(rev[i])


if __name__ == '__main__':
    app.run(debug=True)
