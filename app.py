from flask import Flask, render_template, redirect, url_for, flash
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
import psycopg2
import random
import string
from wtforms import StringField, PasswordField, SelectField, BooleanField
from wtforms.validators import InputRequired, NumberRange

app = Flask(__name__)
app.config["SECRET_KEY"] = "change this really really secret string"
Bootstrap(app)


class DbCreateForm(FlaskForm):
    """
    Database creation form definition
    """
    dbServer = SelectField("Database Server", coerce=int, validators=[NumberRange(min=0, max=5, message="Server name is foobar")])
    dbName = StringField("Database Name", validators=[InputRequired()])
    dbAdmin = StringField("Admin account", default="postgres", validators=[InputRequired()])
    dbAdminPW = StringField("Admin Password", validators=[InputRequired()])
    dbRunSQL = BooleanField("Run generated SQL code", default=False)


@app.route("/")
def index():
    """
    Nothing to see here- go straight to dbcreate
    :return: None
    """
    return redirect(url_for("dbcreate"))


@app.route("/dbcreate", methods=["GET", "POST"])
def dbcreate():
    """
    Create postgres database on requested instance using supplied credentials
    :return: None
    """

    s = list()
    u = list()
    errors = False
    form = DbCreateForm()
    form.dbServer.choices = [(0, "rds-pg-rails-dev"),
                             (1, "rds-postgres-rails-test"),
                             (2, "rds-postgres-rails-prod"),
                             (3, "rds-postgres-launchpad"),
                             (4, "piportal-prime"),
                             (5, "piportal-prod"),]

    if form.validate_on_submit():
        rds_domain = "cms6g4vqt77v.us-east-1.rds.amazonaws.com"
        rds_fqdn = form.dbServer.choices[form.dbServer.data][1] + "." + rds_domain
        rds_db = form.dbName.data
        rds_admin = form.dbAdmin.data
        rds_password = form.dbAdminPW.data
        new_owner = form.dbName.data + "_owner"
        new_user = form.dbName.data + "_user"
        po = pw_gen(16)
        pu = pw_gen(16)

        try:
            con = psycopg2.connect(dbname="postgres", host=rds_fqdn, user=rds_admin, password=rds_password)
        except psycopg2.Error as err:
            errors = True
            flash(err)
            return render_template("dbcreate.html", form=form, errors=errors)

        con.autocommit = True
        cur = con.cursor()

        """
        Verify database and roles do not already exist
        """
        s.append("SELECT 1 FROM pg_database WHERE datname='{}';".format(rds_db))
        s.append("SELECT 1 FROM pg_roles WHERE rolname='{}'".format(new_owner))
        s.append("SELECT 1 FROM pg_roles WHERE rolname='{}'".format(new_user))
        s.append("SELECT 1 FROM pg_roles WHERE rolname='{}_reader'".format(rds_db))
        s.append("SELECT 1 FROM pg_roles WHERE rolname='{}_writer'".format(rds_db))

        for i in range(0, len(s)):
            cur.execute(s[i])
            if cur.fetchone():
                errors = True
                flash("requested object {} exists".format(s[i].split("=")[1]))

        if errors:
            cur.close()
            con.close()
            return render_template("dbcreate.html", form=form, errors=errors)

        """
        Create roles/users and database
        """
        if form.dbRunSQL.data:
            pass
        else:
            flash("-- -------------------------------------")

        s = list()
        s.append({"do": "CREATE USER {} CREATEDB PASSWORD '{}';".format(new_owner, po),
                  "undo": "DROP USER {};".format(new_owner)})
        s.append({"do": "CREATE USER {} PASSWORD '{}';".format(new_user, pu),
                  "undo": "DROP USER {};".format(new_user)})
        s.append({"do": "CREATE ROLE {}_reader;".format(rds_db),
                  "undo": "DROP ROLE {}_reader;".format(rds_db)})
        s.append({"do": "CREATE ROLE {}_writer;".format(rds_db),
                  "undo": "DROP ROLE {}_writer;".format(rds_db)})
        s.append({"do": "GRANT {} TO {};".format(new_owner, rds_admin),
                  "undo": None})
        s.append({"do": "GRANT {}_reader TO {};".format(rds_db, new_user),
                  "undo": None})
        s.append({"do": "GRANT {}_writer TO {};".format(rds_db, new_user),
                  "undo": None})
        s.append({"do": "CREATE DATABASE {} WITH OWNER = {};".format(rds_db, new_owner),
                  "undo": "DROP DATABASE {}".format(rds_db)})

        for i in range(0, len(s)):
            if form.dbRunSQL.data:
                try:
                    cur.execute(s[i]["do"])
                    if s[i]["undo"]:
                        u.append(s[i]["undo"])
                except psycopg2.Error as err:
                    errors = True
                    flash(err)
            else:
                flash(s[i]["do"])

        if errors:
            """
            Undo everything was done, in reverse order
            """
            u.reverse()
            for i in range(0, len(u)):
                cur.execute(u[i])

        cur.close()
        con.close()

        if errors:
            return render_template("dbcreate.html", form=form, errors=errors)

        """
        The program needs to reconnect using the new database for the next set of grants
        """
        if form.dbRunSQL.data:
            try:
                con = psycopg2.connect(dbname=rds_db, host=rds_fqdn, user=rds_admin, password=rds_password)
            except psycopg2.Error as err:
                flash(err)
                return render_template("dbcreate.html", form=form, errors=errors)

            con.autocommit = True
            cur = con.cursor()
        else:
            flash("\connect {}".format(rds_db))

        s = list()
        s.append({"do": "GRANT SELECT ON ALL TABLES IN SCHEMA PUBLIC TO {}_reader;".format(rds_db),
                  "undo": None})
        s.append({"do": "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA PUBLIC TO {}_writer;".format(rds_db),
                  "undo": None})

        for i in range(0, len(s)):
            if form.dbRunSQL.data:
                try:
                    cur.execute(s[i]["do"])
                    if s[i]["undo"]:
                        u.append(s[i]["undo"])
                except psycopg2.Error as err:
                    errors = True
                    flash(err)
            else:
                flash(s[i]["do"])

        if errors:
            """
            Undo everything that was done, in reverse order. 
            Can't drop open database. 
            Close connection and reconnect
            """
            con.close()

            try:
                con = psycopg2.connect(dbname="postgres", host=rds_fqdn, user=rds_admin, password=rds_password)
            except psycopg2.Error as err:
                errors = True
                flash(err)
                return render_template("dbcreate.html", form=form, errors=errors)

            con.autocommit = True
            cur = con.cursor()
            u.reverse()
            for i in range(0, len(u)):
                cur.execute(u[i])

        if con:
            cur.close()
            con.close()

        if errors:
            return render_template("dbcreate.html", form=form, errors=errors)

        flash("-- -------------------------------------")
        flash("-- server: {}".format(rds_fqdn))
        flash("-- database: {}".format(rds_db))
        flash("-- owner: {}".format(new_owner))
        flash("-- owner password: {}".format(po))
        flash("-- user: {}".format(new_user))
        flash("-- user password: {}".format(pu))

    return render_template("dbcreate.html", form=form, errors=errors)


def pw_gen(l):
    """
    Generate a random password
    :param l: expecting an integer for password length, will default to 16
    :return: a password string of length l
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


if __name__ == "__main__":
    app.run(debug=True)
