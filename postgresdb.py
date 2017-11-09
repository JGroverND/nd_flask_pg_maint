import psycopg2
import random
import string


class PostgresDb:
    """
    This class contains the connection tp the database as well as the  generated SQL for the nnew database
    """

    def __init__(self,  dbrunsql, dbserver, dbname, dbadmin, dbadminpw):
        """
        The arguments are collected from the form
        """
        self.runSQL = dbrunsql
        self.dbServer = dbserver
        self.domain = 'cms6g4vqt77v.us-east-1.rds.amazonaws.com'
        self.fqdn = self.dbServer + '.' + self.domain
        self.dbName = dbname
        self.dbAdmin = dbadmin
        self.dbAdminPW = dbadminpw
        self.user = self.dbName + '_user'
        self.owner = self.dbName + '_owner'
        self.reader = self.dbName + '_reader'
        self.writer = self.dbName + '_writer'
        self.owner_pw = self.generate_password(16)
        self.user_pw = self.generate_password(16)
        self.sql = list()
        self.generate_sql()

    def generate_password(self, l):
        """
        Generate a random password
        """
        c = string.ascii_letters + string.digits
        p = ''
        try:
            pwl = int(l)
        except ValueError:
            pwl = 16
        for x in range(pwl):
            char = random.choice(c)
            p = p + char
        return p

    def generate_sql(self):
        """
        Store SQL code and undo code in a list of tuples. The tuple contains the stage of the process, the
        SQL to be executed, and the SQL to reverse the change (or None if no change was made)
        """
        self.sql.append(('verify', 'SELECT 1 FROM pg_database WHERE datname=\'{}\';'.format(self.dbName), None))
        self.sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}\';'.format(self.owner), None))
        self.sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}\';'.format(self.user), None))
        self.sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}_reader\';'.format(self.dbName), None))
        self.sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}_writer\';'.format(self.dbName), None))

        self.sql.append(('create',
                         'CREATE USER "{}" WITH LOGIN CREATEDB PASSWORD \'{}\';'.format(self.owner, self.owner_pw),
                         'drop user "{}";'.format(self.owner)))
        self.sql.append(('create',
                         'CREATE USER "{}" WITH LOGIN PASSWORD \'{}\';'.format(self.user, self.user_pw),
                         'drop user "{}";'.format(self.user)))
        self.sql.append(('create',
                         'CREATE ROLE "{}";'.format(self.reader),
                         'drop role "{}";'.format(self.reader)))
        self.sql.append(('create',
                         'CREATE ROLE "{}";'.format(self.writer),
                         'drop role "{}";'.format(self.writer)))
        self.sql.append(('create',
                         'GRANT "{}" to "{}";'.format(self.owner, self.dbAdmin),
                         'revoke "{}" from "{}";'.format(self.owner, self.dbAdmin)))
        self.sql.append(('create',
                         'CREATE DATABASE "{}" WITH OWNER = "{}";'.format(self.dbName, self.owner),
                         'drop database "{}";'.format(self.dbName)))

        self.sql.append(('grant',
                         '\connect "{}"'.format(self.dbName),
                         None))
        self.sql.append(('grant',
                         'GRANT "{}" to "{}";'.format(self.reader, self.user),
                         'revoke "{}" from "{}";'.format(self.reader, self.user)))
        self.sql.append(('grant',
                         'GRANT "{}" to "{}";'.format(self.writer, self.user),
                         'revoke "{}" from "{}";'.format(self.writer, self.user)))
        self.sql.append(('grant',
                         'grant select on all tables in schema public to "{}";'.format(self.reader),
                         'revoke select on all tables in schema public from "{}";'.format(self.reader)))
        self.sql.append((
            'grant',
            'grant select, insert, update, delete on all tables in schema public to "{}";'.format(self.writer),
            'revoke select, insert, update, delete on all tables in schema public from "{}";'.format(self.writer)))

        self.sql.append(('doc', '-- ----------> Send to requester', None))
        self.sql.append(('doc', 'server: "{}"'.format(self.fqdn), None))
        self.sql.append(('doc', 'database: "{}"'.format(self.dbName), None))
        self.sql.append(('doc', 'owner: "{}"'.format(self.owner), None))
        self.sql.append(('doc', 'owner password: "{}"'.format(self.owner_pw), None))
        self.sql.append(('doc', 'user: "{}"'.format(self.user), None))
        self.sql.append(('doc', 'user password: "{}"'.format(self.user_pw), None))

    def db_connect(self, db='postgres'):
        """
        Create a connection to the requested database
        """
        try:
            self.con = psycopg2.connect(dbname=db, host=self.fqdn, user=self.dbAdmin, password=self.dbAdminPW)
            self.con.autocommit = True
            self.cur = self.con.cursor()
            return True

        except psycopg2.Error as err:
            """
            Connection failed , bail.
            """
            return False

    def db_disconnect(self):
        """
        Disconnect from the database
        """
        self.cur.close()
        self.con.close()
