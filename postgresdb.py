import psycopg2
import random
import string


class PostgresDb:

    def __init__(self,  dbrunsql, dbserver, dbadmindb, dbname, dbadmin, dbadminpw):
        self.runSQL = dbrunsql
        self.dbServer = dbserver
        self.domain = 'cms6g4vqt77v.us-east-1.rds.amazonaws.com'
        self.fqdn = self.dbServer + '.' + self.domain
        self.dbAdminDB = dbadmindb
        self.dbName = dbname
        self.dbAdmin = dbadmin
        self.dbAdminPW = dbadminpw
        self.user = dbname + '_user'
        self.owner = dbname + '_owner'
        self.reader = dbname + '_reader'
        self.writer = dbname + '_writer'
        self.owner_pw = self.generate_password(16)
        self.user_pw = self.generate_password(16)
        self.sql = list()
        self.generate_sql()

    def generate_password(self, l):
        """
        Generate a password of l length
        :param l: integer, length of password
        :return: p a string of random characters of lenth l
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
        List of tuples (stage, sql command, back out sql command)
        """
        self.sql.append(('verify', 'SELECT 1 FROM pg_database WHERE datname=\'{}\';'.format(self.dbName), None))
        self.sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}\';'.format(self.owner), None))
        self.sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}\';'.format(self.user), None))
        self.sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}_reader\';'.format(self.dbName), None))
        self.sql.append(('verify', 'SELECT 1 FROM pg_roles WHERE rolname=\'{}_writer\';'.format(self.dbName), None))

        self.sql.append(('create',
                         'CREATE USER {} WITH LOGIN CREATEDB PASSWORD \'{}\';'.format(self.owner, self.owner_pw),
                         'drop user {};'.format(self.owner)))
        self.sql.append(('create',
                         'CREATE USER {} WITH LOGIN PASSWORD \'{}\';'.format(self.user, self.user_pw),
                         'drop user {};'.format(self.user)))
        self.sql.append(('create',
                         'CREATE ROLE {}_reader;'.format(self.dbName),
                         'drop role {}_reader;'.format(self.dbName)))
        self.sql.append(('create',
                         'CREATE ROLE {}_writer;'.format(self.dbName),
                         'drop role {}_writer;'.format(self.dbName)))
        self.sql.append(('create',
                         'GRANT {} to {};'.format(self.owner, self.dbAdmin),
                         'revoke {} from {};'.format(self.owner, self.dbAdmin)))
        self.sql.append(('create',
                         'CREATE DATABASE {} WITH OWNER = {};'.format(self.dbName, self.owner),
                         'drop database {};'.format(self.dbname)))

        self.sql.append(('grant',
                         '\connect {}'.format(self.dbName),
                         None))
        self.sql.append(('grant',
                         'GRANT {}_reader to {};'.format(self.dbName, self.user),
                         'revoke {}_reader from {};'.format(self.dbName, self.user)))
        self.sql.append(('grant',
                         'GRANT {}_writer to {};'.format(self.dbName, self.user),
                         'revoke {}_writer from {};'.format(self.dbName, self.user)))
        self.sql.append(('grant',
                         'grant select on all tables in schema public to {}_reader;'.format(self.dbName),
                         'revoke select on all tables in schema public from {}_reader;'.format(self.dbName)))
        self.sql.append(('grant',
                         'grant select, insert, update, delete on all tables in schema public to {}_writer;'.format(self.dbName),
                         'revoke select, insert, update, delete on all tables in schema public from {}_writer;'.format(self.dbName)))

        self.sql.append(('doc', '-- ----------> Send to requester', None))
        self.sql.append(('doc', 'server: {}'.format(self.fqdn), None))
        self.sql.append(('doc', 'database: {}'.format(self.dbName), None))
        self.sql.append(('doc', 'owner: {}'.format(self.owner), None))
        self.sql.append(('doc', 'owner password: {}'.format(self.owner_pw), None))
        self.sql.append(('doc', 'user: {}'.format(self.user), None))
        self.sql.append(('doc', 'user password: {}'.format(self.user_pw), None))

    def db_connect(self, db):
        try:
            self.con = psycopg2.connect(dbname=db, host=self.fqdn, user=self.dbAdmin, password=self.dbAdminPW)
            self.con.autocommit = True
            self.cur = conn.cursor()
            return True

        except psycopg2.Error as err:
            """
            Connection failed , bail.
            """
            flash(err)
            return False

    def db_disconnect(self):
        self.cur.close()
        self.con.close()
