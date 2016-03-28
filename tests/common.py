import psycopg2
import unittest

class PgDiffTestCase(unittest.TestCase):
    schema1 = 'public'
    schema2 = 'public'

    def get_random_hash(self, size=8):
        import os
        return os.urandom(size).encode('hex')

    def _connection_kwargs(self, **kwargs):
        from pypgdiff import settings
        default_kwargs = {
            'database'  : None,
            'user'      : settings.TEST_DB_USER,
            'password'  : settings.TEST_DB_PASS,
            'host'      : settings.TEST_DB_HOST,
        }
        default_kwargs.update(kwargs)
        return default_kwargs

    def get_connection(self, database=None, user='pypgdiff', password='pypgdiff', host='localhost'):
        assert database, 'Need a database name!'
        return psycopg2.connect(database=database, user=user, password=password, host=host)

    def _create_database(self, schema_name):
        '''create a random database'''
        # create the database
        name = 'test_pypgdiff_' + self.get_random_hash()
        conn = self.get_connection(**self._connection_kwargs(database='postgres'))
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.cursor().execute('CREATE DATABASE "%s"' % name)
        conn.close()
        # create the schema
        conn = self.get_connection(**self._connection_kwargs(database=name))
        conn.cursor().execute('CREATE SCHEMA "%s"' % schema_name)
        # return the db info
        return {
            'name'  : name,
            'conn'  : conn
        }

    def _destroy_database(self, db):
        '''destroy a database'''
        # close the connection
        if 'conn' in db:
            db['conn'].close()
            db['conn'] = None
        # drop the database
        conn = self.get_connection(**self._connection_kwargs(database='postgres'))
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        conn.cursor().execute('DROP DATABASE "%s"' % db['name'])
        conn.close()

    def setUp(self):
        '''create 2 database to diff'''
        self.schema1 = 's%s' % self.get_random_hash(4)
        self.schema2 = 's%s' % self.get_random_hash(4)
        self.databases  = [
            self._create_database(self.schema1),
            self._create_database(self.schema2)
        ]

    def tearDown(self):
        '''clean up all of the databases'''
        for db in self.databases:
            self._destroy_database(db)

    def _dbx(self, index):
        try:
            return self.databases[index]['conn']
        except (AttributeError, IndexError, KeyError):
            return None

    @property
    def db1(self):
        return self._dbx(0)

    @property
    def db2(self):
        return self._dbx(1)
