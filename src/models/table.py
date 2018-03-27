# -*- coding: utf-8 -*-

from libraries.utils import retry
from libraries.errors import SQLDBError
from libraries.log import get_logger
from libraries.config import config, DB_NAME, DB_DRIVER

if DB_DRIVER == "mysql":
    from libraries.dbs.mysql import SimpleMysql as BaseDB
elif DB_DRIVER == "sqlserver":
    from libraries.dbs.mssdb import SQLServer as BaseDB
else:
    raise TypeError("Unknown database server")


class DBModel(BaseDB):
    _db = DB_NAME

    def __init__(self):
        super(DBModel, self).__init__(host=config['DATABASE']['host'],
                                      port=config['DATABASE']['port'],
                                      database=config['DATABASE']['database'],
                                      user=config['DATABASE']['user'],
                                      password=config['DATABASE']['password'])

    @property
    def db_name(self):
        return self._db


class TableModel(DBModel):
    def __init__(self, table, start_point, query_length):
        super(TableModel, self).__init__()
        self._table = table
        self.query_start_point = int(start_point)
        self.query_length = int(query_length)
        self._primary_key = ""
        self._foreign_keys = []
        self._all_columns = []
        self._indexes = []

        self.parse_table_schema()

        self.logger = get_logger()

    def __repr__(self):
        return "\"{}\" table model of \"{}\" database in {}".format(self._table, self._db, DB_DRIVER)

    @property
    def table_name(self):
        return self._table

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def foreign_keys(self):
        return self._foreign_keys

    @property
    def all_columns(self):
        return self._all_columns

    @property
    def indexes(self):
        return self._indexes

    def parse_table_schema(self):
        self.get_primary_key()
        self.get_foreign_keys()
        self.get_all_columns()
        self.get_indexes()

    def get_primary_key(self):
        select_fields = ['column_name']
        table = "information_schema.key_column_usage"
        condition = {'table_schema': self._db, 'table_name': self._table, 'constraint_name': "primary"}
        result = self.select(table=table, fields=select_fields, condition=condition, select_one=True)
        if result is False:
            raise SQLDBError("Couldn't query information_schema table, make sure you have correct access to the db")

        self._primary_key = result['column_name']

    def get_foreign_keys(self):
        select_fields = ['column_name', 'referenced_table_name', 'referenced_column_name']
        table = "information_schema.key_column_usage"
        condition = {'table_schema': self._db, 'table_name': self._table, 'constraint_name': self._table+"_ibfk_1"}
        self._foreign_keys = self.select(table=table, fields=select_fields, condition=condition)
        if self._foreign_keys is False:
            raise SQLDBError("Couldn't query information_schema table, make sure you have correct access to the db")

    def get_all_columns(self):
        select_fields = ['column_name', 'data_type']
        table = 'information_schema.columns'
        condition = {'table_schema': self._db, 'table_name': self._table}
        self._all_columns = self.select(table=table, fields=select_fields, condition=condition)
        if self._all_columns is False:
            raise SQLDBError("Couldn't query information_schema table, make sure you have correct access to the db")

    def get_indexes(self):
        # TODO: get indexes of the table
        pass

    @retry(3)
    def query_rows(self):
        select_fields = []
        for column in self.all_columns:
            select_fields.append(column['column_name'])

        limit = (self.query_start_point, self.query_length)
        result = self.select(table=self._table, fields=select_fields, order_by=self.primary_key, limit=limit)
        if result is False:
            self.logger.error("Failed to query table '{}' at query point: {}"
                              .format(self._table, self.query_start_point), "Table2Label")
            raise SQLDBError("Got unexpected result while querying DATABASE!")

        return result
