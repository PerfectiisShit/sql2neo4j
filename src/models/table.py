# -*- coding: utf-8 -*-
import re

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
        self.logger = get_logger()

        self.parse_table_schema()

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
        # self.get_primary_key()
        self.get_foreign_keys()
        self.get_all_columns()
        self.get_indexes()

    # def get_primary_key(self):
    #     select_fields = ['column_name']
    #     table = "information_schema.key_column_usage"
    #     condition = {'table_schema': self._db, 'table_name': self._table, 'constraint_name': "primary"}
    #     result = self.select(table=table, fields=select_fields, condition=condition, select_one=True)
    #     if result is False:
    #         raise SQLDBError("Couldn't query information_schema table, make sure you have correct access to the db")
    #
    #     self._primary_key = result['column_name']

    # def get_foreign_keys(self):
    #     sql = """SELECT information_schema.key_column_usage.column_name AS column_name,
    #                information_schema.key_column_usage.referenced_table_name AS referenced_table_name,
    #                information_schema.key_column_usage.referenced_column_name AS referenced_column_name
    #              FROM information_schema.key_column_usage
    #              LEFT JOIN information_schema.table_constraints
    #                ON information_schema.key_column_usage.table_schema = information_schema.table_constraints.table_schema
    #                  AND information_schema.key_column_usage.table_name = information_schema.table_constraints.table_name
    #                  AND information_schema.key_column_usage.constraint_name = information_schema.table_constraints.constraint_name
    #              WHERE information_schema.key_column_usage.table_schema = '%s'
    #                AND information_schema.key_column_usage.table_name = '%s'
    #                AND information_schema.table_constraints.constraint_type = 'foreign key'
    #     """ % (self._db, self._table)
    #     self._foreign_keys = self.query(sql)
    #     if self._foreign_keys is False:
    #         raise SQLDBError("Couldn't query information_schema table, make sure you have correct access to the db")

    def get_foreign_keys(self):
        sql = "show create table %s" % self._table
        result = self.query(sql, select_one=True)
        if result:
            _result = result['Create Table'].splitlines()
            keys_info = [i for i in _result if 'FOREIGN KEY' in i]
            map(self.filter_foreign_keys, keys_info)
        else:
            raise SQLDBError("Couldn't query table creation information, make sure you have correct access to the db")

    def filter_foreign_keys(self, key_info):
        regex = re.compile(r'.*FOREIGN KEY \(`(.*)`\) REFERENCES `(.*)` \(`(.*)`\)')
        foreign_key = regex.match(key_info)
        if foreign_key:
            key = dict()
            key['column_name'] = foreign_key.group(1).encode('utf-8')
            key['referenced_table_name'] = foreign_key.group(2).encode('utf-8')
            key['referenced_column_name'] = foreign_key.group(3).encode('utf-8')
            self._foreign_keys.append(key)

    # def get_all_columns(self):
    #     select_fields = ['column_name', 'data_type']
    #     table = 'information_schema.columns'
    #     condition = {'table_schema': self._db, 'table_name': self._table}
    #     self._all_columns = self.select(table=table, fields=select_fields, condition=condition)
    #     if self._all_columns is False:
    #         raise SQLDBError("Couldn't query information_schema table, make sure you have correct access to the db")
    def get_all_columns(self):
        columns_info = self.query("show columns from %s" % self._table)
        if columns_info:
            for column in columns_info:
                self._all_columns.append({
                    'column_name': column['Field'].encode('utf-8'),
                    'data_type': column['Type'].split('(')[0].encode('utf-8')
                })

                if column['Key'] == 'PRI':
                    self._primary_key = column['Field'].encode('utf-8')
        else:
            SQLDBError("Couldn't query table columns information, make sure you have correct access to the db")

    def get_indexes(self):
        # TODO: get indexes of the table
        pass

    @retry(3)
    def query_rows(self):
        select_fields = [column['column_name'] for column in self.all_columns]
        limit = (self.query_start_point, self.query_length)
        result = self.select(table=self._table, fields=select_fields, order_by=self.primary_key, limit=limit)
        if result is False:
            self.logger.error("Failed to query table '{}' at query point: {}"
                              .format(self._table, self.query_start_point), "TableModel")
            raise SQLDBError("Got unexpected result while querying DATABASE!")

        return result
