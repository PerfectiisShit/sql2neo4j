# -*- coding: utf-8 -*-

import pymssql

from libraries.log import get_logger


class SQLServer(object):
    def __init__(self, host='', port='1433', user='',  password='', database='', timeout=5):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._timeout = timeout
        self._conn = None
        self._cursor = None
        self._last_id = 0
        self._error = ""

        self._logger = get_logger()

    def __repr__(self):
        return "SQLServer connection to database \"{}\" in {}".format(self._database, self._host)

    def _connect(self):
        try:
            self._conn = pymssql.connect(server=self._host,
                                         port=self._port,
                                         user=self._user,
                                         password=self._password,
                                         database=self._database,
                                         timeout=self._timeout)
            self._cursor = self._conn.cursor(as_dict=True)
            self._cursor.execute("""SELECT 1""")
            _ = self._cursor.fetchall()
            return True
        except Exception as e:
            self._error = str(e)
            self._logger.error(self._error, "SQLServer")
            self._logger.debug(self._error, "SQLServer", traceback=True)
            return False

    def _close(self):
        if self._cursor:
            try:
                self._cursor.close()
            except:
                pass
        self._cursor = None
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
        self._connection = None

    @property
    def last_id(self):
        return self._last_id

    def query(self, sql, select_one=False):
        if self._connect() is False:
            msg = sql + self._error
            self._logger.error(msg, "SQLServer")
            return False
        else:
            try:
                self._cursor.execute(sql)
                # self._connection.commit()
                if select_one:
                    _fetch = self._cursor.fetchone()
                else:
                    _fetch = self._cursor.fetchall()
                self._last_id = self._cursor.lastrowid
                self._error = ""
                self._logger.debug(sql, "SQLServer")
                return _fetch
            except Exception as e:
                self._error = str(e)
                msg = sql + self._error
                self._logger.error(msg, "SQLServer")
                self._logger.debug(msg, "SQLServer", traceback=True)
                return False
            finally:
                self._close()

    @classmethod
    def _escape(cls, content):
        return str(content).replace('\'', '\'\'')

    def escape(self, content):
        return self._escape(content)

    def select(self, table=None, condition=None, fields=None, order_by=None, limit=None, select_one=False):
        if fields:
            _field = '"%s"' % "\",\"".join(fields)
        else:
            _field = "*"
        select_sql = "SELECT %s FROM \"%s\" WHERE 1=1" % (_field, table)
        if condition:
            _where = ""
            for k, v in condition.items():
                _where += " AND \"%s\" " % str(k)
                if isinstance(v, (list, tuple)):
                    _where += " in ('%s')" % "','".join([self._escape(_v) for _v in v])
                else:
                    _where += " = '%s'" % self._escape(v)
            select_sql += _where
        if order_by:
            select_sql += " ORDER BY \"%s\" " % order_by
        if limit:
            if isinstance(limit, (list, tuple)):
                select_sql += " OFFSET %d ROWS FETCH NEXT %d ROWS ONLY " % (limit[0], limit[1])
            else:
                select_sql += " OFFSET %d ROWS" % limit
        return self.query(select_sql, select_one=select_one)
