# -*- coding: utf-8 -*-

try:
    import MySQLdb
    import MySQLdb.cursors
except ImportError:
    import pymysql as MySQLdb

from libraries.log import get_logger


class SimpleMysql(object):
    def __init__(self, host='localhost', port=3306, user='root', password='', database='', timeout=5):
        self._hostname = host
        self._port = port
        self._username = user
        self._password = password
        self._database = database
        self._timeout = timeout
        self._connection = None
        self._cursor = None
        self._error = ""
        self._last_id = 0
        self._table = None
        self.logger = get_logger()

    def __repr__(self):
        return "MySQL connection to database \"{}\" in {}".format(self._database, self._hostname)
    
    def _connect(self):
        try:
            self._connection = MySQLdb.connect(host=self._hostname,
                                               user=self._username,
                                               passwd=self._password,
                                               db=self._database,
                                               connect_timeout=self._timeout,
                                               charset='utf8',
                                               cursorclass=MySQLdb.cursors.DictCursor)
            self._cursor = self._connection.cursor()
            return True
        except Exception as e:
            self._error = str(e)
            self.logger.error(self._error, "MySQL")
            self.logger.debug(self._error, "MySQL", traceback=True)
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
            self.logger.error(msg, "MySQL")
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
                self.logger.debug(sql, "MySQL")
                return _fetch
            except Exception as e:
                self._error = str(e)
                msg = sql + self._error
                self.logger.error(msg, "MySQL")
                self.logger.debug(msg, "MySQL", traceback=True)
                return False
            finally:
                self._close()
    
    @classmethod
    def _escape(cls, content):
        return str(content).replace('\\', '\\\\').replace('\'', '\\\'')
    
    def escape(self, content):
        return self._escape(content)
    
    def select(self, table=None, condition=None, fields=None, order_by=None, limit=None, select_one=False):
        if fields:
            _field = '`%s`' % "`,`".join(fields)
        else:
            _field = "*"
        select_sql = "SELECT %s FROM %s WHERE 1=1" % (_field, table)
        if condition:
            _where = ""
            for k, v in condition.items():
                _where += " AND `%s` " % str(k)
                if isinstance(v, (list, tuple)):
                    if v[0] == "in":
                        _where += " in %s" % (self._escape(v[1]))
                    else:
                        _where += " %s '%s'" % (v[0], self._escape(v[1]))
                else:
                    _where += " = '%s'" % self._escape(v)
            select_sql += _where
        if order_by:
            select_sql += " ORDER BY %s " % order_by
        if limit:
            if isinstance(limit, (list, tuple)):
                select_sql += " LIMIT %d, %d" % (limit[0], limit[1])
            else:
                select_sql += " LIMIT %d" % limit
        return self.query(select_sql, select_one=select_one)
