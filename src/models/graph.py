# -*- coding: utf-8 -*-

from py2neo.ogm import GraphObject, Property

from libraries.config import DB_DRIVER
from libraries.dbs.graph import MyGraph
from libraries.utils import retry


class CKGraphObject(GraphObject):
    """
    Create constrain key for graph object if it's not created yet 
    To keep the same schema as the RDBMS tables
    """
    _create_constraint_key = False

    @classmethod
    @retry(3)
    def create_uniqueness_constraint(cls):
        label = cls.__primarylabel__
        property_key = cls.__primarykey__
        if property_key != '__id__' and not cls._create_constraint_key:
            _graph = MyGraph()
            if not _graph.schema.get_uniqueness_constraints(label):
                _graph.schema.create_uniqueness_constraint(label, property_key)
                cls._create_constraint_key = True

        return True

    @classmethod
    def create_index(cls):
        # TODO: create index for label based on the related table
        pass


class FieldProperty(Property):
    """
    Convert python data types which have been converted from the rows of mysql/sqlserver in the db driver to neo4j supported data type
    class Property(object):
        def __init__(self, key=None):
            self.key = key
    
        def __get__(self, instance, owner):
            return instance.__ogm__.node[self.key]
    
        def __set__(self, instance, value):
            instance.__ogm__.node[self.key] = value
    """
    def __init__(self, value_type, key=None):
        super(FieldProperty, self).__init__(key)
        self.type = "_{}2neo".format(value_type)

    def __get__(self, instance, owner):
        return instance.__ogm__.node[self.key]

    def __set__(self, instance, value):
        if value is None or value == "":
            instance.__ogm__.node[self.key] = None
        else:
            if hasattr(self, self.type):
                instance.__ogm__.node[self.key] = getattr(self, self.type)(value)
            else:
                instance.__ogm__.node[self.key] = value.encode('utf-8')

    @staticmethod
    def _tinyint2neo(value):
        return bool(value) if DB_DRIVER == "mysql" else int(value)

    @staticmethod
    def _int2neo(value):
        return int(value)

    _smallint2neo = _int2neo
    _mediumint2neo = _int2neo
    _bigint2neo = _int2neo

    @staticmethod
    def _set2neo(value):
        # List properties must be homogeneous in neo4j, so just change all value to string type
        # python MySQLdb driver already converted sql set to python set
        if isinstance(value, set):
            return list(value)

        # pymssql does not convert sql set to python set, manually convert sql set to python list here
        return [i for i in value.split(',') if i]

    @staticmethod
    def _datetime2neo(value):
        return value.isoformat(' ')

    _timestamp2neo = _datetime2neo
    _datetime22neo = _datetime2neo
    _smalldatetime2neo = _datetime2neo
    _time2neo = _datetime2neo
    _date2neo = _datetime2neo

    @staticmethod
    def _decimal2neo(value):
        return str(value)

    _numeric2neo = _decimal2neo
    _money2neo = _decimal2neo
    _smallmoney2neo = _decimal2neo

