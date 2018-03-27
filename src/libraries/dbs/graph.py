# -*- coding: utf-8 -*-

from py2neo import Graph
from py2neo import Unauthorized
from libraries.config import config
from libraries.log import get_logger
from libraries.errors import ConfigError, GraphDBError


class MyGraph(object):
    """
    Usage::
    
        >>> from libraries.dbs.graph import MyGraph
        >>> graph_1 = MyGraph()
        >>> graph_2 = MyGraph(db="test1")
    For all dbms & schema related operations, create the connection to the database -> data;
    for multiple transaction operations, create different connections with different db names per transaction
    """
    __graphs = {}

    def __new__(cls, *args, **kwargs):
        db = kwargs.pop('db', 'data')
        try:
            _graph = cls.__graphs[db]
        except KeyError:
            _host = config['NEO4J'].get('host', 'localhost')
            _protocol = config['NEO4J'].get('protocol', 'bolt')
            _user = config['NEO4J'].get('user', 'neo4j')
            _password = config['NEO4J'].get('password', 'neo4j')
            _secure = config['NEO4J'].get('secure', False)

            if _protocol == 'http':
                _port = config['NEO4J'].get('PORT', 7474)
            elif _protocol == 'https':
                _port = config['NEO4J'].get('PORT', 7473)
            else:
                _port = config['NEO4J'].get('PORT', 7687)

            _graph = cls._connect(_protocol, db, _host, _port, _user, _password, _secure)
            cls.__graphs[db] = _graph
        return _graph

    @staticmethod
    def _connect(protocol, db, host, port, user, password, secure):
        logger = get_logger()
        try:
            if protocol == "bolt":
                _graph = Graph(database=db, bolt=True, host=host, bolt_port=port,
                               user=user, password=password, secure=secure)
            elif protocol == "http":
                _graph = Graph(database=db, bolt=False, host=host, http_port=port,
                               user=user, password=password, secure=secure)
            elif protocol == "https":
                _graph = Graph(database=db, bolt=False, host=host, https_port=port,
                               user=user, password=password, secure=secure)
            else:
                raise ConfigError("Couldn't connect to Graph db, unsupported protocol %s detected" % protocol)

            _graph.data("MATCH (a:` TEST CONNECTION `) RETURN a LIMIT 0")

        except Unauthorized:
            raise ConfigError("Failed to connect to Neo4j Database, incorrect credential!")

        except Exception as e:
            logger.error(str(e), "Neo4j", traceback=True)
            raise GraphDBError("Unexpected errors detected while connecting to Graph db!")

        return _graph
