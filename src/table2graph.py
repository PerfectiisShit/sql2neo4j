# -*- coding: utf-8 -*-

from libraries.config import TABLE_GRAPH_MAPPING, THREAD_POOL
from libraries.dbs.graph import MyGraph
from libraries.log import get_logger
from models.graph import CKGraphObject, FieldProperty
from models.table import TableModel
from multiprocessing.pool import ThreadPool


class Table2Label(object):
    def __init__(self, table, start_point, query_length):
        self._table = TableModel(table, start_point, query_length)
        self._graph = MyGraph(self.graph_label_name)
        self.graph_object = None
        self.related_graphs = []
        self.wb = None
        self.exported_rows = 0
        self.imported_graphs = 0

        self.logger = get_logger()
        self.log_type = "Table2Label"

    def __repr__(self):
        return "RDBMS table \"{}.{}\" to Graph label \"{}\"".format(
            self._table.db_name, self._table.table_name, self.graph_label_name)
        
    @property
    def graph_label_name(self):
        return self.lookup_graph_name(self._table.table_name)

    @staticmethod
    def lookup_graph_name(table_name):
        return TABLE_GRAPH_MAPPING.get(table_name) or table_name.capitalize().encode('utf8')

    def log(self, msg, level='info', traceback=False):
        try:
            getattr(self.logger, level)(msg, self.log_type, traceback)
        except AttributeError:
            pass

    def create_graph_object(self):
        graph_attrs = dict()
        if self._table.primary_key:
            graph_attrs['__primarykey__'] = self._table.primary_key
        
        for column in self._table.all_columns:
            graph_property = column.get('column_name', '')
            if graph_property:
                graph_attrs[graph_property] = FieldProperty(column.get('data_type', ''))

        self.graph_object = type(self.graph_label_name, (CKGraphObject,), graph_attrs)
        self.graph_object.create_uniqueness_constraint()
        self.graph_object.create_index()

    def create_relationships(self):
        _graph = MyGraph()
        for key in self._table.foreign_keys:
            graph_property = key['column_name']
            related_table = key['referenced_table_name']
            related_graph_name = self.lookup_graph_name(related_table)
            realted_property = key['referenced_column_name']
            relationship_name = related_graph_name + '_HAS_' + self.graph_label_name
            try:
                cursor = _graph.run(
                    "MATCH (a:{n}), (b:{m}) "
                    "WHERE a.{p} = b.{q} "
                    "MERGE (a)<-[:{r}]-(b)"
                    .format(
                        n=self.graph_label_name,
                        m=related_graph_name,
                        p=graph_property,
                        q=realted_property,
                        r=relationship_name
                    )
                )
                result = cursor.stats()['relationships_created']
                self.log("Succeed to create {} '{}' relationships".format(result, relationship_name))
            except Exception as e:
                self.log("Failed to create relationship {}".format(relationship_name), level="error")
                self.log(str(e), level="debug", traceback=True)
            finally:
                cursor.close()
    
    def create_indexes(self):
        # TODO: Create index per the indexes in DATABASE table
        pass

    def row2graph(self, row):
        self.logger.debug("Import data to {} :".format(self.graph_label_name) + str(row), self.log_type)
        try:
            gobject = self.graph_object()
            for k, v in row.iteritems():
                setattr(gobject, k, v)

            self._graph.push(gobject)
            self.imported_graphs += 1
        except Exception as e:
            self.log(msg="Failed to import data to graph db at importing point: " + str(self.imported_graphs),
                     level="error")
            self.log(msg=str(e), level="debug", traceback=True)

    # def batch_write(self, nodes):
    #     try:
    #         self.wb = WriteBatch(self._graph)
    #         for node in nodes:
    #             job = self.wb.create(node)
    #             self.wb.add_labels(job, self.graph_label_name)
    #
    #         self.wb.run()
    #         self.imported_graphs += len(nodes)
    #     except Exception as e:
    #         msg = "Neo4j import '{}' stopped at query point: {}".format(self.graph_label_name, self.imported_graphs)
    #         self.log(msg, level="error")
    #         self.log(str(e), level="debug", traceback=True)
    #         raise SQL2GraphError("Got unexpected error while importing data to neo4j!")

    # def row2graph(self, row):
    #     try:
    #         self.logger.debug("Import data to {} :".format(self.graph_label_name) + str(row), self.log_type)
    #         gobject = self.graph_object()
    #         for k, v in row.iteritems():
    #             setattr(gobject, k, v)
    #
    #         self._graph.push(gobject)
    #         self.imported_graphs += 1
    #
    #     except Exception as e:
    #         msg = "Neo4j import '{}' stopped at query point: {}".format(self.graph_label_name, self.imported_graphs)
    #         self.log(msg, level="error")
    #         self.log(str(e), level="debug", traceback=True)
    #         raise SQL2GraphError("Got unexpected error while importing data to neo4j!")

    def import2graph(self, rows):
        pool = ThreadPool(THREAD_POOL)
        results = pool.map(self.row2graph, rows)
        pool.close()
        pool.join()
        return results

    def generate_graph(self):
        while self._table.query_start_point >= 0:
            rows = self._table.query_rows()
            if rows:
                self.exported_rows += len(rows)
                self.import2graph(rows)
                if len(rows) == self._table.query_length:
                    self._table.query_start_point += self._table.query_length
                    continue

            self._table.query_start_point = -1

        self.log("Exported {} rows from table '{}'".format(self.exported_rows, self._table.table_name))
        self.log("Imported {} '{}' nodes to graph database".format(self.imported_graphs, self.graph_object.__name__))
