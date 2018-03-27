# -*- coding: utf-8 -*-

from threading import Thread

from libraries.config import config
from table2graph import Table2Label


def main():
    db_tables = config['DATABASE']['tables']
    tables = []
    for table in db_tables:
        table_name = table.get("name", "")
        table_start_point = table.get("query_start_point", 0)
        query_length = config['DATABASE'].get('query_length', 1000)
        if table_name:
            table2label = Table2Label(table_name, table_start_point, query_length)
            table2label.create_graph_object()
            tables.append(table2label)

    threads = []
    for table in tables:
        t = Thread(target=table.generate_graph)
        threads.append(t)
        t.start()

    for thread in threads:
        thread.join()

    for t in tables:
        t.create_relationships()

if __name__ == "__main__":
    main()
