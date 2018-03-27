# -*- coding: utf-8 -*-

import json

with open("/etc/sql2neo4j.conf", "r") as f:
    config = json.load(f)
    DB_NAME = config['DATABASE']['database']
    DB_DRIVER = config['DATABASE'].get('driver', 'mysql').lower()
    TABLE_GRAPH_MAPPING = {}
    for table in config['DATABASE']['tables']:
        table_name = table.get('name', '')
        if not table_name:
            continue

        graph_name = table.get('graph_name').encode('utf8') or table_name.capitalize().encode('utf8')
        TABLE_GRAPH_MAPPING[table_name] = graph_name
