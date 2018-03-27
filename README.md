# sql2neo4j
Import data from **microsoft sqlserver** OR **mysql** to neo4j

---
# Change the configuration file according to your environment
<pre>
{
    "LOG_PATH": "/var/log/sql2neo4j/log",
    "NEO4J": {
        "protocol": "bolt",
        "host": "localhost",
        "port": 7687,
        "user": "neo4j",
        "password": "neo4j",
        "secure": false
    },
    "DATABASE": {
        "driver": "mysql",
        "host": "localhost",
        "port": 3306,
        "user": "test",
        "password": "test",
        "database": "test",
        "query_length": 1000,
        "tables": [
           {
               "name": "emails",
               "query_start_point": 0,
               "graph_name": "EMAILS"
           },
           {
               "name": "tickets",
               "query_start_point": 0,
               "graph_name": "TICKETS"
           },
           {
               "name": "meetings",
               "query_start_point": 0,
               "graph_name": "MEETINGS"
           },
           {
               "name": "tasks",
               "query_start_point": 0,
               "graph_name": "TASKS"
           },
           {
               "name": "logs",
               "query_start_point": 0,
               "graph_name": "LOGS"
           }
        ]
    }
}
</pre>

# Create Symbolic link for the configuration file and create the log folder
<pre>
ln -s PATH_TO_CONFIG /etc/sql2neo4j.conf
mkdir /var/log/sql2neo4j
</pre>

# Run the script
<pre>
python sql2neo4j.py
</pre>

# Tips
* You can either define your graph label names in the configuration file or change them in neo4j after the transfer finished 
* By default, we use "Table1ToTable2" as the relationship name for the two related tables, you can change it in neo4j as well after the transfer finished 
* In case there is some unexpected issue happened during exporting from RDBMS or importing to Neo4j, you can find the row failed position and skip those rows which have been imported to neo4j successfully by setting the "query_start_point" in the configuration file
