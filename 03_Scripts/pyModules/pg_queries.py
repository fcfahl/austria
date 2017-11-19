from variables import *
from pyModules.logs import *
from variables import *

def execute_Query (query, table):

    print "\n____________________________________________________________\n"

    cursor = db_PostGIS['cursor']

    try:
        if query:
            info (query)
            cursor.execute(query)
    except:
        if query:
            error (query)
        print "error {0}".format(sys.exc_info()[0])

    if (table != "") or (query == 'VACUUM'):
        try:
            cursor.execute("VACUUM ANALYZE %s  ;" % table)
            print ("VACUUM ANALYZE %s  ;" % table)
        except:
            pass

def create_table (table):
    return "DROP TABLE IF EXISTS {0} CASCADE;\nCREATE TABLE {0}".format (table)

def select_data (select):
    return "SELECT {0}".format (select)

def from_table (from_):
    return "FROM {0}".format (from_)

def filter_table (where):
    return "WHERE {0}".format (where)

def update_column (table, column, value):
    sql = "UPDATE {0} a SET {1} = {2};".format (table, column, value)
    execute_Query (sql, table)

def create_sequence (sequence):
    sql = "DROP SEQUENCE IF EXISTS {0};\nCREATE SEQUENCE {0};".format (sequence)
    execute_Query (sql, "")

def restart_sequence (sequence):
    sql = "ALTER SEQUENCE {0} RESTART WITH 1;".format (sequence)
    execute_Query (sql, "")

def drop_table (table):
    sql = "DROP TABLE IF EXISTS {0} CASCADE;".format (table)
    execute_Query (sql, "")

def rename_table (old_table, new_table):
    sql = "ALTER TABLE {0} RENAME TO {1};".format (old_table, new_table)
    execute_Query (sql, new_table)

def drop_key (table):
    sql = "ALTER TABLE {0} DROP CONSTRAINT ({0}_pkey);".format (table)
    execute_Query (sql, "")

def add_Pkey (table, pkey):
    sql = "ALTER TABLE {0} ADD PRIMARY KEY ({1});".format (table, pkey)
    execute_Query (sql, "")

def add_Fkey (table, constraint, fkey, reference, column):
    sql = "ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY ({2}) REFERENCES {3} ({4});".format (table, constraint, fkey, reference, column)
    execute_Query (sql, "")

def add_column (table, column):
    # sql = "ALTER TABLE {0} ADD COLUMN IF NOT EXISTS {1};".format (table, column)
    sql = "ALTER TABLE {0} ADD COLUMN  {1};".format (table, column)
    execute_Query (sql, table)

def add_geometry (scheme, table, column, srid, type_, dimension):
    sql = "SELECT AddGeometryColumn ('{0}', '{1}','{2}', {3}, '{4}', {5});".format (scheme, table, column, srid, type_, dimension)
    execute_Query (sql, table)

def drop_column (table, column):
    sql = "ALTER TABLE {0} DROP COLUMN IF EXISTS {1};".format (table, column)
    execute_Query (sql, "")

def delete_records (table, where):
    sql = "DELETE FROM {0} WHERE {1};".format (table, where)
    execute_Query (sql, "")

def create_index (table, column):
    sql = "CREATE INDEX ON {0} ({1});".format (table, column)
    execute_Query (sql, table)

def pgr_topology (table, tolerance, id):
    sql =  "SELECT pgr_createTopology(\'{0}\', {1}, 'geom', '{2}');".format (table, tolerance, id)
    execute_Query (sql, table)

def pgr_nodeNetwork (table, tolerance, id):
    sql =  "SELECT pgr_nodeNetwork(\'{0}\', {1}, '{2}', 'geom');".format (table, tolerance, id)
    execute_Query (sql, table)

def pgr_analyzeGraph (table, tolerance, id):
    sql =  "SELECT pgr_analyzeGraph(\'{0}\', {1}, 'geom', '{2}');".format (table, tolerance, id)
    execute_Query (sql, "")

def sql_custom (table, sql):
    execute_Query (sql, table)

def sql_update_table (table, column, value, from_, where):

    sql = "UPDATE {table} a \nSET {column} = {value} \n{from_} \n{where};".format (
        table = table,
        column = column,
        value = value,
        from_ = from_table (from_),
        where = filter_table (where)
    )

    execute_Query (sql, table)

def sql_create_table (table, select, from_, where):

    if from_ == '':
        sql = "{create} AS\n{select};".format (
            create = create_table (table),
            select = select_data (select)
        )

    elif where == '':
        sql = "{create} AS\n{select}\n{from_};".format (
            create = create_table (table),
            select = select_data (select),
            from_ = from_table (from_)
        )

    else:
        sql = "{create} AS\n{select}\n{from_}\n{where};".format (
            create = create_table (table),
            select = select_data (select),
            from_ = from_table (from_),
            where = filter_table (where)
        )

    execute_Query (sql, table)

def sql_create_table_with (table, with_, where):

    if where == '':

        sql = "{create} AS\n{with_};".format (
            create = create_table (table),
            with_ = with_
        )
    else:
        sql = "{create} AS\n{with_}\n{where};".format (
            create = create_table (table),
            with_ = with_,
            where = where
        )

    execute_Query (sql, table)

def sql_create_SQL_function (name, columns, return_, sql):

    sql = """
        DROP FUNCTION IF EXISTS {name} ({columns});
        CREATE OR REPLACE FUNCTION {name} ({columns})
        RETURNS {return_} AS
        $$
            {sql}
        $$ LANGUAGE SQL;
    """.format (
        name = name,
        columns = columns,
        return_ = return_,
        sql = sql
    )

    execute_Query (sql, "")


def sql_create_PLPGSQL_function (name, columns, return_, declare, sql):

    sql = """
        DROP FUNCTION IF EXISTS {name} ({columns});
        CREATE OR REPLACE FUNCTION {name} ({columns})
        RETURNS {return_} AS
        $$
    	DECLARE
    		{declare}
    	BEGIN
            {sql}
    	END;
        $$ LANGUAGE PLPGSQL;
    """.format (
        name = name,
        columns = columns,
        return_ = return_,
        declare = declare,
        sql = sql
    )

    execute_Query (sql, "")
