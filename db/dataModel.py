import csv
import time
import os
import psycopg2


class DataModel():
    def __init__(self, postgresKey, dbName, dbKey, schema=None):
        self.dbName = dbName
        self.dbKey = dbKey
        self.schema = schema
        connection = None
        try:
            connection = psycopg2.connect(
                dbname='postgres',
                password=postgresKey,
            )
        except:
            print('Connection refused.')

        if connection is not None:
            connection.autocommit = True
            cur = connection.cursor()
            cur.execute("SELECT datname FROM pg_database;")
            list_database = cur.fetchall()

            if (dbName,) in list_database:
                print("'{}' Database exist".format(dbName))
                self.connect()
                if (self.isEmpty() and schema is not None):
                    self.createTables()
            else:
                print("'{}' Database does not exist.".format(dbName))
            connection.close()

    def connect(self):
        try:
            print("Connecting to database...")
            # connect to db
            self.con = psycopg2.connect(
                dbname=self.dbName,
                password=self.dbKey,
            )
            self.cur = self.con.cursor()
            print("Successfuly connected to the database", self.dbName)
        except:
            print("Connection failed")

    def close(self):
        try:
            self.con.commit()
            self.con.close()
        except:
            print("Exit failed")

    def isEmpty(self):
        query = "SELECT count(*) FROM pg_catalog.pg_tables where schemaname not in ('information_schema', 'pg_catalog')"
        self.cur.execute(query)
        rows = self.cur.fetchall()
        if (rows[0][0]):
            return False
        else:
            print('The database has no tables')
            return True

    def create(self, tableName, tableDict):
        try:
            primary_keys = []
            foreign_keys = []
            uniques = []
            query = f"CREATE TABLE IF NOT EXISTS {tableName} (\n"
            # Attributes with types
            for a in tableDict.keys():
                query += f"{a} {tableDict[a][0]},\n"
                if tableDict[a][1]:
                    primary_keys.append(a)
                if len(tableDict[a]) > 3:
                    foreign_keys.append(a)
                elif len(tableDict[a]) == 3:
                    uniques.append(a)
            # Primary key(s)
            if len(primary_keys) == 0:
                print(f'No primary key found for table {tableName}')
            else:
                query += 'PRIMARY KEY ('
                for p in primary_keys:
                    query += p
                    if (primary_keys.index(p) < len(primary_keys) - 1):
                        query += ','
                if len(foreign_keys) == 0 and len(uniques) == 0:
                    query += ")\n);"
                else:
                    query += "),\n"
            # Unique constraints
            if len(uniques) != 0:
                for u in uniques:
                    query += f'CONSTRAINT KEEP_UNIQUE UNIQUE ({u})'

                    if (uniques.index(u) < len(uniques) - 1):
                        query += ",\n"
                if len(foreign_keys) == 0:
                    query += "\n);\n"
                else:
                    query += ",\n"
            # Foreign key constraints
            if len(foreign_keys) != 0:
                for fk in foreign_keys:
                    query += f'CONSTRAINT INFORM_{tableDict[fk][2].upper()} FOREIGN KEY({fk}) REFERENCES {tableDict[fk][2]}({tableDict[fk][3]}) ON UPDATE CASCADE ON DELETE CASCADE'

                    if (foreign_keys.index(fk) < len(foreign_keys) - 1):
                        query += ",\n"

                query += "\n);\n"
            return query

        except:
            print(f"Failed to create table {tableName}")

    def createTables(self):
        print("Creating the database tables...")
        for table in self.schema.keys():
            try:
                query = self.create(table, self.schema[table])
                self.cur.execute(query)
                self.con.commit()
            except psycopg2.Error as e:
                print(
                    f"Failed to create table {table} of database with schema {self.schema} because {e}")
                return
        print("Table creation finished")

    def loadTestData(self):
        print("Loading test data...")
        for table in self.schema.keys():
            try:
                csvFile = '\\' + table + '.csv'
                with open(os.path.dirname(__file__) + '\\data' + csvFile, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f, delimiter=",", quotechar='"')
                    for row in reader:
                        self.insert(table, row)
                print(f"Successfuly loaded test data for table {table}")
            except:
                print(f"Failed to load test data for table {table}")
                return

    def getTables(self):
        query = "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()"
        tables = self.executeSQL(query, fetch=True)
        for i in range(len(tables)):
            tables[i] = tables[i][0]
        return tables

    def getAttributes(self, table):
        for t in self.getTables():
            if table == t.upper():
                query = f"""SELECT attname AS col
                            FROM   pg_attribute
                            WHERE  attrelid = '{t}'::regclass
                            AND    attnum > 0
                            AND    NOT attisdropped
                            ORDER  BY attnum;"""
                attributes = self.executeSQL(query, fetch=True)
                for i in range(len(attributes)):
                    attributes[i] = attributes[i][0].upper()
                return attributes
        else:
            print(f'Table {table} not in database schema')
            return False

    def dropTables(self):
        query = """ DO $$ DECLARE
                r RECORD;
                BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                EXECUTE 'DROP TABLE ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
                END $$;"""
        self.cur.execute(query)
        self.con.commit()

    def dropData(self):
        query = """ DO $$ DECLARE
                r RECORD;
                BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                EXECUTE 'TRUNCATE ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
                END $$;"""
        self.cur.execute(query)
        self.con.commit()

    def executeSQL(self, strQuery, values=None, show=False, txtFile=None, fetch=False):
        try:
            if txtFile == None:
                query = strQuery
            else:
                with open(txtFile, "r", encoding='utf8') as txt:
                    query = txt.read()

            for subquery in query.split(";"):
                if subquery.strip():
                    t1 = time.perf_counter()
                    try:
                        if values == None:
                            self.cur.execute(subquery)
                        else:
                            self.cur.execute(subquery, values)
                    except psycopg2.Error as error:
                        print(
                            f"Failed to execute SQL querie \n {query}", error)
                    sql_time = time.perf_counter() - t1
                    if show:
                        print(
                            f'Executing querie {subquery}... finished in {sql_time:.5f} sec')

            self.con.commit()
            if (fetch):
                result = []
                data = self.cur.fetchall()
                if len(data):
                    for row in data:
                        element = []
                        for item in row:
                            element.append(item)
                        result.append(element)
                        if show:
                            print(element)
                    return result

        except psycopg2.Error as error:
            print(f"Failed to execute SQL querie \n {query}", error)
            return False

    def values(self, val):
        try:
            vlist = []
            for c in val.values():
                # For condition values
                if type(c) == list:
                    for v in c:
                        vlist.append(v[1])
                # For data values
                else:
                    vlist.append(c)
            return vlist
        except:
            print(
                f"Failed to create a list for the values\n {val}")
            return False

    def conditions(self, cond, sep):
        try:
            condstr = ''
            for c in cond:
                if type(cond[c]) is list:
                    condlst = cond[c]
                    for condition in condlst:
                        condstr += f"{c} {condition[0]} %s"
                        if condlst.index(condition) < len(condlst) - 1:
                            condstr += sep
                else:
                    condstr += f"{c} = %s"

                if list(cond.keys()).index(c) < len(list(cond.keys())) - 1:
                    condstr += sep

            return condstr

        except:
            print(
                f"Failed to create a unified string for the conditions\n {cond}")
            return False

    def select(self, table, attributes=None, conditions=None, joins=None):
        try:
            query = f"\nSELECT "
            if attributes:
                for a in attributes:
                    if joins:
                        query += f'{table}.{a}'
                    else:
                        query += a
                    if attributes.index(a) < len(attributes) - 1:
                        query += ', '
                    else:
                        query += '\n'
            else:
                query += '*\n'
            query += f"FROM {table}\n"

            if joins:
                jind = {}
                for j in joins:
                    if j[2] in jind:
                        jind[j[2]].append(joins.index(j))
                    else:
                        jind[j[2]] = [joins.index(j)]
                for j in joins:
                    sEntity = j[0]
                    sAttribute = j[1]
                    tEntity = j[2]
                    tAttribute = j[3]
                    query += f"JOIN {tEntity} "
                    if len(jind[tEntity]) > 1 and joins.index(j) > jind[tEntity][0]:
                        tEntity = tEntity + \
                            str(jind[tEntity].index(joins.index(j)))
                    query += f"AS {tEntity} ON {tEntity}.{tAttribute} = {sEntity}.{sAttribute}\n"

            if conditions:
                condstr = self.conditions(conditions, ' and ')
                query += f"""WHERE({condstr}); \n"""
                values = self.values(conditions)
                data = self.executeSQL(
                    query, values=values, fetch=True)
            else:
                data = self.executeSQL(query, fetch=True)
            return data

        except:
            print(
                f"Failed to select attributes {attributes} from {table} using conditions {conditions}")
            return False

    def insert(self, table, val):
        try:
            values = self.values(val)
            strQuery = f"""INSERT INTO {table}({",".join(val.keys())}) VALUES(%s{(len(val)-1) * ", %s"}); \n"""
            self.executeSQL(strQuery, values=values)
            return True
        except:
            print(f"Failed to insert values {values} to table {table}")
            return False

    def update(self, table, new, conditions=None, joins=None):
        try:
            if joins:
                using = []
                for j in joins:
                    for jt in j:
                        if jt[0] != table and jt[0] not in using:
                            using.append(jt[0])

            newstr = self.conditions(new, ', ')
            values = self.values(new)
            query = f"""UPDATE {table}\nSET {newstr}\n"""

            if joins:
                query += 'FROM '
                for u in using:
                    query += f"""{u}"""
                    if using.index(u) < len(using) - 1:
                        query += ', '
                    else:
                        query += '\n'

            if conditions:
                condstr = self.conditions(conditions, ' and ')
                if joins:
                    query += f"""WHERE("""
                    for j in joins:
                        query += f"""{j[0][0]}.{j[0][1]} = {j[1][0]}.{j[1][1]} AND """
                        if joins.index(j) == len(joins) - 1:
                            query += f"""{condstr}); \n"""
                else:
                    query += f"""WHERE({condstr}); \n"""
                condstr = self.conditions(conditions, ' and ')
                condval = self.values(conditions)
                values.extend(condval)
                self.executeSQL(query, values=values)
            else:
                self.executeSQL(query, values=values)
            return True
        except:
            print(
                f"Failed to update table {table} for conditions {condstr} and value(s) {newstr}")
            return False

    def delete(self, table, conditions=None, joins=None):
        try:
            if joins:
                using = []
                for j in joins:
                    for jt in j:
                        if jt[0] != table and jt[0] not in using:
                            using.append(jt[0])

            query = f"""\nDELETE FROM {table}\n"""

            if joins:
                query += 'USING '
                for u in using:
                    query += f"""{u}"""
                    if using.index(u) < len(using) - 1:
                        query += ', '
                    else:
                        query += '\n'

            if conditions:
                condstr = self.conditions(conditions, ' and ')
                if joins:
                    query += f"""WHERE("""
                    for j in joins:
                        query += f"""{j[0][0]}.{j[0][1]} = {j[1][0]}.{j[1][1]} AND """
                        if joins.index(j) == len(joins) - 1:
                            query += f"""{condstr}); \n"""
                else:
                    query += f"""WHERE({condstr}); \n"""
                values = self.values(conditions)
                self.executeSQL(query, values=values)
            else:
                self.executeSQL(query)
            return True
        except:
            print(
                f"Failed to delete the row from table {table}")
            return False
