import csv
import time
import os
import psycopg2


class DataModel():
    def __init__(self, postgresKey, dbName, dbKey, schema):
        self.dbName = dbName
        self.dbKey = dbKey
        self.schema = schema
        connection = None
        try:
            connection = psycopg2.connect(
                user='postgres',
                host='localhost',
                password=postgresKey,
                port='5433'
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
                if(self.isEmpty()):
                    self.createTables()
            else:
                print("'{}' Database does not exist.".format(dbName))
            connection.close()

    def connect(self):
        try:
            print("Connecting to database...")
            # connect to db
            self.con = psycopg2.connect(
                host='localhost',
                database=self.dbName,
                user='postgres',
                password=self.dbKey,
                port=5433
            )
            self.cur = self.con.cursor()
            print("Successfuly connected to the database", self.dbName)
        except:
            print("Connection failed")

    def isEmpty(self):
        q = "SELECT count(*) FROM pg_catalog.pg_tables where schemaname not in ('information_schema', 'pg_catalog')"
        self.cur.execute(q)
        rows = self.cur.fetchall()
        if(rows[0][0]):
            return False
        else:
            print('The database has no tables')
            return True

    def createTables(self):

        print("Creating the database tables...")
        for table in self.schema.keys():
            try:
                q = self.create(table, self.schema[table])
                self.cur.execute(q)
                self.con.commit()
            except psycopg2.Error as e:
                print(
                    f"Failed to upload table {table} of database with schema {self.schema} because {e}")
                return
        print("Table creation finished")

    def loadTestData(self):
        for table in self.schema.keys():
            try:
                csvFile = '\\' + table + '.csv'
                with open(os.path.dirname(__file__) + csvFile, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f, delimiter=",", quotechar='"')
                    for row in reader:
                        self.insertRow(table, row)
            except:
                print(f"Failed to create table {table}")
                return

    def close(self):
        try:
            self.con.commit()
            self.con.close()
        except:
            print("Exit failed")

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
                    if values == None:
                        self.cur.execute(subquery)
                    else:
                        try:
                            self.cur.execute(subquery, values)
                        except psycopg2.Error as error:
                            print(
                                f"Failed to execute SQL querie \n {query}", error)
                    sql_time = time.perf_counter() - t1
                    if show:
                        print(
                            f'Executing querie {subquery[:50]}... finished in {sql_time:.5f} sec')

            self.con.commit()
            if(fetch):
                result = []
                if len(self.cur.fetchall()):
                    for row in self.cur.fetchall():
                        element = []
                        for item in row:
                            element.append(str(item))
                        result.append(element)
                        if show:
                            print(element)
                    return result

        except psycopg2.Error as error:
            # print(f"Failed to execute SQL querie \n {query}", error)
            return False

    def values(self, val, ins=0, table=None):
        try:
            vlist = []
            for f in val.keys():
                vlist.append(val[f])
            return vlist
        except:
            print(
                f"Failed to create a list for the values\n {val}")
            return False

    def conditions(self, cond, sep, table, upd=0):
        try:
            condstr = ''
            for f in cond.keys():
                c = cond[f]
                c = c.split('&')
                type = self.schema[table][f][0]

                if len(c) > 1:
                    if type == 'date' and not upd:
                        condstr += f'(julianday({f}) {c[0][0]} julianday(?) and julianday({f}) {c[1][0]} julianday(?))'
                    else:
                        condstr += f'({f} {c[0][0]} ? and {f} {c[1][0]} ?)'
                    cond[f] = f'{c[0][1:]} & {c[1][1:]}'

                else:
                    if type == 'date' and not upd:
                        condstr += f'(julianday({f}) {c[0][0]} julianday(?))'
                    else:
                        condstr += f"{f} {c[0][0]} ?"
                    cond[f] = cond[f][1:]

                if list(cond.keys()).index(f) < len(list(cond.keys())) - 1:
                    condstr += sep

            return condstr

        except:
            print(
                f"Failed to create a unified string for the conditions\n {cond}")
            return False

    def search(self, table, conditions):
        try:
            query = f"\nSELECT *\nFROM {table}\n"
            if conditions != None:
                condstr = self.conditions(conditions, ' and ', table=table)
                query += f"""WHERE({condstr}); \n"""
                values = self.values(conditions)
                filtered = self.executeSQL(query, values=values)
            else:
                filtered = self.executeSQL(query)

            return filtered

        except:
            print(
                f"Failed to filter {table} using conditions {conditions}")
            return False

    def insertRow(self, table, val):

        try:
            values = self.values(val, ins=1, table=table)
            strQuery = f"""INSERT INTO {table}({",".join(val.keys())}) VALUES(%s{(len(val)-1) * ", %s"}); \n"""
            self.executeSQL(strQuery, values=values)
            return True
        except:
            print(f"Failed to insert values {values} to table {table}")
            return False

    def deleteRow(self, table, conditions):
        try:
            condstr = self.conditions(conditions, ' and ', table=table)
            values = self.values(conditions)
            query = f"""DELETE FROM {table} WHERE({condstr}); \n"""
            self.executeSQL(query, values=values)
            return True
        except:
            print(
                f"Failed to delete the row from table {table}")
            return False

    def updateRow(self, table, conditions, new):
        try:
            condstr = self.conditions(conditions, ' and ', table=table)
            condval = self.values(conditions)
            newstr = self.conditions(new, ', ', table=table, upd=1)
            newval = self.values(new, table=table, ins=1)
            values = newval + condval
            query = f"UPDATE {table} SET {newstr} WHERE ({condstr});\n"
            self.executeSQL(query, values)
            return True
        except:
            print(
                f"Failed to update the rows that have {condstr} from table {table} with value(s) {newstr}")
            return False

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

                    query += f'CONSTRAINT INFORM_{tableDict[fk][2].upper()} FOREIGN KEY({fk}) REFERENCES {tableDict[fk][2]}({tableDict[fk][3]}) ON UPDATE CASCADE'
                    if tableDict[fk][2] == 'Partner':
                        query += ' ON DELETE CASCADE'
                    else:
                        query += ' ON DELETE SET NULL'

                    if (foreign_keys.index(fk) < len(foreign_keys) - 1):
                        query += ",\n"

                query += "\n);\n"
            return query

        except:
            print(f"Failed to create table {tableName}")

    def dropTables(self):
        q = """ DO $$ DECLARE
                r RECORD;
                BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                EXECUTE 'DROP TABLE ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
                END $$;"""
        self.cur.execute(q)
        self.con.commit()

    def dropData(self):
        q = """ DO $$ DECLARE
                r RECORD;
                BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                EXECUTE 'TRUNCATE ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
                END $$;"""
        self.cur.execute(q)
        self.con.commit()
