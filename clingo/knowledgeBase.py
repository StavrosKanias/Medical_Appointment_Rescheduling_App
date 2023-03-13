from clorm import FactBase, Predicate, IntegerField, StringField
from customPredicates import DateField, TimeField
import sys
from datetime import datetime, timedelta
sys.path.append('./db')  # nopep8
from dataModel import DataModel


class KnowledgeBase():
    def __init__(self, name, schema, dbInfo=None, dbConditions=None, data=None):
        self.name = name
        self.schema = schema
        self.kb = FactBase()
        self.TYPE2FIELD = {'integer': IntegerField,
                           'text': StringField, 'date': DateField, 'time': TimeField}
        self.literals = self.createLiterals(schema)
        if dbInfo:
            self.bindToDb(dbInfo)
            if dbConditions != None:
                self.conditions = dbConditions
            else:
                self.conditions = {}
            self.db2kb()

        elif data:
            self.data2kb(data)

    def createLiterals(self, schema):
        literals = {}
        for e in schema:
            literals[e] = {}
            attributes = schema[e].copy()
            primary = self.createPrimaryLiteral(e, attributes, literals)
            for a in attributes:
                if attributes[a][0] == 'boolean':
                    content = primary.copy()
                    literalName = self.getLiteralName(a)
                    literal = type(literalName,
                                   (Predicate, ), content)
                    literals[a] = literal
                else:
                    field = self.TYPE2FIELD[attributes[a][0]]
                    content = primary.copy()
                    content[a.lower()] = field
                    literalName = self.getLiteralName(a)
                    literal = type(literalName,
                                   (Predicate, ), content)
                    literals[a] = literal
        return literals

    def isPrimary(self, entity, attribute):
        l = self.schema[entity][attribute]
        if len(l) > 1 and l[1] == 'primary':
            return True
        else:
            return False

    # Creates the primary fields taking into account complex primary keys
    def createPrimaryLiteral(self, entity, attributes, literals):
        primary = {}
        for a in list(attributes):
            if self.isPrimary(entity, a):
                p = attributes.pop(a)
                primaryField = self.TYPE2FIELD[p[0]]
                primary[a.lower()] = primaryField
        content = primary.copy()
        literalName = self.getLiteralName(entity)
        literal = type(literalName, (Predicate, ), content)
        literals[entity] = literal
        for p in list(primary):
            primary[entity.lower() + p[0].upper() + p[1:]] = primary.pop(p)
        return primary

    def getPrimaryData(self, entity, attributes, data):
        primaryData = []
        for a in list(attributes):
            if self.isPrimary(entity, a):
                d = data[attributes.index(a)]
                primaryData.append(d)
        return primaryData

    def getLiteralName(self, schemaName):
        literal_name_parts = schemaName.split('_')
        literal_name = ''
        for w in literal_name_parts:
            literal_name += w[0].upper() + w[1:].lower()
            if literal_name_parts.index(w) < len(literal_name_parts) - 1:
                literal_name += '_'
        return literal_name

    def bindToDb(self, dbInfo):
        self.db = DataModel(dbInfo[0], dbInfo[1], dbInfo[2])
        dbEntities = self.db.getTables()
        for s in self.schema:
            if s not in dbEntities:
                raise Exception(
                    f"Literal {s} doesn't exist in the given database.")

    def getConditions(self, entity):
        conditions = {}
        for attribute in self.conditions[entity]:
            for condition in self.conditions[entity][attribute]:
                conditions[attribute] = []
                if condition[1] in ['+', '-']:
                    if self.schema[entity][attribute][0] == 'date':
                        conditions[attribute].append(condition[0] + (datetime.today() +
                                                                     timedelta(days=int(condition[1:]))).strftime("%Y-%m-%d"))
                    elif self.schema[entity][attribute][0] == 'time':
                        conditions[attribute].append(condition[0] + (datetime.now() +
                                                                     timedelta(hours=int(condition[1:]))).strftime("%H:%M:%S"))
                else:
                    conditions[attribute].append(condition)
        return conditions

    # Translate db data to clingo literals
    def db2kb(self):
        # first clear the kb TODO
        for e in self.schema:
            attributes = list(self.schema[e])
            if e in self.conditions:
                conditions = self.getConditions(e)
                data = self.db.select(e, attributes, conditions)
            else:
                data = self.db.select(e, attributes)
            self.data2kb(e, attributes, data)

    def data2kb(self, entity, attributes, data):
        for a in list(attributes):
            if self.isPrimary(entity, a):
                l = self.literals[entity]
            else:
                l = self.literals[a]
            for d in data:
                pv = self.getPrimaryData(entity, attributes, d)
                v = pv.copy()
                if not self.isPrimary(entity, a) and not self.schema[entity][a][0] == 'boolean':
                    v.append(d[attributes.index(a)])
                self.kb.add(l(*v))

    def insert(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass

    def toFile(self, path, format='lp'):
        filename = path + self.name.lower() + '.' + format
        f = open(filename, "w")
        content = FactBase.asp_str(self.kb)
        f.write(content)
        f.close()

    def __repr__(self):
        return "Test()"

    def __str__(self):
        return "member of Test"
