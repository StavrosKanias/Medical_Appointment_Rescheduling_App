from clorm import monkey
monkey.patch()  # nopep8 # must call this before importing clingo
from clorm import FactBase, Predicate, IntegerField, StringField
from clingo import Control
from customPredicates import DateField, TimeField
from datetime import datetime, timedelta
import sys
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
            for entity in data:
                attributes = list(self.schema[entity])
                d = data[entity]
                self.data2kb(entity, attributes, d)

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
                        if conditions[attribute][-1][0] == '>' and conditions[attribute][-1][1:] > '17:00:00':
                            conditions[attribute][-1] = '>09:00:00'
                            d = []
                            for t in conditions['DATE'][-1][1:].split('-'):
                                d.append(int(t))
                            conditions['DATE'][-1] = conditions['DATE'][-1][0] + (
                                datetime(*d) + timedelta(days=1)).strftime("%Y-%m-%d")
                        elif conditions[attribute][-1][0] == '<' and conditions[attribute][-1][1:] < '09:00:00':
                            conditions[attribute][-1] = '<17:00:00'
                            d = []
                            for t in conditions['DATE'][-1][1:].split('-'):
                                d.append(int(t))
                            conditions['DATE'][-1] = conditions['DATE'][-1][0] + (
                                datetime(*d) + timedelta(days=-1)).strftime("%Y-%m-%d")
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
        print(entity, data)
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
                print(l, v)
                self.kb.add(l(*v))

    # Select from kb (More code needed for creating a condition)
    def select(self, entity, on=None, conditions=None, order=None, asc=True, group=None, attributes=None):
        l = []
        if type(entity) == list:
            for e in entity:
                l.append(self.literals[e.upper()])
        else:
            l = [self.literals[entity.upper()]]
        query = self.kb.query(*l)
        if type(entity) == list:
            if on:
                query = self.kb.query(*entity).join(on)
            else:
                raise Exception(
                    f"Attribute to join on for entities {entity} is not specified")
        if conditions:
            query = query.where(conditions)
        if order:
            query = query.order_by(order)
        if group:
            query = query.group_by(group)
        if attributes:
            query = query.select(attributes)
        return list(query.all())

    # Insert to kb and db
    def insert(self):
        pass

    # Update to kb and db
    def update(self):
        pass

    # Delete from kb and db
    def delete(self):
        pass

    def toFile(self, path, format='lp'):
        filename = path + self.name.lower() + '.' + format
        f = open(filename, "w")
        content = FactBase.asp_str(self.kb)
        f.write(content)
        f.close()

    def run(self, asp):

        # Create a Control object that will unify models against the appropriate
        # predicates. Then load the asp file that encodes the problem domain.
        ctrl = Control(unifier=list(self.literals.values()))
        ctrl.load(asp)

        # Add the instance data and ground the ASP program
        ctrl.add_facts(self.kb)
        ctrl.ground([("base", [])])

        # Generate a solution - use a call back that saves the solution
        solution = None

        def on_model(model):
            nonlocal solution
            solution = model.facts(atoms=True)

        ctrl.solve(on_model=on_model)
        if not solution:
            raise ValueError("No solution found")
        else:
            return solution

    def __repr__(self):
        return FactBase.asp_str(self.kb)

    def __str__(self):
        return FactBase.asp_str(self.kb)
