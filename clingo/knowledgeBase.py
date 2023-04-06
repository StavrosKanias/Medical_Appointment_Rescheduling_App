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
        self.predicates = self.createPredicates(schema)
        print(self.predicates)
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

    def createPredicates(self, schema):
        predicates = {}
        for e in schema:
            predicates[e] = {}
            attributes = schema[e].copy()
            primary = self.createPrimaryPredicate(e, attributes, predicates)
            for a in attributes:
                if attributes[a][0] == 'boolean':
                    content = primary.copy()
                    predicateName = self.getPredicateName(a)
                    predicate = type(predicateName,
                                     (Predicate, ), content)
                    predicates[a] = predicate
                else:
                    field = self.TYPE2FIELD[attributes[a][0]]
                    content = primary.copy()
                    content[a.lower()] = field
                    print(content)
                    predicateName = self.getPredicateName(a)
                    predicate = type(predicateName,
                                     (Predicate, ), content)
                    predicates[a] = predicate
        return predicates

    def isPrimary(self, entity, attribute):
        p = self.schema[entity][attribute]
        if len(p) > 1 and p[1] == 'primary':
            return True
        else:
            return False

    # Creates the primary fields taking into account complex primary keys
    def createPrimaryPredicate(self, entity, attributes, predicates):
        primary = {}
        for a in list(attributes):
            if self.isPrimary(entity, a):
                p = attributes.pop(a)
                primaryField = self.TYPE2FIELD[p[0]]
                primary[a.lower()] = primaryField
        content = primary.copy()
        predicateName = self.getPredicateName(entity)
        predicate = type(predicateName, (Predicate, ), content)
        predicates[entity] = predicate
        for p in list(primary):
            primary[entity.lower() + p[0].upper() + p[1:]] = primary.pop(p)
        return primary

    def getPrimaries(self, entity):
        primaries = []
        for a in self.schema[entity]:
            if self.isPrimary(entity, a):
                primaries.append(a)
        return primaries

    def getPrimaries(self, entity):
        primaries = []
        for a in list(self.schema[entity]):
            if self.isPrimary(entity, a):
                primaries.append(a)
        return primaries

    def getPrimaryData(self, entity, attributes, data):
        primaryData = []
        for a in list(attributes):
            if self.isPrimary(entity, a):
                d = data[attributes.index(a)]
                primaryData.append(d)
        return primaryData

    def getPredicateName(self, schemaName):
        predicate_name_parts = schemaName.split('_')
        predicate_name = ''
        for w in predicate_name_parts:
            predicate_name += w[0].upper() + w[1:].lower()
            if predicate_name_parts.index(w) < len(predicate_name_parts) - 1:
                predicate_name += '_'
        return predicate_name

    def bindToDb(self, dbInfo):
        self.db = DataModel(dbInfo[0], dbInfo[1], dbInfo[2])
        dbEntities = self.db.getTables()
        for s in self.schema:
            if s not in dbEntities:
                raise Exception(
                    f"The predicate {s} doesn't exist in the given database.")

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

    # Translate db data to clingo predicates
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
                p = self.predicates[entity]
            else:
                p = self.predicates[a]
            for d in data:
                pv = self.getPrimaryData(entity, attributes, d)
                v = pv.copy()
                if not self.isPrimary(entity, a) and not self.schema[entity][a][0] == 'boolean':
                    v.append(d[attributes.index(a)])
                self.kb.add(p(*v))

    def getMatchingPrimaries(self, entity, conditions):
        primaries = self.getPrimaries(entity)
        cattr = []
        cpred = []
        for c in conditions:
            cattr.append(c[0])
        for attribute in list(self.schema[entity]):
            if self.isPrimary(entity.upper(), attribute):
                cpred.append(self.predicates[entity.upper()])
            else:
                cpred.append = self.predicates[attribute.upper()]
        query = self.kb.query(*cpred)
        # Μετά βάλε condition και να επιλέγει και να επιστρέφει τα primaries

    def conditionQuery(self, query, entity, conditions):
        params = []
        for conditionedEntity in conditions:
            for c in conditions[conditionedEntity]:
                attribute = c[0]
                pathAttribute = None
                predicate = None
                if self.isPrimary(entity.upper(), attribute):
                    predicate = self.predicates[entity.upper()]
                    pathAttribute = getattr(predicate, attribute.lower())
                else:
                    predicate = self.predicates[attribute.upper()]
                    pathAttribute = getattr(predicate, attribute.lower())
                match c[1]:
                    case '=':
                        params.append(pathAttribute == c[2])
                    case '!=':
                        params.append(pathAttribute != c[2])
                    case '>':
                        params.append(pathAttribute > c[2])
                    case '<':
                        params.append(pathAttribute < c[2])
                    case '>=':
                        params.append(pathAttribute >= c[2])
                    case '<=':
                        params.append(pathAttribute == c[2])
        return query.where(*params)

    # Select from kb (More code needed for creating a condition)
    def select(self, entity, on=None, conditions=None, order=None, asc=True, group=None, attributes=None):
        p = []
        if type(entity) == list:
            for e in entity:
                p.append(self.predicates[e.upper()])
        else:
            p = [self.predicates[entity.upper()]]
        query = self.kb.query(*p)
        if type(entity) == list:
            if on:
                query = self.kb.query(*entity).join(on)
            else:
                raise Exception(
                    f"Attribute to join on for entities {entity} is not specified")
        if conditions:
            query = self.conditionQuery(query, entity, conditions)
        if order:
            query = query.order_by(order)
        if group:
            query = query.group_by(group)
        if attributes:
            a = []
            for attribute in attributes:
                if self.isPrimary(entity.upper(), attribute):
                    a.append(self.predicates[entity.upper()])
                else:
                    a.append = self.predicates[attribute.upper()]
            query = query.select(*a)
        return list(query.all())

    # Insert to kb and db
    def insert(self):
        pass

    # Update to kb and db
    def update(self):
        pass

    # Delete from kb and db
    def delete(self, entity, condtitions=None):
        attributes = self.schema[entity.upper()].keys()
        primaries = self.select(
            entity, conditions=condtitions, attributes='ID')
        p = [self.predicates[entity.upper()]]
        query = self.kb.query(p)
        query = self.conditionQuery()

    def toFile(self, path, format='lp'):
        filename = path + self.name.lower() + '.' + format
        f = open(filename, "w")
        content = FactBase.asp_str(self.kb)
        f.write(content)
        f.close()

    def run(self, asp):

        # Create a Control object that will unify models against the appropriate
        # predicates. Then load the asp file that encodes the problem domain.
        ctrl = Control(unifier=list(self.predicates.values()))
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
