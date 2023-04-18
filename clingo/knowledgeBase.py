from clorm import monkey
monkey.patch()  # nopep8 # must call this before importing clingo
from clorm import FactBase, Predicate, IntegerField, StringField
from clorm.clingo import Control
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
        if dbInfo:
            self.bindToDb(dbInfo)
            if dbConditions:
                self.conditions = dbConditions
            else:
                self.conditions = {}
            self.db2kb()

        elif data:
            for entity in data:
                attributes = list(self.schema[entity])
                d = data[entity]
                self.insert(entity, attributes, d, toDb=False)

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
                    predicateName = self.getPredicateName(a)
                    predicate = type(predicateName,
                                     (Predicate, ), content)
                    predicates[a] = predicate
        return predicates

    def isPrimary(self, entity, attribute):
        p = self.schema[entity.upper()][attribute]
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

    def getPrimary(self, entity):
        primaries = []
        for a in list(self.schema[entity.upper()]):
            if self.isPrimary(entity.upper(), a):
                return a

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
        entcond = self.conditions[entity]
        conditions = {}
        for attribute in entcond:
            attrcond = entcond[attribute]
            for condition in attrcond:
                conditions[attribute] = []
                if condition[1][0] in ['+', '-']:
                    if self.schema[entity][attribute][0] == 'date':
                        conditions[attribute].append((condition[0], (datetime.today() +
                                                                     timedelta(days=int(condition[1][1:]))).strftime("%Y-%m-%d")))
                    elif self.schema[entity][attribute][0] == 'time':
                        conditions[attribute].append((condition[0], (datetime.now() +
                                                                     timedelta(hours=int(condition[1][1:]))).strftime("%H:%M:%S")))
                        if conditions[attribute][-1][0] == '>' and conditions[attribute][-1][1] > '17:00:00':
                            conditions[attribute][-1][1] = '> 09:00:00'
                            d = []
                            for t in conditions['DATE'][-1][1].split('-'):
                                d.append(int(t))
                            conditions['DATE'][-1][1] = conditions['DATE'][-1][0] + (
                                datetime(*d) + timedelta(days=1)).strftime("%Y-%m-%d")
                        elif conditions[attribute][-1][0] == '<' and conditions[attribute][-1][1:] < '09:00:00':
                            conditions[attribute][-1][1] = '< 17:00:00'
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
        for e in self.schema:
            attributes = list(self.schema[e])
            if e in self.conditions:
                conditions = self.getConditions(e)
                data = self.db.select(e, attributes, conditions)
                if data:
                    self.insert(e, attributes, data, toDb=False)
                else:
                    print('No database data found to fit the conditions ' +
                          str(conditions) + ' for entity ' + e + '.')
            else:
                data = self.db.select(e, attributes)
                if data:
                    self.insert(e, attributes, data, toDb=False)
                else:
                    print('No database data found for the entity ' + e + ' ')

    # Insert to kb and db
    def insert(self, entity, attributes, data, toDb=True):
        for a in attributes:
            if self.isPrimary(entity, a):
                p = self.predicates[entity.upper()]
            else:
                p = self.predicates[a]
            for d in data:
                pv = self.getPrimaryData(entity, attributes, d)
                v = pv.copy()
                if not self.isPrimary(entity, a) and not self.schema[entity.upper()][a.upper()][0] == 'boolean':
                    v.append(d[attributes.index(a)])
                self.kb.add(p(*v))
        if toDb:
            self.db.insert(entity, data)

    def getMatchingPrimaries(self, entity, conditions=None):
        pv = []
        if conditions:
            for attribute in conditions:
                primary = self.getPrimary(entity)
                query = None
                if self.isPrimary(entity, attribute):
                    query = self.kb.query(self.predicates[entity.upper()])
                else:
                    query = self.kb.query(self.predicates[attribute.upper()])
                query = self.conditionQuery(
                    query, entity, {attribute: conditions[attribute]})
                if self.isPrimary(entity, attribute):
                    attrpred = getattr(self.predicates[
                        entity.upper()], primary.lower())
                    query = query.select(attrpred)
                else:
                    attrpred = getattr(
                        self.predicates[attribute.upper()], entity.lower()+primary[0].upper()+primary[1:].lower())
                    query = query.select(attrpred)
                pv.append(set(query.all()))
            return list(pv[0].intersection(*pv))
        else:
            primary = self.getPrimary(entity)
            query = self.kb.query(self.predicates[entity.upper()])
            attrpred = getattr(self.predicates[
                entity.upper()], primary.lower())
            query = query.select(attrpred)
            return list(query.all())

    def conditionQuery(self, query, entity, conditions):
        params = []
        for attribute in conditions:
            for c in conditions[attribute]:
                compop = c[0]
                pathAttribute = None
                predicate = None
                if self.isPrimary(entity.upper(), attribute):
                    predicate = self.predicates[entity.upper()]
                    pathAttribute = getattr(predicate, attribute.lower())
                else:
                    predicate = self.predicates[attribute.upper()]
                    pathAttribute = getattr(predicate, attribute.lower())
                match compop:
                    case '=':
                        params.append(pathAttribute == c[1])
                    case '!=':
                        params.append(pathAttribute != c[1])
                    case '>':
                        params.append(pathAttribute > c[1])
                    case '<':
                        params.append(pathAttribute < c[1])
                    case '>=':
                        params.append(pathAttribute >= c[1])
                    case '<=':
                        params.append(pathAttribute <= c[1])
        return query.where(*params)

    # Select from kb (Can be extended to use joins and grouping)
    def select(self, entity, conditions=None, attributes=None, order=None):
        data = []
        primaries = self.getMatchingPrimaries(entity, conditions=conditions)
        if not attributes:
            attributes = list(self.schema[entity.upper()])
        for p in primaries:
            record = []
            for a in attributes:
                if self.isPrimary(entity, a):
                    record.append(p)
                elif self.schema[entity.upper()][a.upper()][0] == 'boolean':
                    record.append(1)
                else:
                    apred = self.predicates[a.upper()]
                    cpred = getattr(apred, entity.lower()+'Id')
                    vpred = getattr(apred, a.lower())
                    query = self.kb.query(apred).where(
                        cpred == p).select(vpred)
                    record.append(list(query.all())[0])
            data.append(record)
            if order:
                if order not in attributes:
                    print(
                        f"Unable to order by the attribute {order} since it doesn't appear in the attribute list.")
                else:
                    index = attributes.index(order)
                    data = sorted(data, key=lambda item: item[index])
        return data

    # Update to kb and db
    def update(self, entity, conditions=None, values=None, toDb=True):
        # for booleans insert and delete (Fix conditions to accept boolean)
        # Update to kb
        for c in conditions:
            if self.schema[entity.upper()][c.upper()][0] == 'boolean':
                pass

        primaries = self.getMatchingPrimaries(entity, conditions=conditions)
        primary = self.getPrimary(entity)
        for p in primaries:
            for v in values:
                if self.isPrimary(entity, v):
                    # Select needed for every attribute to change the primary key everywhere
                    # The primary has to change first therefore two loops are needed
                    data = self.select(
                        entity, {primary: [('=', p)]}, order=primary)[0]
                    data[0] = values[v]
                    self.delete(entity, {primary: [('=', p)]}, fromDb=False)
                    self.insert(entity, list(self.schema[entity.upper()]), [
                                data], toDb=False)
                elif self.schema[entity.upper()][v.upper()][0] == 'boolean':
                    if values[v]:
                        self.insert(entity, [v], [p], toDb=False)
                    else:
                        self.delete(entity, {entity.lower(): p}, fromDb=False)
                else:
                    # Delete the specific predicate
                    apred = self.predicates[v.upper()]
                    cpred = getattr(apred, entity.lower()+'Id')
                    self.kb.query(apred).where(cpred == p).delete()
                    # Insert with new field value
                    self.insert(entity, [primary, v], [
                                [p, values[v]]], toDb=False)

        # Update to db
        if toDb:
            self.db.update(entity.upper(), conditions, values)
        return True

    # Delete from kb and db
    def delete(self, entity, conditions=None, fromDb=True):
        # Delete from kb
        primary = self.getPrimary(entity)
        primaries = self.getMatchingPrimaries(entity, conditions=conditions)
        attributes = list(self.schema[entity.upper()])
        for p in primaries:
            for a in attributes:
                if self.isPrimary(entity, a):
                    apred = self.predicates[entity.upper()]
                    cpred = getattr(apred, primary.lower())
                else:
                    apred = self.predicates[a.upper()]
                    cpred = getattr(apred, entity.lower()+'Id')
                self.kb.query(apred).where(cpred == p).delete()
        # Delete from db
        if fromDb:
            self.db.delete(entity.upper(), conditions)
        return True

    def toFile(self, path, format='lp'):
        filename = path + self.name.lower() + '.' + format
        f = open(filename, "w")
        content = FactBase.asp_str(self.kb)
        f.write(content)
        f.close()

    def run(self, asp, outPreds=None, show=False):
        # Create a Control object that will unify models against the appropriate
        # predicates. Then load the asp file that encodes the problem domain.
        fname = asp.split('/')[-1]
        print(f'\nExecuting {fname}...')
        predicates = list(self.predicates.values())
        if outPreds:
            for p in outPreds:
                if p not in predicates:
                    predicates.append(p)
        ctrl = Control(unifier=predicates)
        ctrl.load(asp)

        # Add the instance data and ground the ASP program
        ctrl.add_facts(self.kb)
        ctrl.ground([("base", [])])

        # Generate a solution - use a call back that saves the solution
        solution = None

        def on_model(model):
            nonlocal solution
            solution = [model.optimality_proven,
                        model.facts(atoms=True), model.cost]

        ctrl.solve(on_model=on_model)
        if not solution:
            raise ValueError("No solution found")
        else:
            output = {}
            statistics = ctrl.statistics
            time_elapsed = statistics['summary']['times']['cpu']
            benefit = -statistics['summary']['lower'][0]
            optimal = statistics['summary']['models']['optimal']
            if show:
                if optimal and solution[0]:
                    print('\nOPTIMAL SOLUTION FOUND')
                elif optimal and not solution[0]:
                    print('\nOPTIMUM SOLUTION FOUND')
                else:
                    print('\nOPTIMIZATION FAILED')

                print('\nOUTPUT\n')
                for p in outPreds:
                    out = list(solution[1].query(p).all())
                    for o in out:
                        print(str(o))
                print('\nSTATISTICS\n')
                print(f'Benefit: {benefit}')
                print(f'Time elapsed: {round(time_elapsed,20)}')

            # return list(solution.query(outPred).all())
            for p in outPreds:
                out = list(solution[1].query(p).all())
                output[p.__name__] = out
            return output

    def clear(self):
        for e in self.schema:
            self.delete(e, fromDb=False)

    def reload(self):
        self.clear()
        self.db2kb()

    def __repr__(self):
        return FactBase.asp_str(self.kb)

    def __str__(self):
        return FactBase.asp_str(self.kb)
