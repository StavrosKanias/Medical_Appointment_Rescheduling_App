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
        self.splitPreds = self.createSplitPreds(schema)
        for sp in self.splitPreds:
            print(sp, [attr for attr in dir(self.splitPreds[sp]) if callable(
                getattr(self.splitPreds[sp], attr)) and not (attr.startswith("__") or attr.startswith("_")) and attr not in ['Field', 'clone', 'sign']])
        self.joins = {}
        if dbInfo:
            self.bindToDb(dbInfo)
            if dbConditions:
                self.conditions = dbConditions
            else:
                self.conditions = {}
            self.joins = self.getJoins()
            print(self.joins)
            self.db2kb()

        elif data:
            for entity in data:
                attributes = list(self.schema[entity])
                d = data[entity]
                self.insert(entity, attributes, d, toDb=False)

    def createSplitPred(self, attribute, t, primary):
        if t == 'boolean':
            content = primary.copy()
            predicateName = self.getPredicateName(attribute)
            predicate = type(predicateName,
                             (Predicate, ), content)
        else:
            field = self.TYPE2FIELD[t]
            content = primary.copy()
            content[attribute.lower()] = field
            predicateName = self.getPredicateName(attribute)
            predicate = type(predicateName,
                             (Predicate, ), content)
        return predicate

    def createSplitPreds(self, schema):
        predicates = {}
        for e in schema:
            predicates[e] = {}
            attributes = schema[e].copy()
            primary = self.createPrimaryPredicate(e, attributes,  predicates)
            for a in attributes:
                predicates[a] = self.createSplitPred(
                    a, attributes[a][0], primary)
        return predicates

    def isPrimary(self, entity, attribute):
        p = self.schema[entity.upper()][attribute]
        if p[1]:
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
                primaries.append(a)
        if len(primaries) == 1:
            return primaries[0]
        else:
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
        entcond = self.conditions[entity]
        conditions = {}
        for attribute in entcond:
            if entity in self.schema and attribute in self.schema[entity]:
                type = self.schema[entity][attribute][0]
            elif entity in self.conditions and attribute in self.conditions[entity]:
                type = self.conditions[entity][attribute][0]
            else:
                print(
                    f'Unable to specify the type of condition attribute {attribute}')
            attrcond = entcond[attribute][1]
            for condition in attrcond:
                conditions[f'{entity}.{attribute}'] = []
                if type in ['date', 'time'] and condition[1][0] in ['+', '-']:
                    if type == 'date':
                        conditions[f'{entity}.DATE'].append((condition[0], (datetime.today() +
                                                                            timedelta(days=int(condition[1][1:]))).strftime("%Y-%m-%d")))
                    elif type == 'time':
                        if f'{entity}.DATE' not in conditions:
                            print(
                                'Set date conditions before time conditions if they exist.')
                        conditions[f'{entity}.{attribute}'].append((condition[0], (datetime.now() +
                                                                                   timedelta(hours=int(condition[1][1:]))).strftime("%H:%M:%S")))
                        if conditions[f'{entity}.{attribute}'][-1][0] == '>' and conditions[f'{entity}.{attribute}'][-1][1] > '17:00:00':
                            conditions[f'{entity}.{attribute}'][-1] = (
                                '>', '09:00:00')
                            d = []
                            if f'{entity}.DATE' in conditions:
                                for t in conditions[f'{entity}.DATE'][-1][1].split('-'):
                                    d.append(int(t))
                                conditions[f'{entity}.DATE'][-1][1] = conditions[f'{entity}.DATE'][-1][0] + (
                                    datetime(*d) + timedelta(days=1)).strftime("%Y-%m-%d")
                        elif conditions[f'{entity}.{attribute}'][-1][0] == '<' and conditions[f'{entity}.{attribute}'][-1][1:] < '09:00:00':
                            conditions[f'{entity}.DATE'][-1] = (
                                '<', '17:00:00')
                            d = []
                            if f'{entity}.DATE' in conditions:
                                for t in conditions[f'{entity}.DATE'][-1][1:].split('-'):
                                    d.append(int(t))
                                conditions[f'{entity}.DATE'][-1] = conditions[f'{entity}.DATE'][-1][0] + (
                                    datetime(*d) + timedelta(days=-1)).strftime("%Y-%m-%d")
                else:
                    conditions[f'{entity}.{attribute}'].append(condition)
        return conditions

    def getJoins(self):
        inForeigns = {e: {} for e in self.schema}
        ijoins = {e: {a: [] for a in self.schema[e]} for e in self.schema}
        joins = {e: [] for e in self.schema}
        for e in self.schema:
            for a in self.schema[e]:
                inForeigns[e][a] = self.getInwardForeigns(e, a)
                for i in inForeigns[e][a]:
                    ijoins[i[0]][i[1]].append((e, a))

        for i in ijoins:
            for a in ijoins[i]:
                for f in ijoins[i][a]:
                    jlst = [(i, a, f[0], f[1])]
                    jlst.extend(joins[f[0]])
                    for cj in joins[i]:
                        for nj in jlst:
                            if cj[2] == nj[2]:
                                if cj[2] in self.conditions and nj[2] not in self.conditions:
                                    jlst.remove(nj)
                                elif cj[2] not in self.conditions and nj[2] in self.conditions:
                                    joins[i].remove(cj)
                                elif cj[2] in self.conditions and nj[2] in self.conditions:
                                    print(
                                        f'Unable to use db conditions due to cyclical dependency of entity {cj[2]}')
                                else:
                                    jlst.remove(nj)
                                    joins[i].remove(cj)

                    joins[i].extend(jlst)
        return joins

    # Translate db data to clingo predicates
    def db2kb(self):
        for e in self.schema:
            attributes = list(self.schema[e])
            joins = self.joins[e]
            conditions = {}
            for j in joins:
                if j[2] in self.conditions:
                    conditions.update(self.getConditions(j[2]))
            if e in self.conditions:
                conditions.update(self.getConditions(e))
            if conditions:
                data = self.db.select(
                    e, attributes, conditions, joins)
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
                if self.schema[entity.upper()][a.upper()][0] != 'boolean' or self.schema[entity.upper()][a.upper()][0] == 'boolean' and d[attributes.index(a)]:
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
            # For booleans
            booleans = {}
            cond = conditions.copy()
            for c in list(conditions):
                if self.schema[entity.upper()][c.upper()][0] == 'boolean':
                    booleans[c] = cond.pop(c)
            for b in booleans:
                pv.append(self.getBooleanPrimaries(entity, b, booleans[b]))
            # For non booleans
            for attribute in cond:
                primary = self.getPrimary(entity)
                query = None
                if self.isPrimary(entity, attribute):
                    query = self.kb.query(self.predicates[entity.upper()])
                else:
                    query = self.kb.query(self.predicates[attribute.upper()])
                query = self.conditionQuery(
                    query, entity, {attribute: cond[attribute]})
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

    def getBooleanPrimaries(self, entity, boolean, value):
        boolPred = self.predicates[boolean]
        boolPrimPred = getattr(boolPred, entity.lower()+'Id')
        true = set(self.kb.query(boolPred).select(boolPrimPred).all())
        if value:
            return true
        else:
            entPred = self.predicates[entity]
            primary = self.getPrimary(entity)
            primPred = getattr(entPred, primary.lower())
            allPrims = set(self.kb.query(entPred).select(primPred).all())
            return allPrims ^ true

    def getForeign(self, e1, e2):
        es = None
        ed = None
        for j in self.joins[e1]:
            if e2 == j[2]:
                es = e1
                fs = j[1]
                ed = e2
                fd = j[3]
        if not es and not ed:
            for j in self.joins[e2]:
                if e1 == j[2]:
                    es = e2
                    fs = j[1]
                    ed = e1
                    fd = j[3]
        elif not es and not ed:
            print(
                f'Unable to join the entities {e1} and {e2}. No foreign key found.')
        return (es, fs, ed, fd)

    def joinData(self, data, attributes=None):
        jattr = [a for l in attributes.values() for a in l]
        jdata = [d for d in data[list(data.keys())[0]]]
        for e1 in data:
            for e2 in data:
                if e1 != e2:
                    # # Get foreign key
                    f = self.getForeign(e1, e2)
                    s = f[0]
                    sf = f[1]
                    d = f[2]
                    df = f[3]
                # Find the index of the foreign in the data for both source and destination
                    if attributes:
                        fsi = attributes[s].index(sf)
                        fdi = attributes[d].index(df)
                    else:
                        fsi = list(self.schema[s].keys()).index(sf)
                        fdi = list(self.schema[d].keys()).index(df)
                # Hash the destination data according to the foreign
                destData = [d[fdi] for d in data[d]]
                # Join by appending destination data to source data if the foreigns match
                for d in data[s]:
                    if d[fsi] not in destData:
                        pass
        return data

    # Select from kb (break into two selects one with join and one without) also delete the data csvs in dataModel
    def select(self, entities, conditions=None, attributes=None, order=None):
        data = {e: [] for e in entities}
        primaries = {e: [] for e in entities}
        for e in entities:
            if e in conditions:
                primaries[e].extend(self.getMatchingPrimaries(
                    e, conditions=conditions[e]))
            else:
                primaries[e].extend(self.getMatchingPrimaries(e))
        if not attributes:
            attributes = {e: list(self.schema[e.upper()]) for e in entities}
        for e in entities:
            for p in primaries[e]:
                record = []
                for a in attributes[e]:
                    if self.isPrimary(e, a):
                        record.append(p)
                    elif self.schema[e.upper()][a.upper()][0] == 'boolean':
                        record.append(True)
                    else:
                        apred = self.predicates[a.upper()]
                        cpred = getattr(apred, e.lower()+'Id')
                        vpred = getattr(apred, a.lower())
                        query = self.kb.query(apred).where(
                            cpred == p).select(vpred)
                        record.append(list(query.all())[0])
                data[e].append(record)
        joinedData = self.joinData(data, attributes=attributes)
        # print(joinedData)
        # if order:
        #     if order not in attributes:
        #         print(
        #             f"Unable to order by the attribute {order} since it doesn't appear in the attribute list.")
        #     else:
        #         index = attributes.index(order)
        #         data = sorted(data, key=lambda item: item[index])
        return data

    # Update to kb and db
    def update(self, entity, conditions=None, values=None, toDb=True):
        # Update to kb
        primaries = self.getMatchingPrimaries(entity, conditions=conditions)
        primary = self.getPrimary(entity)
        for p in primaries:
            for v in values:
                if self.isPrimary(entity, v):
                    data = self.select(
                        entity, {primary: [('=', p)]})[0]
                    data[0] = values[v]
                    self.delete(
                        entity, {primary: [('=', p)]}, fromDb=False)
                    self.insert(entity, list(self.schema[entity]), [
                                data], toDb=False)
                elif self.schema[entity.upper()][v.upper()][0] == 'boolean':
                    if values[v]:
                        self.insert(entity, [v], [p], toDb=False)
                    else:
                        self.delete(
                            entity, {entity.lower(): p}, fromDb=False)
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

    def getInwardForeigns(self, entity, attribue):
        schema = self.schema.copy()
        schema.pop(entity)
        inForeigns = []
        for e in schema:
            for a in self.schema[e]:
                if len(self.schema[e.upper()][a]) == 4 and self.schema[e.upper()][a][2] == entity.upper() and self.schema[e.upper()][a][3] == attribue.upper():
                    inForeigns.append((e, a))
        return inForeigns

    def getOutwardForeigns(self, entity):
        outForeigns = []
        for a in self.schema[entity]:
            if len(self.schema[entity.upper()][a]) == 4:
                f = self.schema[entity.upper()][a]
                outForeigns.append((entity, a, f[2], f[3]))
        return outForeigns

    # Delete from kb and db
    def delete(self, entity, conditions=None, fromDb=True, cascade=False):
        # Delete from kb
        primary = self.getPrimary(entity)
        primaries = self.getMatchingPrimaries(entity, conditions=conditions)
        attributes = list(self.schema[entity.upper()])
        foreignPrims = {}
        for p in primaries:
            for a in attributes:
                # If foreign get all matching foreign records
                fattributes = self.getInwardForeigns(entity, a)
                if len(fattributes):
                    foreignPrims[a] = {}
                    for f in fattributes:
                        fe = f[0]
                        foreignPrims[a][fe] = []
                        fa = f[1]
                        fv = self.select(entity, conditions={
                            'ID': [('=', p)]}, attributes=[a])
                        fp = self.getMatchingPrimaries(
                            fe, conditions={fa: [('=', fv[0][0])]})
                        if len(fp):
                            foreignPrims[a][fe].extend(fp)
                if self.isPrimary(entity, a):
                    apred = self.predicates[entity.upper()]
                    cpred = getattr(apred, primary.lower())
                else:
                    apred = self.predicates[a.upper()]
                    cpred = getattr(apred, entity.lower()+'Id')
                self.kb.query(apred).where(cpred == p).delete()
        if cascade:
            # if cascade delete the foreign records
            for a in foreignPrims:
                for e in foreignPrims[a]:
                    for p in foreignPrims[a][e]:
                        fprim = self.getPrimary(e)
                        self.delete(e, conditions={
                                    fprim: [('=', p)]}, fromDb=False)
        # Delete from db
        if fromDb:
            self.db.delete(entity.upper(), conditions)
        return True

    def toFile(self, path, format='lp', entities=None, merged=False):
        filename = path + self.name.lower() + '.' + format
        f = open(filename, "w")
        if merged:
            if entities:
                subKb = self.extract(entities, merged=True)
                content = FactBase.asp_str(subKb)
            else:
                mergedKb = self.extract(list(self.schema), merged=True)
                content = FactBase.asp_str(mergedKb)
        else:
            if entities:
                subKb = self.extract(entities, merged=False)
                content = FactBase.asp_str(subKb)
            else:
                content = FactBase.asp_str(self.kb)
        f.write(content)
        f.close()

    # Add conditions
    def extract(self, predicates, merged=False):
        kb = FactBase()
        for e in predicates:
            content = {}
            if merged:
                data = []
            for p in predicates[e]:
                t = self.schema[e][p][0]
                field = self.TYPE2FIELD[t]
                content[p.lower()] = field
                if self.isPrimary(e, p):
                    apred = self.predicates[e.upper()]
                else:
                    apred = self.predicates[p.upper()]
                if merged:
                    vpred = getattr(apred, p.lower())
                    data.append(list(self.kb.query(apred).select(vpred).all()))
                else:
                    data = self.kb.query(apred).all()
                    kb.add(data)
            if merged:
                mergedPred = type(e, (Predicate, ), content)
                for i in range(len(data[0])):
                    v = []
                    for d in data:
                        v.append(d[i])
                    kb.add(mergedPred(*v))
        return kb

    def split(self, mergedPred):
        entity = type(mergedPred).__name__.upper()
        attributes = list(self.schema[entity])
        data = [getattr(mergedPred, attr) for attr in dir(mergedPred) if not callable(
            getattr(mergedPred, attr)) and not attr.startswith("__")]
        for a in attributes:
            if self.isPrimary(entity, a):
                p = self.splitPreds[entity.upper()]
            else:
                p = self.splitPreds[a]
            for d in data:
                if self.schema[entity.upper()][a.upper()][0] != 'boolean' or self.schema[entity.upper()][a.upper()][0] == 'boolean' and d[attributes.index(a)]:
                    pv = self.getPrimaryData(entity, attributes, d)
                    v = pv.copy()
                    if not self.isPrimary(entity, a) and not self.schema[entity.upper()][a.upper()][0] == 'boolean':
                        v.append(d[attributes.index(a)])
        return p(*v)

    def merge(self, predicate):
        pass

    def run(self, asp, outPreds=None, searchDuration=None, show=False, limit=False, subKB=None, subKBCond=None, merged=False, strOut=False):
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
        if limit:
            ctrl.configuration.solve.models = limit

        ctrl.load(asp)

        # Add the instance data and ground the ASP program
        if subKB:
            kb = self.extract(subKB, merged=merged)
            ctrl.add_facts(kb)
            self.toFile('clingo/sub_',
                        entities=subKB, merged=merged)
        else:
            ctrl.add_facts(self.kb)
        ctrl.ground([("base", [])])

        # Generate a solution - use a call back that saves the solution
        solution = None
        start = datetime.now()
        end = start + timedelta(minutes=int(searchDuration))

        def on_model(model):
            nonlocal solution
            if strOut:
                solution = [model.optimality_proven,
                            model.symbols(shown=True), model.cost, model.number]
            else:
                solution = [model.optimality_proven,
                            model.facts(atoms=True), model.cost, model.number]
            if solution[3] % 100 == 0:
                print(f'MODEL {solution[3]}\nBENEFIT {-solution[2][0]}')
            if datetime.now() > end:
                Control.interrupt(ctrl)

        ctrl.solve(on_model=on_model)
        if not solution:
            raise ValueError("No solution found")
        else:
            output = {}
            statistics = ctrl.statistics
            total_time = statistics['summary']['times']['total']
            cpu_time = statistics['summary']['times']['cpu']
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
                    if strOut:
                        print(solution[1])
                    else:
                        out = list(solution[1].query(p).all())
                        for o in out:
                            print(str(o))
                print('\nSTATISTICS\n')
                print(f'Benefit: {benefit}')
                print(f'Total time: {round(total_time,5)}')
                print(f'CPU time: {round(cpu_time,5)}')

            if strOut:
                return solution[1]
            else:
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
