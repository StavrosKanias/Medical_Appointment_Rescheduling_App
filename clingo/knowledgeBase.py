from clorm import monkey
monkey.patch()  # nopep8 # must call this before importing clingo
from copy import copy
import sys
sys.path.append('./db')  # nopep8
from dataModel import DataModel
from clorm import FactBase, Predicate, IntegerField, StringField
from clorm.clingo import Control
from datetime import datetime, timedelta
from customPredicates import DateField, TimeField


class KnowledgeBase():

    def __init__(self, name, schema, dbInfo=None, dbConditions=None, data=None):
        self.name = name
        self.schema = schema
        self.kb = FactBase()
        self.TYPE2FIELD = {'integer': IntegerField, 'boolean': IntegerField,
                           'text': StringField, 'date': DateField, 'time': TimeField}
        self.splitPreds = self.createSplitPreds()
        self.mergedPreds = self.createMergedPreds()
        self.foreignPaths = self.getForeignPaths()
        if dbInfo:
            self.bind2db(dbInfo)
            if dbConditions:
                self.conditions = dbConditions
            else:
                self.conditions = {}
            self.joins = self.getJoins()
            self.db2kb()

        elif data:
            for entity in data:
                d = data[entity]
                self.insert(entity, d, toDb=False)

    def showPredContent(self, p):
        print(p, [attr for attr in dir(p) if callable(
            getattr(p, attr)) and not (attr.startswith("__") or attr.startswith("_")) and attr not in ['Field', 'clone', 'sign']])

    def isPrimary(self, entity, attribute):
        p = self.schema[entity.upper()][attribute.upper()]
        if p[1]:
            return True
        else:
            return False

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
        for a in attributes:
            if self.isPrimary(entity, a):
                d = data[attributes.index(a)]
                primaryData.append(d)
        return primaryData

    def getSplitPredName(self, schemaName):
        predicate_name_parts = schemaName.split('_')
        predicate_name = ''
        for w in predicate_name_parts:
            predicate_name += w[0].upper() + w[1:].lower()
            if predicate_name_parts.index(w) < len(predicate_name_parts) - 1:
                predicate_name += '_'
        return predicate_name

    # Creates the primary fields taking into account complex primary keys
    def createPrimaryPredicate(self, entity, attributes, predicates):
        primary = {}
        for a in list(attributes):
            if self.isPrimary(entity, a):
                p = attributes.pop(a)
                primaryField = self.TYPE2FIELD[p[0]]
                primary[a.lower()] = primaryField
        content = primary.copy()
        predicateName = self.getSplitPredName(entity)
        predicate = type(predicateName, (Predicate, ), content)
        predicates[entity] = predicate
        for p in list(primary):
            primary[entity.lower() + p[0].upper() + p[1:]] = primary.pop(p)
        return primary

    def createSplitPreds(self):
        predicates = {}
        for e in self.schema:
            predicates[e] = {}
            attributes = self.schema[e].copy()
            primary = self.createPrimaryPredicate(e, attributes,  predicates)
            for a in attributes:
                predicates[a] = self.createSplitPred(a,
                                                     {a: attributes[a][0]}, primary=primary)
        return predicates

    def createSplitPred(self, name, attributes, primary=None):
        predicateName = self.getSplitPredName(name)
        if primary:
            content = primary.copy()
        else:
            content = {}

        for a in attributes:
            field = self.TYPE2FIELD[attributes[a]]
            content[a.lower()] = field
        if len(list(content)):
            predicate = type(predicateName,
                             (Predicate, ), content)
        else:
            print('Unable to create empty predicate')
            return False
        return predicate

    def createMergedPreds(self):
        predicates = {}
        for e in self.schema:
            predicates[e] = []
            attributes = self.schema[e]
            predicates[e] = self.createMergedPred(e)
        return predicates

    def createMergedPred(self, entity, attributes=None):
        if not attributes:
            attributes = list(self.schema[entity])
        content = {a.lower(): self.TYPE2FIELD[self.schema[entity.upper()][a.upper()][0]]
                   for a in attributes}
        predicateName = entity[0].upper() + entity[1:].lower()
        predicate = type(predicateName,
                         (Predicate, ), content)
        return predicate

    def split(self, mergedPred):
        entity = type(mergedPred).__name__.upper()
        if entity not in self.schema:
            print(
                f'Predicate {entity} doesn not appear on the database schema.')
            return False
        attributes = [attr for attr in dir(type(mergedPred)) if callable(
            getattr(type(mergedPred), attr)) and not (attr.startswith("__") or attr.startswith("_")) and attr not in ['Field', 'clone', 'sign']]
        data = [getattr(mergedPred, attr) for attr in attributes]
        prim = None
        outPreds = []
        for a in attributes:
            if self.isPrimary(entity, a):
                p = self.splitPreds[entity.upper()]
                prim = a
            else:
                p = self.splitPreds[a.upper()]
            if self.schema[entity.upper()][a.upper()][0] != 'boolean' or self.schema[entity.upper()][a.upper()][0] == 'boolean' and data[attributes.index(a)]:
                pv = self.getPrimaryData(entity, attributes, data)
                v = pv.copy()
                if not self.isPrimary(entity, a) and not self.schema[entity.upper()][a.upper()][0] == 'boolean':
                    v.append(data[attributes.index(a)])
                outPreds.append(p(*v))
        if not prim:
            print('No primary key found. Unable to split predicate.')
            return False
        return outPreds

    def merge(self, splitPreds):
        pname = None
        content = {}
        data = []
        attributes = {type(sp).__name__: [attr for attr in dir(type(sp)) if callable(
            getattr(type(sp), attr)) and not (attr.startswith("__") or attr.startswith("_")) and attr not in ['Field', 'clone', 'sign']] for sp in splitPreds}
        for e in attributes:
            if len(attributes[e]) == 1:
                pname = e
        if not pname:
            print(' Unable to merge predicates. No primary key found')
            return False

        for sp in splitPreds:
            attr = attributes[type(sp).__name__]
            if len(attr) == 1:
                aname = attr[0]
            else:
                aname = type(sp).__name__.lower()
            d = getattr(sp, aname)
            data.append(d)
            t = self.schema[pname.upper()][aname.upper()][0]
            field = self.TYPE2FIELD[t]
            content[aname] = field
        mergedPred = type(
            pname[0].upper() + pname[1:].lower(), (Predicate, ), content)
        return mergedPred(*data)

    def isForeign(self, e, a):
        if len(self.schema[e][a]) == 4:
            return True
        else:
            return False

    def getForeign(self, e1, e2):
        fs = None
        fd = None
        if e1 in self.joins and e2 in self.joins:
            for j in self.joins[e1]:
                if j[0] == e1 and j[2] == e2:
                    fs = (j[0], j[1])
                    fd = (j[2], j[3])
                    return (fs, fd)
        return False

    def clear2dDict(self, dict):
        for e in list(dict):
            for a in list(dict[e]):
                if not dict[e][a]:
                    dict[e].pop(a)
            if not dict[e]:
                dict.pop(e)

    def getForeignPath(self, jent, e, a=None):
        fpaths = self.in2out(self.foreignPaths)
        for fe in self.foreignPaths:
            for fa in self.foreignPaths[fe]:
                for j in self.foreignPaths[fe][fa]:
                    if fe in fpaths:
                        if fa in fpaths[fe]:
                            fpaths[fe][fa].append(j)
                        else:
                            fpaths[fe][fa] = [j]
        p = []
        if a:
            if e in fpaths and a in fpaths[e]:
                p = fpaths[e][a]
        else:
            if e in fpaths:
                p = list(fpaths[e].values())
                if p:
                    p = p[0]
        for j in p:
            if a:
                if j[0] not in jent:
                    jent[j[0]] = [(j[1], e, a)]
                elif j not in jent[j[0]]:
                    jent[j[0]].append((j[1], e, a))
            else:
                if j[0] not in jent:
                    jent.append(j[0])

    def getForeignPaths(self):
        inForeigns = {e: {} for e in self.schema}
        paths = {e: {} for e in self.schema}
        for e in self.schema:
            for a in self.schema[e]:
                inForeigns[e][a] = self.getInwardForeigns(e, a)
                for i in inForeigns[e][a]:
                    if i[1] not in paths[i[0]]:
                        paths[i[0]][i[1]] = [(e, a)]
                    else:
                        paths[i[0]][i[1]].append((e, a))

        for e in paths:
            for a in paths[e]:
                for f in paths[e][a]:
                    if f[1] in paths[f[0]]:
                        for nf in paths[f[0]][f[1]]:
                            if nf not in paths[e][a]:
                                paths[e][a].append(nf)
        self.clear2dDict(paths)
        return paths

    def in2out(self, inPaths):
        outPaths = {e: {} for e in self.schema}
        for e in inPaths:
            for a in inPaths[e]:
                for j in inPaths[e][a]:
                    if j[1] not in outPaths[j[0]]:
                        outPaths[j[0]][j[1]] = [(e, a)]
                    elif (e, a) not in outPaths[j[0]][j[1]]:
                        outPaths[j[0]][j[1]].append((e, a))
        return outPaths

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

    def getAllDeps(self, ent, cond):
        jent = self.getJoinEntities(ent, cond)
        jlst = []
        for e1 in jent:
            for e2 in list(jent)[list(jent).index(e1) + 1:]:
                f = self.getForeign(e1, e2)
                if not f:
                    f = self.getForeign(e2, e1)
                if f and f not in jlst:
                    jlst.append(f)
        return jlst

    def getDepChain(self, e1, e2):
        e1 = e1.upper()
        e2 = e2.upper()
        d = e2
        joins = self.joins[e1].copy()
        invPath = [d]
        if joins:
            while d != e1:
                for j in joins:
                    if d == j[2]:
                        d = j[0]
                        joins.remove(j)
                        invPath.append(d)
                if len(invPath) == 1:
                    return False
        return invPath

    def getJoinPreds(self, e1, e2):
        e1 = e1.upper()
        e2 = e2.upper()
        f = self.getForeign(e1, e2)
        if f:
            p1 = self.mergedPreds[f[0][0]]
            p2 = self.mergedPreds[f[1][0]]
            j1 = getattr(p1, f[0][1].lower())
            j2 = getattr(p2, f[1][1].lower())
            return (j1, j2)
        else:
            return False

    def getJoinEntities(self, ent, cond):
        jent = list(ent)
        cond = {e.upper(): {a.upper(): cond[e][a]
                            for a in cond[e]} for e in cond}
        for e in cond:
            for je in ent:
                if e != je:
                    path = self.getDepChain(je, e)
                    if not path:
                        path = self.getDepChain(e, je)
                    if path:
                        for p in path:
                            if p not in jent:
                                jent.append(p)
                    else:
                        print(f'Unable to link joined entities {e}, {je}.')
                        return False
        return jent

    def getJoins(self):
        inForeigns = {e: {} for e in self.schema}
        ijoins = {e: {a: [] for a in self.schema[e]} for e in self.schema}
        joins = {e: [] for e in self.schema}
        for e in self.schema:
            for a in self.schema[e]:
                inForeigns[e][a] = self.getInwardForeigns(e, a)
                for i in inForeigns[e][a]:
                    ijoins[i[0]][i[1]].append([e, a])
        for i in ijoins:
            for a in ijoins[i]:
                for f in ijoins[i][a]:
                    jlst = [[i, a, f[0], f[1]]]
                    jlst.extend(joins[f[0]])
                    joins[i].extend(jlst)
        return joins

    def getJoinedConditions(self, e, joins):
        conditions = {}
        conde = [e]
        for j in joins:
            if j[0] in self.conditions and j[0] not in conde:
                conde.append(j[0])
            if j[2] in self.conditions and j[2] not in conde:
                conde.append(j[2])
        for e in conde:
            if e in self.conditions:
                cond = self.getDbConditions(
                    e, self.conditions[e])
                conditions.update(cond)

        return conditions

    def getDbConditions(self, entity, cond):
        entity = entity.upper()
        cond = {a.upper(): cond[a] for a in cond}
        conditions = {}
        for attribute in cond:
            if entity in self.schema and attribute in self.schema[entity]:
                type = self.schema[entity][attribute][0]
            elif entity in self.conditions and attribute in self.conditions[entity]:
                type = self.conditions[entity][attribute][0]
            else:
                print(
                    f'Unable to specify the type of condition attribute {attribute}')
            attrcond = cond[attribute]
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

    def bind2db(self, dbInfo):
        self.db = DataModel(dbInfo[0], dbInfo[1], dbInfo[2])
        dbEntities = self.db.getTables()
        for s in self.schema:
            if s not in [de.upper() for de in dbEntities]:
                raise Exception(
                    f"Invalid schema.\nPredicate {s} doesn't exist in the given database.")
            else:
                dbAttributes = self.db.getAttributes(s)
                for a in self.schema[s]:
                    if a not in [da.upper() for da in dbAttributes]:
                        raise Exception(
                            f"Invalid schema.\nAttribute {a} of entity {s} doesn't exist in the given database.")

    # Translate db data to clingo predicates
    def db2kb(self):
        for e in self.schema:
            attributes = list(self.schema[e])
            joins = self.joins[e]
            conditions = self.getJoinedConditions(e, joins)
            if conditions:
                data = self.db.select(
                    e, attributes, conditions, joins)
                if data:
                    self.insert(e, data, toDb=False)
                else:
                    print('No database data found to fit the conditions ' +
                          str(conditions) + ' for entity ' + e + '.')
            else:
                data = self.db.select(e, attributes)
                if data:
                    self.insert(e, data, toDb=False)
                else:
                    print('No database data found for the entity ' + e + ' ')

    def getCompExp(self, entity, conditions):
        explst = []
        for attribute in conditions:
            for c in conditions[attribute]:
                compop = c[0]
                pathAttribute = None
                entPred = self.mergedPreds[entity.upper()]
                pathAttribute = getattr(entPred, attribute.lower())
                match compop:
                    case '=':
                        explst.append(pathAttribute == c[1])
                    case '!=':
                        explst.append(pathAttribute != c[1])
                    case '>':
                        explst.append(pathAttribute > c[1])
                    case '<':
                        explst.append(pathAttribute < c[1])
                    case '>=':
                        explst.append(pathAttribute >= c[1])
                    case '<=':
                        explst.append(pathAttribute <= c[1])
        return explst

    # Select from kb also delete the data csvs in dataModel
    def select(self, ent, cond=None, order=None, pOut=False, getQuery=False):
        if type(ent) == dict:
            ent = {e.upper(): [a.upper() for a in ent[e]]
                   for e in ent}
        elif type(ent) == list:
            ent = [e.upper() for e in ent]
        else:
            print('Invalid input')
            return False
        jlst = []
        entpred = None
        jent = None
        if cond:
            jent = self.getJoinEntities(ent, cond)
        else:
            jent = list(ent)
        for e1 in jent:
            for e2 in list(jent)[list(jent).index(e1) + 1:]:
                j = self.getJoinPreds(e1, e2)
                if not j:
                    j = self.getJoinPreds(e2, e1)
                if (j):
                    jlst.append(j[0] == j[1])
        if type(ent) == dict:
            entpred = [self.mergedPreds[e.upper()] for e in jent]
            if pOut:
                outPreds = [self.createMergedPred(e, ent[e]) for e in ent]
            attrlst = [getattr(self.mergedPreds[e.upper()],
                               a.lower()) for e in ent for a in ent[e]]

        elif type(ent) == list:
            entpred = [self.mergedPreds[e.upper()] for e in jent]
            attr = {e: list(self.schema[e.upper()]) for e in ent}
            if pOut:
                attrlst = [self.mergedPreds[e.upper()] for e in ent]
            else:
                attrlst = [getattr(self.mergedPreds[e.upper()],
                                   a.lower()) for e in attr for a in attr[e]]
        if not entpred:
            print('Unable to create query.')
            return False

        else:
            query = self.kb.query(*entpred)
        if len(jlst):
            query = query.join(*jlst)
        if cond:
            cent = {c.upper(): cond[c] for c in cond}
            condlst = []
            for e in jent:
                if e in cent:
                    condlst.extend(self.getCompExp(e, conditions=cent[e]))
            query = query.where(*condlst)
        query = query.select(*attrlst)

        if order:
            if type(order) != dict:
                print(
                    'Wrong input format for order attributes. Dictionary input required.')
                return False
            ordlst = [getattr(self.mergedPreds[e.upper()], o.lower())
                      for e in order for o in order[e]]
            query = query.order_by(*ordlst)

        pdata = list(query.all())
        outdata = []

        if pOut and type(ent).__name__ == 'dict':
            attrlen = [len(ent[e]) for e in ent]
            eind = []
            s = 0
            d = attrlen[0]
            for e in ent:
                eind.append((s, d))
                ei = list(ent).index(e)
                if ei < len(ent) - 1:
                    s = d
                    d += attrlen[ei+1]
            for pd in pdata:
                for e in ent:
                    ei = list(ent).index(e)
                    outdata.append(outPreds[ei](
                        *pd[eind[ei][0]:eind[ei][1]]))

        else:
            outdata = pdata.copy()

        if getQuery:
            qout = copy(query)
            return outdata, qout
        else:
            return outdata

    # Insert to kb and db
    def insert(self, entity, data, toDb=True):
        p = self.mergedPreds[entity]
        for d in data:
            self.kb.add(p(*d))
        if toDb:
            self.db.insert(entity, data)

    def cascade(self, ent):
        jent = ent.copy()
        for e in ent:
            if type(jent) == dict:
                for a in jent[e]:
                    self.getForeignPath(jent, e, a)
            if type(jent) == list:
                self.getForeignPath(jent, e)
        return jent

    # Update to kb and db

    def update(self, upd, cond=None, cascade=True, toDb=True):
        upd = {e.upper(): {a.upper(): upd[e][a] for a in upd[e]} for e in upd}
        ent = {e: list(upd[e]) for e in upd}
        # Update to kb
        mpreds = self.delete(ent, cascade=cascade,
                             cond=cond, getData=list(upd), fromDb=False)
        if cascade:
            cent = self.cascade(ent)
            val = {e: {} for e in cent}
            for e in cent:
                for a in cent[e]:
                    if type(a) == tuple:
                        aname = a[0].lower()
                        if aname.upper() not in self.schema[e]:
                            print(
                                f'Unknown attribute {aname.upper()} for entity {e}')
                            return False
                        val[e][aname] = upd[a[1]][a[2]]
                    else:
                        aname = a.lower()
                        if aname.upper() not in self.schema[e]:
                            print(
                                f'Unknown attribute {aname.upper()} for entity {e}')
                            return False
                        val[e][aname] = upd[e][a]
        for m in mpreds:
            for p in m:
                e = type(p).__name__.upper()
                p = p.clone(**val[e])
                self.kb.add(p)

        # Update to db
        if toDb:
            val = {e.upper(): {a.upper(): val[e][a]
                               for a in val[e]} for e in val}
            jlst = self.getAllDeps(cent, cond)
            conditions = {}
            for c in cond:
                conditions.update(self.getDbConditions(c, cond[c]))
            self.db.update(list(cent)[-1].upper(),
                           val[e], conditions, joins=jlst)
        return True

    def delete(self, ent, cond=None, getData=None, cascade=True, fromDb=True):
        ent = [e.upper() for e in ent]
        if cond:
            ent.extend(e.upper() for e in cond if e not in ent)
        if cascade:
            cent = self.cascade(ent)
            mpreds, qsout = self.select(
                cent, cond=cond, pOut=True, getQuery=True)
        else:
            mpreds, qsout = self.select(
                list(ent), cond=cond, pOut=True, getQuery=True)
        qsout.delete()

        if fromDb:
            jlst = self.getAllDeps(ent, cond)
            conditions = {}
            for c in cond:
                conditions.update(self.getDbConditions(c, cond[c]))
            for e in ent:
                self.db.delete(e, conditions=conditions, joins=jlst)

        if getData:
            outPreds = []
            iOut = []
            for e in cent:
                for d in getData:
                    if d.upper() == e:
                        if cascade:
                            iOut.append(cent.index(e))
                        else:
                            iOut.append(ent.index(e))
            for m in mpreds:
                outPreds.append(m[i] for i in iOut)
            return outPreds
        else:
            return True

    def extract(self, ent, split=False, cond=None, order=None):
        outKB = FactBase()
        preds = self.select(ent, cond=cond, order=order, pOut=True)
        for p in preds:
            if split:
                spreds = self.split(p)
                for sp in spreds:
                    outKB.add(sp)
            else:
                outKB.add(p)
        return outKB

    def run(self, asp, outPreds=None, searchDuration=None, show=False, limit=False, subKB=None, subKBCond=None, merged=False, symbOut=False):
        # Create a Control object that will unify models against the appropriate predicates.
        # Then load the asp file that encodes the problem domain.
        fname = asp.split('/')[-1]
        print(f'\nExecuting {fname}...')
        predicates = list(self.mergedPreds.values())
        if outPreds:
            for p in outPreds:
                if p not in predicates:
                    predicates.append(p)
        ctrl = Control(unifier=predicates)
        if limit:
            ctrl.configuration.solve.models = limit
        try:
            ctrl.load(asp)
        except RuntimeError as e:
            print('Aborting...')
            return False

        # Add the instance data and ground the ASP program
        if subKB:
            # Fill in primaries if needed
            for e in subKB:
                prim = self.getPrimary(e)
                if prim not in subKB[e]:
                    subKB[e].insert(0, prim)
            if merged:
                kb = self.extract(subKB, cond=subKBCond)
            else:
                kb = self.extract(subKB, cond=subKBCond, split=True)
            ctrl.add_facts(kb)
            self.toFile('clingo/sub_', entities=subKB, merged=merged)

        else:
            ctrl.add_facts(self.kb)
        ctrl.ground([("base", [])])

        # Generate a solution - use a call back that saves the solution
        solution = None
        start = datetime.now()
        end = start + timedelta(minutes=int(searchDuration))

        def on_model(model):
            nonlocal solution
            if symbOut:
                solution = [model.optimality_proven,
                            model.cost, model.number, model.symbols(shown=True)]
            else:
                solution = [model.optimality_proven,
                            model.cost, model.number, model.facts(atoms=True)]
            if solution[2] % 100 == 0:
                print(f'MODEL {solution[2]}\nBENEFIT {-solution[1][0]}')
            if datetime.now() > end:
                Control.interrupt(ctrl)

        ctrl.solve(on_model=on_model)
        if not solution:
            print('No solution found.\n')
            return False

        elif not solution[1]:
            print(
                'No actions found to reach optimality\nCheck the grinding info above for more details')

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
                    if symbOut:
                        print(solution[3])
                    else:
                        out = list(solution[3].query(p).all())
                        for o in out:
                            print(str(o))
                print('\nSTATISTICS\n')
                print(f'Model: {solution[2]}')
                print(f'Benefit: {benefit}')
                print(f'Total time: {round(total_time,5)}')
                print(f'CPU time: {round(cpu_time,5)}')

            if symbOut:
                return solution[3]
            else:
                for p in outPreds:
                    out = list(solution[3].query(p).all())
                    output[p.__name__] = out
                return output

    def toFile(self, path, format='lp', entities=None, merged=True):
        filename = path + self.name.lower() + '.' + format
        f = open(filename, "w")
        if entities:
            if merged:
                subKb = self.extract(entities)
            else:
                subKb = self.extract(entities, split=True)
            content = FactBase.asp_str(subKb)
        else:
            content = FactBase.asp_str(self.kb)
        f.write(content)
        f.close()

    def clear(self):
        for e in self.schema:
            self.delete([e], fromDb=False)

    def reload(self):
        self.clear()
        self.db2kb()

    def __repr__(self):
        return FactBase.asp_str(self.kb)

    def __str__(self):
        return FactBase.asp_str(self.kb)
