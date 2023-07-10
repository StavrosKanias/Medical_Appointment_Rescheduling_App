from clorm import monkey
monkey.patch()  # nopep8 # must call this before importing clingo
from clorm import FactBase, Predicate, IntegerField, StringField
from clorm.clingo import Control
from datetime import datetime, timedelta
import sys
sys.path.append('./db')  # nopep8
from dataModel import DataModel
from dbCreator import schema
sys.path.append('./clingo')  # nopep8
from customPredicates import DateField, TimeField
from copy import copy


class KnowledgeBase():

    def __init__(self, name, schema, dbInfo=None, dbConditions=None, data=None):
        self.name = name
        self.schema = schema
        self.kb = FactBase()
        self.TYPE2FIELD = {'integer': IntegerField, 'boolean': IntegerField,
                           'text': StringField, 'date': DateField, 'time': TimeField}
        self.splitPreds = self.createSplitPreds()
        self.mergedPreds = self.createMergedPreds()
        if dbInfo:
            self.bind2db(dbInfo)
            if dbConditions:
                self.conditions = dbConditions
            else:
                self.conditions = {}
            self.joins = self.getJoins()
            self.getForeignPath('DOCTOR', 'REQUEST')
            self.db2kb()

        elif data:
            for entity in data:
                attributes = list(self.schema[entity])
                d = data[entity]
                self.insert(entity, d, toDb=False)

    def showPreds(self):
        for sp in self.splitPreds:
            print(sp, [attr for attr in dir(self.splitPreds[sp]) if callable(
                getattr(self.splitPreds[sp], attr)) and not (attr.startswith("__") or attr.startswith("_")) and attr not in ['Field', 'clone', 'sign']])
        for mp in self.mergedPreds:
            print(mp, [attr for attr in dir(self.mergedPreds[mp]) if callable(
                getattr(self.mergedPreds[mp], attr)) and not (attr.startswith("__") or attr.startswith("_")) and attr not in ['Field', 'clone', 'sign']])

    def createSplitPreds(self):
        predicates = {}
        for e in self.schema:
            predicates[e] = {}
            attributes = self.schema[e].copy()
            primary = self.createPrimaryPredicate(e, attributes,  predicates)
            print(primary)
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

    def isPrimary(self, entity, attribute):
        p = self.schema[entity.upper()][attribute.upper()]
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
        predicateName = self.getSplitPredName(entity)
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

    def bind2db(self, dbInfo):
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

    # Insert to kb and db
    def insert(self, entity, data, toDb=True):
        p = self.mergedPreds[entity]
        for d in data:
            self.kb.add(p(*d))
        if toDb:
            self.db.insert(entity, data)

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

    def getForeignPath(self, e1, e2):
        invPath = []
        joins = self.joins[e1.upper()]
        d = e2.upper()
        while (True):
            for j in joins:
                if j[2] == d:
                    invPath.append(d)
                    d = j[0]
            if d == e1.upper():
                invPath.reverse()
                return invPath
            if not len(invPath):
                joins = self.joins[e2.upper()]
                d = e1.upper()
                while (True):
                    for j in joins:
                        if j[2] == d:
                            invPath.append(d)
                            d = j[0]
                    if d == e2.upper():
                        invPath.reverse()
                        return invPath
                    if not len(invPath):
                        print(f'Entities {e1}, {e2} are not connected.\n')
                        return False

    def isForeign(self, e, a):
        if len(self.schema[e][a]) == 4:
            return True
        else:
            return False

    def getForeign(self, e1, e2):
        fs = None
        fd = None
        for j in self.joins[e1]:
            if e2 == j[2] and j[1] in self.schema[e1] and self.isForeign(e1, j[1]):
                fs = (e1, j[1])
                fd = (e2, j[3])
        if not fs and not fd:
            for j in self.joins[e2]:
                if e1 == j[2] and j[1] in self.schema[e2] and self.isForeign(e2, j[1]):
                    fs = (e2, j[1])
                    fd = (e1, j[3])
        if not fs and not fd:
            print(
                f'Unable to join the entities {e1} and {e2} directly. No foreign key found.')
            return False
        return (fs, fd)

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

    # Select from kb also delete the data csvs in dataModel
    def select(self, ent, cond=None, order=None, pOut=False, getQuery=False):
        jent = [e.upper() for e in ent]
        jlst = []
        if cond:
            for e in cond:
                for je in list(ent):
                    path = self.getForeignPath(je, e)
                    if path:
                        for p in path:
                            if p not in jent:
                                jent.append(p)
        for e1 in jent:
            for e2 in list(jent)[list(jent).index(e1) + 1:]:
                j = self.getJoinPreds(e1, e2)
                if (j):
                    jlst.append(j[0] == j[1])

        if type(ent).__name__ == 'dict':
            entpred = [self.mergedPreds[e.upper()] for e in jent]
            if pOut:
                outPreds = [self.createMergedPred(e, ent[e]) for e in ent]
            attrlst = [getattr(self.mergedPreds[e.upper()],
                               a.lower()) for e in ent for a in ent[e]]

        elif type(ent).__name__ == 'list':
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
            if type(order).__name__ != 'dict':
                print(
                    'Wrong input format for order attributes. Dictionary input required.')
                return False
            ordlst = [getattr(self.mergedPreds[e.upper()], o.lower())
                      for e in order for o in order[e]]
            query = query.order_by(*ordlst)

        pdata = list(query.all())
        outdata = []

        if pOut and type(ent).__name__ == 'dict':
            eind = []
            so = 0
            for e in ent:
                eind.append(so)
                s = so + len(ent[e])
                so = s
            eind.append(s)
            for pd in pdata:
                for e in ent:
                    ei = eind[list(ent).index(e)]
                    outdata.append(outPreds[ei](
                        *pd[eind[ei]:eind[ei+1]]))
            return outdata

        else:
            outdata = pdata.copy()

        if getQuery:
            qout = copy(query)
            return outdata, qout
        else:
            return outdata

    def cascade(self):
        pass

    def extract(self, ent, cond=None, order=None):
        outKB = FactBase()
        preds = self.select(ent, cond=cond, order=order, pOut=True)
        for p in preds:
            outKB.add(p)
        return outKB

    # Update to kb and db
    def update(self, ent, val, cond=None, toDb=True):
        # Update to kb
        if type(ent).__name__ == 'tuple' or type(ent).__name__ == 'list':
            entpred = [self.mergedPreds[e.upper()] for e in ent]
        else:
            entpred = self.mergedPreds[ent.upper()]
        for e in ent:
            if e in val:
                mpreds, qout = self.select(
                    ent, cond=cond, outPreds=[e], getQuery=True)
                qout.delete()
                for m in mpreds:
                    m = m.clone(**val[e])
                    self.kb.add(m)

        # Update to db
        if toDb:
            self.db.update([e.upper() for e in ent], cond, val)
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

    def toFile(self, path, format='lp', entities=None):
        filename = path + self.name.lower() + '.' + format
        f = open(filename, "w")
        if entities:
            subKb = self.extract(entities)
            content = FactBase.asp_str(subKb)
        else:
            content = FactBase.asp_str(self.kb)
        f.write(content)
        f.close()

    def split(self, mergedPred):
        entity = type(mergedPred).__name__.upper()
        attributes = [attr for attr in dir(self.splitPreds[mergedPred]) if callable(
            getattr(self.splitPreds[mergedPred], attr)) and not (attr.startswith("__") or attr.startswith("_")) and attr not in ['Field', 'clone', 'sign']]
        data = [getattr(mergedPred, attr) for attr in attributes]
        outPreds = []
        for a in attributes:
            if self.isPrimary(entity, a):
                p = self.splitPreds[entity.upper()]
            else:
                p = self.splitPreds[a.upper()]
            for d in data:
                if self.schema[entity.upper()][a.upper()][0] != 'boolean' or self.schema[entity.upper()][a.upper()][0] == 'boolean' and d[attributes.index(a)]:
                    pv = self.getPrimaryData(entity, attributes, d)
                    v = pv.copy()
                    if not self.isPrimary(entity, a) and not self.schema[entity.upper()][a.upper()][0] == 'boolean':
                        v.append(d[attributes.index(a)])
                    outPreds.append(p(*v))
        return outPreds

    def merge(self, splitPreds):
        pname = None
        content = {}
        data = []
        for sp in splitPreds:
            attributes = [attr for attr in dir(self.splitPreds[sp]) if callable(
                getattr(self.splitPreds[sp], attr)) and not (attr.startswith("__") or attr.startswith("_")) and attr not in ['Field', 'clone', 'sign']]
            if len(attributes) == 1:
                pname = type(sp).__name__
                aname = attributes[0]
            else:
                aname = type(sp).__name__.lower()
            data = getattr(sp, aname)
            t = type(data)
            field = self.TYPE2FIELD[t]
            content[aname] = field
            data.append(getattr(sp, aname))
        if not pname:
            print(' Unable to merge predicates. No primary key found')
            return False
        else:
            mergedPred = type(
                pname[0].upper + pname[1:].lower(), (Predicate, ), content)
            return mergedPred(*data)


def main():
    # TODO finish select and update
    # TODO tomorrow finish update(cascade) and delete
    # TODO Sunday finish db update and run with split and merge
    # TODO Monday create tests per specialty per doctor and general
    # TODO Tuesday create a simple html request interface
    # TODO Wednesday final changes and comments
    # TODO Thursday Τα έγγραφα του Τσάμπρα
    # TODO 13 - 17 paper

    dbConditions = {'TIMESLOT': {
        "TIMESLOT_AVAILABLE": ['boolean', [('=', True)]]}, 'SPECIALTY': {
        "TITLE": ['text', [('=', 'Preventive_medicine')]]}}
    db_info = ['kanon2000', 'nhs', 'kanon2000']
    kb = KnowledgeBase('NHS_APPOINTMENTS', schema,
                       dbInfo=db_info, dbConditions=dbConditions)
    # kb.showPreds()
    data = kb.select({'Request': ['id'], 'Doctor': ['id']}, cond={
        'Doctor': {'id': [('=', '04099610232')]}}, order={'Request': ['id']}, pOut=True)
    # data = kb.select(['Request', 'Doctor'], cond={
    #     'Doctor': {'id': [('=', '04099610232')]}}, order={'Request': ['id']}, pOut=True)
    # newKB = kb.extract(['Request', 'Doctor'], cond={
    #     'Doctor': {'id': [('=', '04099610232')]}})
    newKB = kb.extract({'Request': ['id'], 'Doctor': ['id']}, cond={
        'Doctor': {'id': [('=', '04099610232')]}}, order={'Request': ['id', 'timeslot_id']})

    print(FactBase.asp_str(newKB))
    # kb.update(('Person', 'Doctor'), val={'Doctor': {'id': '1234'}}, cond={
    #     'Doctor': {'doctor_available': [('=', True)]}})
    kb.toFile('clingo/')

    # print(data)


if __name__ == "__main__":
    main()
