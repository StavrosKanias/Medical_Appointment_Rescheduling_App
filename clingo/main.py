from clorm import StringField, IntegerField, Predicate
from knowledgeBase import KnowledgeBase
from datetime import date
import sys
sys.path.append('./db')  # nopep8
from dbCreator import schema


def main():
    dbConditions = {'TIMESLOT': {
        "DATE": ['date', [('>', '+0')]], "TIMESLOT_AVAILABLE": ['boolean', [('=', True)]]}}
    db_info = ['kanon2000', 'nhs', 'kanon2000']
    kb = KnowledgeBase('NHS_APPOINTMENTS', schema,
                       dbInfo=db_info, dbConditions=dbConditions)
    kb.delete('REQUEST', conditions={
        "ID": [('=', 2391)]}, fromDb=False)
    kb.toFile('clingo/')

    class Grant(Predicate):
        request = IntegerField

    class Claimed(Predicate):
        request = IntegerField

    subKB = {'REQUEST': ['PATIENT_ID',
                         'TIMESLOT_ID', 'SCORE', 'STATUS']}
    # Add split data parameter in run. Run seperately for each specialty
    solution = kb.run('clingo/reschedulers/reschedulerGrant.lp',
                      [Grant, Claimed], searchDuration=12, show=True, subKB=[subKB, False], strOut=False)

    # class Grant(Predicate):
    #     request = IntegerField
    #     score = IntegerField

    # subKB = {'REQUEST': ['ID', 'PATIENT_ID',
    #                      'TIMESLOT_ID', 'SCORE', 'STATUS']}
    # solution = kb.run('clingo/reschedulers/reschedulerMergedGrant.lp',
    #                   [Grant, Claimed], searchDuration=12, show=True, subKB=[subKB, True], strOut=False)

    granted = set({x.request: 1 for x in solution['Grant']})
    claimed = set({x.request: 1 for x in solution['Claimed']})
    assigned = granted.union(claimed)
    requestIDs = set([x[0]
                      for x in kb.select('Request', attributes=['ID'], order='ID')])
    waiting = set({x: 0 for x in list(requestIDs ^ assigned)})
    update = sorted(dict((assigned | waiting)).items())

    for u in update:
        kb.update('REQUEST', conditions={'ID': [
                  ('=', u)]}, values={'STATUS': update[u]}, toDb=True)

# TODO
# Fix update for primary and implement cascade in deletion


if __name__ == "__main__":
    main()
