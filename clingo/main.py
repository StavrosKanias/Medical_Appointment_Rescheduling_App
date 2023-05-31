from clorm import IntegerField, Predicate
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

    class Claimed(Predicate):
        request = IntegerField

    merged = input(" Do you want to run in merged mode?\n y/n:\n")
    if merged == 'y':
        class Grant(Predicate):
            request = IntegerField
            score = IntegerField

        subKB = {'REQUEST': ['ID', 'PATIENT_ID',
                             'TIMESLOT_ID', 'SCORE', 'STATUS'], 'TIMESLOT': ['ID', 'DOCTOR_ID'], 'DOCTOR': ['ID', 'SPECIALTY_TITLE']}
        solution = kb.run('clingo/reschedulers/reschedulerMergedGrant.lp',
                          [Grant, Claimed], searchDuration=12, show=True, subKB=[subKB, True], strOut=False)

    else:
        class Grant(Predicate):
            request = IntegerField

        subKB = {'REQUEST': ['PATIENT_ID',
                             'TIMESLOT_ID', 'SCORE', 'STATUS'], 'TIMESLOT': ['DOCTOR_ID'], 'DOCTOR': ['SPECIALTY_TITLE']}
        solution = kb.run('clingo/reschedulers/reschedulerGrant.lp',
                          [Grant, Claimed], searchDuration=12, show=True, subKB=[subKB, False], strOut=False)
    update = {}
    granted = {x.request: 1 for x in solution['Grant']}
    claimed = {x.request: 1 for x in solution['Claimed']}
    granted.update(claimed)
    update.update(granted)
    requestIDs = [x[0]
                  for x in kb.select('Request', attributes=['ID'], order='ID')]
    waiting = {x: 0 for x in list(set(requestIDs) ^ set(granted))}
    update.update(waiting)
    update = {x: update[x] for x in sorted(update)}

    for u in update:
        kb.update('REQUEST', conditions={'ID': [
                  ('=', u)]}, values={'STATUS': update[u]}, toDb=False)


# TODO: Implement cascade in deletion


if __name__ == "__main__":
    main()
