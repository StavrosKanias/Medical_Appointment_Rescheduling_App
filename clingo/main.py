from clorm import IntegerField, Predicate
from knowledgeBase import KnowledgeBase
import sys
sys.path.append('./db')  # nopep8
from dbCreator import schema


def main():

    dbConditions = {'TIMESLOT': {
        "DATE": [('>', '+0')], "TIMESLOT_AVAILABLE": [('=', True)]}}
    db_info = ['Kanon2000', 'nhs', 'Kanon2000']
    kb = KnowledgeBase('NHS_APPOINTMENTS', schema,
                       dbInfo=db_info, dbConditions=dbConditions)

    kb.toFile('clingo/')

    class Claimed(Predicate):
        request = IntegerField

    kb.delete(['Request'], cond={'Request': {'id': [('=', 1)]}})

    merged = input(" Do you want to run in merged mode?\n y/n:\n")
    if merged == 'y':
        class Grant(Predicate):
            request = IntegerField
            score = IntegerField

        subKB = {'REQUEST': ['ID', 'PATIENT_ID',
                             'TIMESLOT_ID', 'SCORE', 'STATUS'], 'TIMESLOT': ['ID', 'DOCTOR_ID'], 'DOCTOR': ['ID', 'SPECIALTY_TITLE']}
        solution = kb.run('clingo/reschedulers/reschedulerMergedGrant.lp',
                          [Grant, Claimed], searchDuration=12, show=True, subKB=subKB, merged=True)

    else:
        class Grant(Predicate):
            request = IntegerField

        subKB = {'REQUEST': ['PATIENT_ID', 'TIMESLOT_ID', 'SCORE', 'STATUS'], 
                 'TIMESLOT': ['DOCTOR_ID'], 'DOCTOR': ['SPECIALTY_TITLE']}
        solution = kb.run('clingo/reschedulers/reschedulerGrant.lp',
                          [Grant, Claimed], searchDuration=12, show=True, subKB=subKB,  merged=False)
    # if solution:
    #     update = {}
    #     granted = {x.request: 1 for x in solution['Grant']}
    #     claimed = {x.request: 1 for x in solution['Claimed']}
    #     granted.update(claimed)
    #     update.update(granted)
    #     requestIDs = [x for x in kb.select(
    #         {'Request': ['id']}, order={'Request': ['id']})]
    #     waiting = {x: 0 for x in list(set(requestIDs) ^ set(granted))}
    #     update.update(waiting)
    #     update = {x: update[x] for x in sorted(update)}

    #     for u in update:
    #         kb.update({'REQUEST': {'STATUS': update[u]}}, cond={'REQUEST': {'ID': [
    #             ('=', u)]}}, toDb=False)


# TODO Last feature take into account cyclical dependencies in dataModel
if __name__ == "__main__":
    main()
