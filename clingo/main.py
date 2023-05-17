from clorm import StringField, IntegerField, Predicate
from knowledgeBase import KnowledgeBase
from datetime import date
import sys
sys.path.append('./db')  # nopep8
from dbCreator import schema

# reducedSchema = {
#     'REQUEST': {"ID": ['integer', True], "PATIENT_ID": ['text', False, 'PATIENT', 'ID'],
#                 "TIMESLOT_ID": ['integer', False, 'TIMESLOT', 'ID'], "SCORE": ['integer', False], "STATUS": ['integer', False]}
# }


def main():
    dbConditions = {'TIMESLOT': {
        "DATE": ['date', [('>', '+0')]], "TIMESLOT_AVAILABLE": ['boolean', [('=', True)]]}}
    db_info = ['kanon2000', 'nhs', 'kanon2000']
    kb = KnowledgeBase('NHS_APPOINTMENTS', schema,
                       dbInfo=db_info, dbConditions=dbConditions)
    # kb.fb = kb.extract({'TIMESLOT': ['ID', 'DATE']}, merge=True)
    # # for i in range(1000, 1200):
    # kb.delete('REQUEST', conditions={
    #     "ID": [('=', 3)]})
    kb.toFile('clingo/')

    # class Assign(Predicate):
    #     patient = StringField
    #     timeslot = IntegerField
    #     request = IntegerField

    # solutionAssign = kb.run('clingo/finalRescheduler.lp',
    #                         [Assign], show=True, searchDuration=12)

    class Grant(Predicate):
        request = IntegerField

    class Claimed(Predicate):
        request = IntegerField

    subKB = {'REQUEST': ['PATIENT_ID',
                         'TIMESLOT_ID', 'SCORE', 'STATUS']}
    # Add split data parameter in run. Run seperately for each specialty
    solutionGrant = kb.run('clingo/reschedulers/simplifiedReschedulerGrant.lp',
                           [Grant, Claimed], searchDuration=12, show=True, subKB=[subKB, False], strOut=False)

    subKB = {'REQUEST': ['ID', 'PATIENT_ID',
                         'TIMESLOT_ID', 'SCORE', 'STATUS']}
    # solutionMergedGrant = kb.run('clingo/reschedulers/reschedulerMergedGrant.lp',
    #                              [Grant, Claimed], searchDuration=12, show=True, subKB=[subKB, True], strOut=False)
    # # solution2 = kb.run('clingo/scheduler.lp',
    # #                    [Assign], show=True)
    # assigned = {x.request: 1 for x in solution1['Assign']}
    # requestIDs = [x[0]
    #               for x in kb.select('Request', attributes=['ID'], order='ID')]
    # waiting = {x: 0 for x in list(set(requestIDs) ^ set(assigned))}
    # update = dict(sorted((assigned | waiting).items()))

    # for u in update:
    #     kb.update('REQUEST', conditions={'ID': [
    #               ('=', u)]}, values={'STATUS': update[u]}, toDb=True)

    # print(solution)
    # kb.reload()
    # print(kb)
    # kb.update('Request', conditions={"ID": [('=', 2)]}, values={
    #           "STATUS": 1}, toDb=False)

# TODO
    # Fix update for primary and implement cascade in deletion


if __name__ == "__main__":
    main()
