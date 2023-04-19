from clorm import StringField, IntegerField, Predicate
from knowledgeBase import KnowledgeBase
from datetime import date

schema = {
    # # 'DOCTOR': {"ID": ['text', 'primary'], "DOCTOR_AVAILABLE": ['boolean'],
    # #            "SPECIALTY_TITLE": ['text']},

    # 'PATIENT': {"ID": ['text', 'primary'], "PRIORITY": ['integer']},

    # # 'TIMESLOT': {"ID": ['integer', 'primary'], "DATE": ['date'], "TIME": ['time'],
    # #               "TIMESLOT_AVAILABLE": ['boolean'], "DOCTOR_ID": ['text']},

    # 'TIMESLOT': {"ID": ['integer', 'primary']},


    # # 'REQUEST': {"ID": ['integer', 'primary'], "PATIENT_ID": ['text'],
    # #             "TIMESLOT_ID": ['integer'], "PREFERENCE": ['integer'],
    # #             "SCORE": ['integer'], "STATUS": ['integer']}

    'REQUEST': {"ID": ['integer', 'primary'], "PATIENT_ID": ['text'],
                "TIMESLOT_ID": ['integer'], "SCORE": ['integer'], "STATUS": ['integer']}
}


def main():
    dbConditions = {'TIMESLOT': {
        "TIME": ['time', [('>', '+0')]], "DATE": ['date', [('>', '+0')]], "TIMESLOT_AVAILABLE": ['boolean', [('=', True)]]}}
    dbConditions = {}
    db_info = ['kanon2000', 'nhs', 'kanon2000']
    kb = KnowledgeBase('NHS_APPOINTMENTS', schema,
                       dbInfo=db_info, dbConditions=dbConditions)
    kb.delete('REQUEST', conditions={
        "ID": [('=', 1)]})
    kb.toFile('clingo/')
    # 3. Fix update for primary and implement cascade in deletion
    # 1. Rewrite the kb and the code simplified for the base scenario
    # 2. Create a method to merge predicated from existing data and create the simplified kb automtically
    # class Assign(Predicate):
    #     patient = StringField
    #     timeslot = IntegerField
    #     request = IntegerField

    # solution1 = kb.run('clingo/rescheduler.lp',
    #                    [Assign], show=True)
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

    # TODO fix primary choice for booleans, after that doctor on strike and multiple chains
    # print(solution)
    # kb.reload()
    # print(kb)
    # kb.update('Request', conditions={"ID": [('=', 2)]}, values={
    #           "STATUS": 1}, toDb=False)


if __name__ == "__main__":
    main()
