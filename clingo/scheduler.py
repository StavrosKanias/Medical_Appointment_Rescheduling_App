from clorm import StringField, IntegerField, Predicate
from knowledgeBase import KnowledgeBase
from datetime import date

schema = {
    'DOCTOR': {"ID": ['text', 'primary'], "DOCTOR_AVAILABLE": ['boolean'],
               "SPECIALTY_TITLE": ['text']},

    'PATIENT': {"ID": ['text', 'primary'], "PRIORITY": ['integer']},

    'TIMESLOT': {"ID": ['integer', 'primary'], "DATE": ['date'], "TIME": ['time'],
                 "TIMESLOT_AVAILABLE": ['boolean'], "DOCTOR_ID": ['text']},


    'REQUEST': {"ID": ['integer', 'primary'], "PATIENT_ID": ['text'],
                "TIMESLOT_ID": ['integer'], "PREFERENCE": ['integer'],
                "SCORE": ['integer'], "STATUS": ['integer']}
}


class Assign(Predicate):
    patient = StringField
    timeslot = IntegerField


def main():
    dbConditions = {'TIMESLOT': {
        "TIME": [('>', '09:00:00')], "DATE": [('>', '+0')]}}
    db_info = ['kanon2000', 'nhs', 'kanon2000']
    kb = KnowledgeBase('NHS_APPOINTMENTS', schema,
                       dbInfo=db_info, dbConditions=dbConditions)
    kb.toFile('clingo/')
    kb.delete('Request', conditions={
        "ID": [('=', 1)]})
    solution = kb.run('clingo/scheduler.lp')
    print(list(solution.query(Assign).all()))

    # kb.reload()
    # print(kb)
    # kb.update('Request', conditions={"ID": [('=', 2)]}, values={
    #           "STATUS": 1}, toDb=False)


if __name__ == "__main__":
    main()
