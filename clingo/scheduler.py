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


def main():
    # TODO Change that
    dbConditions = {'TIMESLOT': {
        "TIME": [('>', '09:00:00')], "DATE": [('>', '+0')]}}
    db_info = ['kanon2000', 'nhs', 'kanon2000']
    kb = KnowledgeBase('NHS_APPOINTMENTS', schema,
                       dbInfo=db_info, dbConditions=dbConditions)
    # kb.toFile('clingo/')
    # solution = kb.run('clingo/scheduler.lp')
    # print(solution)
    # TODO
    kb.delete('Timeslot', conditions={
        "ID": [('>=', 3)]})
    print(kb.select('Timeslot'))


if __name__ == "__main__":
    main()
