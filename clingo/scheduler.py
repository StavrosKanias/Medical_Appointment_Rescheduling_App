from knowledgeBase import KnowledgeBase
import sys
sys.path.append('./db')  # nopep8
from dbCreator import schema
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
    dbConditions = {'TIMESLOT': {"DATE": ['>+0'], "TIME": ['>+0']}}
    db_info = ['kanon2000', 'nhs', 'kanon2000']
    kb = KnowledgeBase('NHS_APPOINTMENTS', schema,
                       dbInfo=db_info, dbConditions=dbConditions)
    kb.toFile('clingo/')
    # TODO
    #print(kb.select('Timeslot', conditions={'Timeslot': [('ID', '>10')]}))


if __name__ == "__main__":
    main()
