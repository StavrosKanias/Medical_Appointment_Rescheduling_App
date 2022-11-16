from db import DataModel
from fabricate_data import DataFabricator


def main():
    # capitalize

    schema = {
        'PERSON': {"SSN": ['text', True], "FIRSTNAME": ['text', False], "LASTNAME": ['text', False],
                   "PHONE": ['text', False], "EMAIL": ['text', False], "BIRTH_DATE": ['date', False]},

        'SPECIALTY': {"TITLE": ['text', True]},

        'DOCTOR': {"ID": ['text', True, 'PERSON', 'SSN'], "UPIN": ['text', False, True],
                   "AVAILABILITY": ['boolean', False], "SPECIALTY_TITLE": ['text', False, 'SPECIALTY', 'TITLE']},

        'PATIENT': {"ID": ['text', True, 'PERSON', 'SSN'], "PRIORITY": ['integer', False]},

        'TIMESLOT': {"ID": ['integer', True], "DATE": ['date', False], "TIME": ['time', False], "AVAILABILITY": ['boolean', False], "DOCTOR_ID": ['text', False, 'DOCTOR', 'ID']},

        'REQUEST': {"ID": ['integer', True], "PATIENT_ID": ['text', False, 'PATIENT', 'ID'], "TIMESLOT_ID": ['integer', False, 'TIMESLOT', 'ID'],
                    "PREFERENCE": ['integer', False], "STATUS": ['integer', False]}  # Add "SCORE": ['float', False] to make the scores visible
    }

    d = DataModel('kanon2000', 'nhs', 'kanon2000', schema)
    d.dropTables()
    d.createTables()
    inp = input(" Do you want to fabricate new test data?\n y/n:")
    if inp == 'y':
        fab = DataFabricator(schema, 500, 1000, 5, 10, 10, 20, 2.5)
        for e in schema.keys():
            fab.fabricate(e)
        if d.isEmpty():
            d.loadTestData()
        else:
            d.dropData()
            d.loadTestData()

    d.close()


if __name__ == "__main__":
    main()
