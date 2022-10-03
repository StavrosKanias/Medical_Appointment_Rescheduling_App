from db import DataModel
from fabricate_data import DataFabricator


def main():

    schema = {
        'Person': {"SSN": ['text', True], "Firstname": ['text', False], "Surname": ['text', False],
                   "Phone": ['text', False], "Email": ['text', False], "Birth_date": ['date', False]},

        'Specialty': {"Title": ['text', True]},

        'Doctor': {"Doctor_ID": ['text', True, 'Person', 'SSN'], "UPIN": ['text', False, True],
                   "Availability": ['boolean', False], "Specialty": ['text', False, 'Specialty', 'Title']},

        'Patient': {"Patient_ID": ['text', True, 'Person', 'SSN'], "Priority": ['float', False]},

        'Timeslot': {"Datetime": ['timestamp', True], "Status": ['boolean', False], "Doctor": ['text', False, 'Doctor', 'Doctor_ID']},

        'Chooses': {"Choice_ID": ['integer', True], "Patient": ['text', False, 'Patient', 'Patient_ID'], "Timeslot": ['timestamp', False, 'Timeslot', 'Datetime'],
                    "Preference": ['integer', False]}
    }

    d = DataModel('kanon2000', 'nhs', 'kanon2000', schema)
    d.dropTables()
    d.createTables()
    inp = input(" Do you want to fabricate new test data?\n y/n:")
    if inp == 'y':
        fab = DataFabricator(schema, 10000, 40000, 50, 2100, 5, 15, 1.5)
        for e in list(schema.keys()):
            fab.fabricate(e)
        d.dropData()
        d.loadTestData()

    elif d.isEmpty():
        for i in list(schema.keys()):
            f = open('{}.csv'.format(i), 'w')
            f.close()
            d.loadTestData()
    else:
        d.loadTestData()


if __name__ == "__main__":
    main()
