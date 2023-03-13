from dataModel import DataModel
from dataFabricator import DataFabricator

schema = {
    'PERSON': {"SSN": ['text', True], "FIRSTNAME": ['text', False], "LASTNAME": ['text', False],
               "PHONE": ['text', False], "EMAIL": ['text', False], "BIRTH_DATE": ['date', False]},

    'SPECIALTY': {"TITLE": ['text', True]},

    'DOCTOR': {"ID": ['text', True, 'PERSON', 'SSN'], "UPIN": ['text', False, True],
               "DOCTOR_AVAILABLE": ['boolean', False], "SPECIALTY_TITLE": ['text', False, 'SPECIALTY', 'TITLE']},

    'PATIENT': {"ID": ['text', True, 'PERSON', 'SSN'], "PRIORITY": ['integer', False]},

    'TIMESLOT': {"ID": ['integer', True], "DATE": ['date', False], "TIME": ['time', False], "TIMESLOT_AVAILABLE": ['boolean', False], "DOCTOR_ID": ['text', False, 'DOCTOR', 'ID']},

    'REQUEST': {"ID": ['integer', True], "PATIENT_ID": ['text', False, 'PATIENT', 'ID'], "TIMESLOT_ID": ['integer', False, 'TIMESLOT', 'ID'],
                "PREFERENCE": ['integer', False], "SCORE": ['integer', False], "STATUS": ['integer', False]}  # The "SCORE" must be rounded to an int because clingo does not support floats yet
}


def main():

    d = DataModel('kanon2000', 'nhs', 'kanon2000', schema)

    new_db = input(" Do you want to recreate the database?\n y/n:")
    if new_db == 'y':
        d.dropTables()
        d.createTables()

    new_data = input(" Do you want to fabricate new test data?\n y/n:\n")
    if new_data == 'y':
        minimum_people = int(input(" Minimum number of people: \n"))
        maximum_people = int(input(" Maximum number of people: \n"))
        minimum_doctors = int(input(" Minimum number of doctors: \n"))
        maximum_doctors = int(input(" Maximum number of doctors: \n"))
        minimum_specialites = int(input(" Minimum number of specialites: \n"))
        maximum_specialites = int(input(" Maximum number of specialites: \n"))
        demand = float(input(" Demand (load factor): \n"))
        timeslot_availability = float(input(" Timeslot availability: \n"))
        # 500, 1000, 5, 10, 10, 20, 2.5, 0.95
        fab = DataFabricator(schema, minimum_people,
                             maximum_people, minimum_doctors, maximum_doctors, minimum_specialites, maximum_specialites, demand, timeslot_availability)
        for e in schema.keys():
            fab.fabricate(e)
        if d.isEmpty():
            d.loadTestData()
        else:
            d.dropData()
            d.loadTestData()
    else:
        # d = Doctor('a', 'b', 1, 'c')
        # p = Patient('a', 1)
        # pe = Person('a', 's', 'k', '1412321', 'agsgacgz', datetime.date(2023, 1, 1))
        # r = Request(1, 'asd', 1, 1, 1, 1)
        # s = Specialty('lalala')
        # t = Timeslot(1, datetime.date(2023, 10, 12), datetime.time(), 1, 'a')
        doctor = ['DOCTOR', [{'aaaaaaa', 'ababababa', 1, 'Ortho'}]]
        patient = ['PATIENT', [{'bbbbb', 50}, {
            'ccccc', 60}, {'ddddd', 70}, {'eeeee', 80}]]
        request = ['REQUEST', [{1, 'bbbbb', 1, 1, 65, 0}, {
            2, 'ccccc', 1, 1, 65, 0}, {3, 'ddddd', 1, 1, 65, 0}]]
        timeslot = ['TIMESLOT', [{}]]
        data = [doctor, patient, request, timeslot]
        print('Using default data\n')
        for d in data:
            DataFabricator.write_to_csv(d[0], schema[d[0]], d[1])
        if d.isEmpty():
            d.loadTestData()
        else:
            d.dropData()
            d.loadTestData()

    d.close()


if __name__ == "__main__":
    main()
