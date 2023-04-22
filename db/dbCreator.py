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

# ----------------- DEFAULT DATA ----------------------------------
person = ['PERSON', [{"SSN": '21088977182', "FIRSTNAME": "Dimitrios", "LASTNAME": "Dimitriou",
                      "PHONE": '6978909873', "EMAIL": "dimitriosdimitriou@gmail.com", "BIRTH_DATE": "1985-05-05"},
                     {"SSN": '21088977183', "FIRSTNAME": "Nikos", "LASTNAME": "Nikou",
                      "PHONE": "6978909845", "EMAIL": "nikosnikou@gmail.com", "BIRTH_DATE": "1990-05-05"},
                     {"SSN": "21088977184", "FIRSTNAME": "Giorgos", "LASTNAME": "Georgiou",
                      "PHONE": "6978909846", "EMAIL": "giogrosgeorgiou@gmail.com", "BIRTH_DATE": "1991-05-05"},
                     {"SSN": "21088977185", "FIRSTNAME": "Ioannis", "LASTNAME": "Ioannou",
                      "PHONE": "6978903456", "EMAIL": "ioannisioannou@gmail.com", "BIRTH_DATE": "1992-05-05"},
                     {"SSN": "21088977186", "FIRSTNAME": "Maria", "LASTNAME": "Marietti",
                      "PHONE": "6945609846", "EMAIL": "mariamarietti@gmail.com", "BIRTH_DATE": "1995-05-05"},
                     {"SSN": "21088977187", "FIRSTNAME": "Despoina", "LASTNAME": "Panagiotopoulou",
                      "PHONE": "6972349846", "EMAIL": "despoinapanagiotopoulou@gmail.com", "BIRTH_DATE": "1997-05-05"},
                     {"SSN": "21088977188", "FIRSTNAME": "Kostas", "LASTNAME": "Karakostas",
                      "PHONE": "6972747848", "EMAIL": "kostaskarakostas@gmail.com", "BIRTH_DATE": "1992-05-05"}]]

specialty = ['SPECIALTY', [{"TITLE": 'Cardiology'}]]

doctor = ['DOCTOR', [{"ID": "21088977182", "UPIN": 'cfnasnd123',
                      "DOCTOR_AVAILABLE": 1, "SPECIALTY_TITLE": 'Cardiology'}]]

patient = ['PATIENT', [{"ID": "21088977183", "PRIORITY": 85},
                       {"ID": '21088977184', "PRIORITY": 81},
                       {"ID": '21088977185', "PRIORITY": 43},
                       {"ID": '21088977186', "PRIORITY": 36},
                       {"ID": '21088977187', "PRIORITY": 57},
                       {"ID": '21088977188', "PRIORITY": 80}]]

timeslot = ['TIMESLOT', [
    {"ID": 1, "DATE": "2023-05-01", "TIME": "09:00:00",
     "TIMESLOT_AVAILABLE": 1, "DOCTOR_ID": "21088977182"},
    {"ID": 2, "DATE": "2023-05-02", "TIME": "13:00:00",
     "TIMESLOT_AVAILABLE": 1, "DOCTOR_ID": "21088977182"},
    {"ID": 3, "DATE": "2023-05-04", "TIME": "11:00:00",
     "TIMESLOT_AVAILABLE": 1, "DOCTOR_ID": "21088977182"},
    {"ID": 4, "DATE": "2023-05-05", "TIME": "15:00:00",
     "TIMESLOT_AVAILABLE": 1, "DOCTOR_ID": "21088977182"}]]


def score(preference, priority):
    return round(30 * 1/preference + 70 * (priority/100))


request = ['REQUEST', [{"ID": 1, "PATIENT_ID": '21088977183', "TIMESLOT_ID": 1,
                        "PREFERENCE": 1, "SCORE": 90, "STATUS": 1},
                       {"ID": 2, "PATIENT_ID": '21088977184', "TIMESLOT_ID": 1,
                        "PREFERENCE": 1, "SCORE": 87, "STATUS": 0},
                       {"ID": 3, "PATIENT_ID": '21088977188', "TIMESLOT_ID": 1,
                        "PREFERENCE": 1, "SCORE": 86, "STATUS": 0},
                       {"ID": 4, "PATIENT_ID": '21088977184', "TIMESLOT_ID": 2,
                        "PREFERENCE": 2, "SCORE": 72, "STATUS": 1},
                       {"ID": 5, "PATIENT_ID": '21088977185', "TIMESLOT_ID": 2,
                        "PREFERENCE": 1, "SCORE": 60, "STATUS": 0},
                       {"ID": 6, "PATIENT_ID": '21088977186', "TIMESLOT_ID": 2,
                        "PREFERENCE": 1, "SCORE": 55, "STATUS": 0},
                       {"ID": 7, "PATIENT_ID": '21088977185', "TIMESLOT_ID": 3,
                        "PREFERENCE": 2, "SCORE": 45, "STATUS": 1},
                       {"ID": 8, "PATIENT_ID": '21088977186', "TIMESLOT_ID": 3,
                        "PREFERENCE": 1, "SCORE": 40, "STATUS": 0},
                       {"ID": 9, "PATIENT_ID": '21088977188', "TIMESLOT_ID": 4,
                        "PREFERENCE": 2, "SCORE": 71, "STATUS": 1},
                       {"ID": 10, "PATIENT_ID": '21088977187', "TIMESLOT_ID": 4,
                        "PREFERENCE": 1, "SCORE": 70, "STATUS": 0}]]

data = [person, specialty, doctor, patient, request, timeslot]


def main():

    db = DataModel('kanon2000', 'nhs', 'kanon2000', schema)

    new_db = input(" Do you want to recreate the database?\n y/n:")
    if new_db == 'y':
        db.dropTables()
        db.createTables()

    new_data = input(" Do you want to fabricate new test data?\n y/n:\n")
    if new_data == 'y':
        # minimum_people = int(input(" Minimum number of people: \n"))
        # maximum_people = int(input(" Maximum number of people: \n"))
        # minimum_doctors = int(input(" Minimum number of doctors: \n"))
        # maximum_doctors = int(input(" Maximum number of doctors: \n"))
        # minimum_specialites = int(input(" Minimum number of specialites: \n"))
        # maximum_specialites = int(input(" Maximum number of specialites: \n"))
        # demand = float(input(" Demand (load factor): \n"))
        # timeslot_availability = float(input(" Timeslot availability: \n"))

        minimum_people = 200
        maximum_people = 300
        minimum_doctors = 10
        maximum_doctors = 20
        minimum_specialites = 10
        maximum_specialites = 15
        demand = 2.5
        timeslot_availability = 0.8

        fab = DataFabricator(schema, minimum_people,
                             maximum_people, minimum_doctors, maximum_doctors, minimum_specialites, maximum_specialites, demand, timeslot_availability, seed=1)
        for e in schema.keys():
            fab.fabricate(e)
        if db.isEmpty():
            db.loadTestData()
        else:
            db.dropData()
            db.loadTestData()
    else:
        print('Using default data\n')
        fab = DataFabricator()
        for d in data:
            fab.write_to_csv(d[0], schema[d[0]], d[1])
        if db.isEmpty():
            db.loadTestData()
        else:
            db.dropData()
            db.loadTestData()

    db.close()


if __name__ == "__main__":
    main()
