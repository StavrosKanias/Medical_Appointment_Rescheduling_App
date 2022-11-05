import pandas as pd
import csv
import string
import random
from faker import Faker
from unidecode import unidecode
from datetime import date, timedelta


class DataFabricator():
    def __init__(self, schema, minP, maxP, minD, maxD, minS, maxS, demand):
        self.schema = schema
        self.quantities = {}
        self.quantities['Person'] = random.randint(minP, maxP)
        self.quantities['Doctor'] = random.randint(minD, maxD)
        self.quantities['Patient'] = self.quantities['Person'] - \
            self.quantities['Doctor']
        self.quantities['Specialty'] = random.randint(minS, maxS)
        # 8 timeslots per day, per doctor for 5 days a week for 3 months
        self.quantities['Timeslot'] = int(
            self.quantities['Doctor'] * 8 * 5 * 12)
        self.quantities['Requests'] = int(
            demand * self.quantities['Timeslot'])
        for i in self.quantities:
            print(i, self.quantities[i])

    def write_to_csv(self, entity, entity_diction, list_of_dicts):
        with open('data/{}.csv'.format(entity), 'w', encoding='utf8') as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=entity_diction.keys())
            writer.writeheader()
            writer.writerows(list_of_dicts)

    def handleIntPrimaries(self, primaries, primaryKey, temp_dict, attribute):
        temp = primaryKey
        primaryKey += 1
        temp_dict[attribute] = temp
        primaries.append(temp)
        return primaryKey

    def loadNonForeign(self, entity, entity_diction,  attribute):
        type = entity_diction[attribute][0]
        if type == 'text':
            type = 'str'
        elif type == 'integer':
            type = 'int'
        df = pd.read_csv(
            "data/{}.csv".format(entity), dtype={attribute: type})
        return list(df[attribute])

    def loadForeign(self, entity_diction, attribute):
        foreign_type = entity_diction[attribute][0]
        foreign_entity = entity_diction[attribute][2]
        foreign_attribute = entity_diction[attribute][3]
        if foreign_type == 'text':
            foreign_type = 'str'
        elif foreign_type == 'integer':
            foreign_type = 'int'
        df = pd.read_csv(
            "data/{}.csv".format(foreign_entity), dtype={foreign_attribute: foreign_type})
        return list(df[foreign_attribute])

    def chooseForeign(self, foreign_lists, attribute, remove=False):
        foreign = random.choice(foreign_lists[attribute])
        if remove:
            foreign_lists[attribute].remove(foreign)
        return foreign

    def calculateScore(self, request, patient_info):
        patient = request["Patient"]
        priority = patient_info[patient]
        preference = request["Preference"]
        # the smallest the score the better
        score = preference / priority
        return score, patient

    def handleStatus(self, requests, patient_info):
        timeslot_requests = {}
        for i in range(len(requests)):
            request = requests[i]
            score, patient = self.calculateScore(request, patient_info)
            if request['Timeslot'] not in timeslot_requests.keys():
                timeslot_requests[request['Timeslot']] = [
                    (request['Request_ID'], i, patient, score)]
            else:
                timeslot_requests[request['Timeslot']].append(
                    (request['Request_ID'], i, patient, score))
        for timeslot in timeslot_requests:
            timeslot_requests[timeslot] = sorted(
                timeslot_requests[timeslot], key=lambda item: item[3])
            priority_patient = timeslot_requests[timeslot].pop()
            requests[priority_patient[1]]['Status'] = 1
            for patient in timeslot_requests[timeslot]:
                requests[patient[1]]['Status'] = 0

    def fabricatePerson(self, quantity):
        fake = Faker('el_GR')
        entity_diction = self.schema['Person']
        list_of_dicts = []
        for i in range(0, quantity):
            if not (i % 1000):
                print(i)
            temp_dict = {}
            for attribute in entity_diction.keys():
                type = entity_diction[attribute][0]
                if type == 'text':
                    if attribute == 'SSN':
                        temp_dict[attribute] = fake.unique.ssn()
                    elif attribute == 'Firstname':
                        if i % 2:
                            fname = unidecode(fake.first_name_male())
                        else:
                            fname = unidecode(fake.first_name_female())
                        temp_dict[attribute] = fname
                    elif attribute == 'Surname':
                        if i % 2:
                            sname = unidecode(fake.last_name_male())
                        else:
                            sname = unidecode(fake.last_name_female())
                        temp_dict[attribute] = unidecode(sname)
                    elif attribute == 'Phone':
                        temp_dict[attribute] = fake.phone_number()
                    elif attribute == 'Email':
                        temp_dict[attribute] = fname.lower(
                        ) + sname.lower() + '@gmail.com'
                elif type == 'date':
                    temp_dict[attribute] = fake.date_of_birth(
                        minimum_age=18, maximum_age=100)

            list_of_dicts.append(temp_dict)
        self.write_to_csv('Person', entity_diction, list_of_dicts)

    def fabricateSpecialty(self, quantity):
        entity_diction = self.schema['Specialty']
        list_of_dicts = []
        specialty_titles = ['Allergy_and_immunology', 'Anesthesiology', 'Dermatology', 'Diagnostic_radiology', 'Emergency_medicine',
                            'Family_medicine', 'Internal_medicine', 'Medical_genetics', 'Neurology', 'Nuclear_medicine', 'Obstetrics_and_gynecology',
                            'Ophthalmology', 'Pathology', 'Pediatrics', 'Physical_medicine_and_rehabilitation', 'Preventive_medicine', 'Psychiatry',
                            'Radiation_oncology', 'Surgery', 'Urology']

        for i in range(0, quantity):
            if not (i % 1000):
                print(i)
            temp_dict = {}
            for attribute in entity_diction.keys():
                type = entity_diction[attribute][0]
                attribute = attribute
                if type == 'text':
                    if attribute == 'Title':
                        random.shuffle(specialty_titles)
                        temp_dict[attribute] = specialty_titles.pop()
            list_of_dicts.append(temp_dict)
        self.write_to_csv('Specialty', entity_diction, list_of_dicts)

    def fabricateDoctor(self, quantity):
        fake = Faker('el_GR')
        entity_diction = self.schema['Doctor']
        list_of_dicts = []
        # contains the coresponding column to the referenced enity for each foreign
        foreign_lists = {}
        for i in range(0, quantity):
            if not (i % 1000):
                print(i)
            temp_dict = {}
            for attribute in entity_diction.keys():
                type = entity_diction[attribute][0]
                primary = entity_diction[attribute][1]

                if len(entity_diction[attribute]) == 4:
                    if i == 0:
                        foreign_lists[attribute] = self.loadForeign(
                            entity_diction, attribute)
                    if primary:
                        foreign = self.chooseForeign(
                            foreign_lists, attribute, True)
                    else:
                        foreign = self.chooseForeign(
                            foreign_lists, attribute)
                    temp_dict[attribute] = foreign

                elif len(entity_diction[attribute]) < 4:
                    if type == 'text':
                        if attribute == 'UPIN':
                            temp_dict[attribute] = fake.unique.bothify(
                                text='????-########')

                    elif type == 'boolean':
                        temp_dict[attribute] = random.randint(0, 1)

            list_of_dicts.append(temp_dict)
        self.write_to_csv('Doctor', entity_diction, list_of_dicts)

    def fabricatePatient(self, quantity):
        entity_diction = self.schema['Patient']
        list_of_dicts = []

        # contains the coresponding column to the referenced enity for each foreign
        foreign_lists = {}
        for i in range(0, quantity):
            if not (i % 1000):
                print(i)
            temp_dict = {}
            for attribute in entity_diction.keys():
                type = entity_diction[attribute][0]

                if len(entity_diction[attribute]) == 4:
                    if i == 0:
                        foreign_lists[attribute] = self.loadForeign(
                            entity_diction, attribute)
                        df = pd.read_csv("data/Doctor.csv",
                                         dtype={"Doctor_ID": "string"})
                        doctors = list(df['Doctor_ID'])
                        for d in doctors:
                            foreign_lists['Patient_ID'].remove(d)
                    foreign = self.chooseForeign(
                        foreign_lists, attribute, True)
                    temp_dict[attribute] = foreign

                elif len(entity_diction[attribute]) < 4:
                    if type == 'integer':
                        if attribute == 'Priority':
                            temp_dict[attribute] = random.randint(1, 10)
            list_of_dicts.append(temp_dict)
        self.write_to_csv('Patient', entity_diction, list_of_dicts)

    def fabricateTimeslot(self, quantity):
        primaryKey = 1
        entity_diction = self.schema['Timeslot']
        list_of_dicts = []
        primaries = []
        start_date = date.today()
        final_date = start_date + timedelta(days=90)
        current_date = start_date
        appointment_times = ['09:00:00', '10:00:00', '11:00:00', '12:00:00',
                             '13:00:00', '14:00:00', '15:00:00', '16:00:00', '17:00:00']
        available_appointments = appointment_times[:]
        doctor_finished = True
        current_appointment = None
        # contains the coresponding column to the referenced enity for each foreign
        foreign_lists = {}
        for i in range(0, quantity):

            if len(available_appointments):
                current_appointment = available_appointments.pop(0)
            else:
                available_appointments = appointment_times[:]
                if (current_date + timedelta(days=1)).weekday() not in [5, 6]:
                    current_date = current_date + timedelta(days=1)
                else:
                    while current_date.weekday() != 0:
                        current_date = current_date + timedelta(days=1)
            if current_date > final_date:
                doctor_finished = True
                current_date = start_date

            if not (i % 1000):
                print(i)
            temp_dict = {}
            for attribute in entity_diction.keys():
                type = entity_diction[attribute][0]
                primary = entity_diction[attribute][1]

                if len(entity_diction[attribute]) == 4:
                    if i == 0:
                        foreign_lists[attribute] = self.loadForeign(
                            entity_diction, attribute)
                    if doctor_finished:
                        foreign = self.chooseForeign(
                            foreign_lists, attribute, True)
                        doctor_finished = False

                    temp_dict[attribute] = foreign

                elif len(entity_diction[attribute]) < 4:
                    if primary:
                        primaryKey = self.handleIntPrimaries(
                            primaries, primaryKey, temp_dict, attribute)
                    else:
                        if type == 'date':
                            temp_dict[attribute] = current_date

                        elif type == 'time':
                            temp_dict[attribute] = current_appointment

                        elif type == 'boolean':
                            temp_dict[attribute] = random.randint(0, 1)

            list_of_dicts.append(temp_dict)
        self.write_to_csv('Timeslot', entity_diction, list_of_dicts)

    def fabricateRequests(self, quantity):
        primaryKey = 1
        entity_diction = self.schema['Requests']
        patient_dict = self.schema['Patient']
        priorities = self.loadNonForeign(
            "Patient", patient_dict, "Priority")

        list_of_dicts = []
        primaries = []
        patients = {}
        current_patient = None
        # contains the coresponding column to the referenced enity for each foreign
        foreign_lists = {}
        for i in range(0, quantity):
            if not (i % 1000):
                print(i)
            temp_dict = {}
            for attribute in entity_diction.keys():
                type = entity_diction[attribute][0]
                primary = entity_diction[attribute][1]
                if len(entity_diction[attribute]) == 4:
                    if i == 0:
                        foreign_lists[attribute] = self.loadForeign(
                            entity_diction, attribute)
                        if attribute == "Patient":
                            for p in foreign_lists["Patient"]:
                                patients[p] = []
                    foreign = self.chooseForeign(
                        foreign_lists, attribute)
                    if entity_diction[attribute][3] == 'Patient_ID':
                        current_patient = foreign
                    elif entity_diction[attribute][3] == 'Timeslot_ID':
                        while foreign in patients[current_patient]:
                            foreign = self.chooseForeign(
                                foreign_lists, attribute)
                        patients[current_patient].append(foreign)
                    temp_dict[attribute] = foreign

                elif len(entity_diction[attribute]) < 4:
                    if primary:
                        primaryKey = self.handleIntPrimaries(
                            primaries, primaryKey, temp_dict, attribute)

                    else:
                        if type == 'integer':
                            if attribute == 'Preference':
                                temp_dict[attribute] = len(
                                    patients[current_patient])

            list_of_dicts.append(temp_dict)
        patient_info = {patient: priority for patient, priority in zip(
            foreign_lists["Patient"], priorities)}
        self.handleStatus(list_of_dicts, patient_info)
        self.write_to_csv("Requests", entity_diction, list_of_dicts)

    def fabricate(self, entity):
        try:
            match entity:
                case 'Person':
                    self.fabricatePerson(self.quantities['Person'])
                case 'Specialty':
                    self.fabricateSpecialty(self.quantities['Specialty'])
                case 'Doctor':
                    self.fabricateDoctor(self.quantities['Doctor'])
                case 'Patient':
                    self.fabricatePatient(self.quantities['Patient'])
                case 'Timeslot':
                    self.fabricateTimeslot(self.quantities['Timeslot'])
                case 'Requests':
                    self.fabricateRequests(self.quantities['Requests'])

            print(f'Data fabrication for table {entity} finished')

        except:
            print(f'Failed to fabricate data for table {entity} finished')
