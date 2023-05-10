import pandas as pd
import csv
import random
import numpy as np
from faker import Faker
from unidecode import unidecode
from datetime import date, timedelta


class DataFabricator():
    def __init__(self, schema=None, minP=None, maxP=None, minD=None, maxD=None, minS=None, maxS=None, demand=None, tAvailability=None, seed=None):
        if schema != None:
            self.schema = schema
            self.quantities = {}
            self.quantities['PERSON'] = random.randint(minP, maxP)
            self.quantities['DOCTOR'] = random.randint(minD, maxD)
            self.quantities['PATIENT'] = self.quantities['PERSON'] - \
                self.quantities['DOCTOR']
            self.quantities['SPECIALTY'] = random.randint(minS, maxS)
            # 8 timeslots per day, per doctor for 5 days a week for 3 months
            self.quantities['TIMESLOT'] = int(
                self.quantities['DOCTOR'] * 8 * 5 * 2)
            self.quantities['REQUEST'] = int(
                demand * self.quantities['TIMESLOT'])
            self.tAvailability = tAvailability
            for i in self.quantities:
                print(i, self.quantities[i])
            if seed:
                Faker.seed(seed)
                random.seed(seed)

    def write_to_csv(self, entity, entity_diction, list_of_dicts):
        with open('db\\data\\{}.csv'.format(entity), 'w', encoding='utf8') as csvfile:
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

    def loadNonForeign(self, entity, attribute):
        type = self.schema[entity][attribute][0]
        if type == 'text':
            type = 'str'
        elif type == 'integer':
            type = 'int'
        df = pd.read_csv(
            "db\\data\\{}.csv".format(entity), dtype={attribute: type})
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
            "db\\data\\{}.csv".format(foreign_entity), dtype={foreign_attribute: foreign_type})
        return list(df[foreign_attribute])

    def chooseForeign(self, foreign_lists, attribute, remove=False, get_index=False):
        index = random.randint(0, len(foreign_lists[attribute]) - 1)
        foreign = foreign_lists[attribute][index]
        if remove:
            foreign_lists[attribute].remove(foreign)
        if get_index:
            return foreign, index
        else:
            return foreign

    def calculateScore(self, request, patient_info):
        patient = request["PATIENT_ID"]
        priority = patient_info[patient]
        preference = request["PREFERENCE"]
        score = round(30 * 1/preference + 70 * (priority/100))
        return score, patient

    def handleStatus(self, requests, patient_info):
        timeslot_requests = {}
        for i in range(len(requests)):
            request = requests[i]
            score, patient = self.calculateScore(request, patient_info)
            requests[i]['SCORE'] = score
            if request['TIMESLOT_ID'] not in timeslot_requests.keys():
                timeslot_requests[request['TIMESLOT_ID']] = [
                    (request['ID'], i, patient, score)]
            else:
                timeslot_requests[request['TIMESLOT_ID']].append(
                    (request['ID'], i, patient, score))
        appointed = []
        for timeslot in timeslot_requests:
            timeslot_requests[timeslot] = sorted(
                timeslot_requests[timeslot], key=lambda item: item[3])
            priority_patient = timeslot_requests[timeslot].pop(0)
            while priority_patient[2] in appointed and len(timeslot_requests[timeslot]):
                priority_patient = timeslot_requests[timeslot].pop(0)
            if priority_patient[2] not in appointed:
                requests[priority_patient[1]]['STATUS'] = 1
                appointed.append(priority_patient[2])

    def fabricatePerson(self, quantity):
        fake = Faker('el_GR')
        entity_diction = self.schema['PERSON']
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
                    elif attribute == 'FIRSTNAME':
                        if i % 2:
                            fname = unidecode(fake.first_name_male())
                        else:
                            fname = unidecode(fake.first_name_female())
                        temp_dict[attribute] = fname
                    elif attribute == 'LASTNAME':
                        if i % 2:
                            sname = unidecode(fake.last_name_male())
                        else:
                            sname = unidecode(fake.last_name_female())
                        temp_dict[attribute] = unidecode(sname)
                    elif attribute == 'PHONE':
                        temp_dict[attribute] = fake.phone_number()
                    elif attribute == 'EMAIL':
                        temp_dict[attribute] = fname.lower(
                        ) + sname.lower() + '@gmail.com'
                elif type == 'date':
                    temp_dict[attribute] = fake.date_of_birth(
                        minimum_age=18, maximum_age=100)

            list_of_dicts.append(temp_dict)
        self.write_to_csv('PERSON', entity_diction, list_of_dicts)

    def fabricateSpecialty(self, quantity):
        entity_diction = self.schema['SPECIALTY']
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
                    if attribute == 'TITLE':
                        random.shuffle(specialty_titles)
                        temp_dict[attribute] = specialty_titles.pop()
            list_of_dicts.append(temp_dict)
        self.write_to_csv('SPECIALTY', entity_diction, list_of_dicts)

    def fabricateDoctor(self, quantity):
        fake = Faker('el_GR')
        entity_diction = self.schema['DOCTOR']
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
                            foreign_lists, attribute, remove=True)
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
                        if attribute == 'DOCTOR_AVAILABLE':
                            temp_dict[attribute] = 1

            list_of_dicts.append(temp_dict)
        self.write_to_csv('DOCTOR', entity_diction, list_of_dicts)

    def fabricatePatient(self, quantity):
        entity_diction = self.schema['PATIENT']
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
                        doctors = self.loadNonForeign(
                            'DOCTOR', 'ID')
                        for d in doctors:
                            foreign_lists['ID'].remove(d)
                    foreign = self.chooseForeign(
                        foreign_lists, attribute, remove=True)
                    temp_dict[attribute] = foreign

                elif len(entity_diction[attribute]) < 4:
                    if type == 'integer':
                        if attribute == 'PRIORITY':
                            temp_dict[attribute] = random.randint(1, 100)
            list_of_dicts.append(temp_dict)
        self.write_to_csv('PATIENT', entity_diction, list_of_dicts)

    def fabricateTimeslot(self, quantity):
        primaryKey = 1
        entity_diction = self.schema['TIMESLOT']
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
            if not (i % 1000):
                print(i)
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
                            foreign_lists, attribute, remove=True)
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
                            if attribute == 'TIMESLOT_AVAILABLE':
                                temp_dict[attribute] = np.random.choice(
                                    [0, 1], p=[1-self.tAvailability, self.tAvailability])

            list_of_dicts.append(temp_dict)
        self.write_to_csv('TIMESLOT', entity_diction, list_of_dicts)

    def fabricateRequest(self, quantity):
        primaryKey = 1
        entity_diction = self.schema['REQUEST']
        priorities = self.loadNonForeign(
            "PATIENT", "PRIORITY")

        list_of_dicts = []
        primaries = []
        patients = {}
        current_patient = None
        # availabilities = self.loadNonForeign(
        #     'TIMESLOT', 'TIMESLOT_AVAILABLE')

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
                        if attribute == "PATIENT_ID":
                            for p in foreign_lists["PATIENT_ID"]:
                                patients[p] = []
                    foreign, index = self.chooseForeign(
                        foreign_lists, attribute, get_index=True)

                    if attribute == 'PATIENT_ID':
                        current_patient = foreign
                    elif attribute == 'TIMESLOT_ID':
                        # available = availabilities[index]
                        # while foreign in patients[current_patient] or not available:
                        while foreign in patients[current_patient]:
                            foreign, index = self.chooseForeign(
                                foreign_lists, attribute, get_index=True)
                            # available = availabilities[index]
                        patients[current_patient].append(foreign)
                    temp_dict[attribute] = foreign

                elif len(entity_diction[attribute]) < 4:
                    if primary:
                        primaryKey = self.handleIntPrimaries(
                            primaries, primaryKey, temp_dict, attribute)

                    else:
                        if type == 'integer':
                            if attribute == 'PREFERENCE':
                                temp_dict[attribute] = len(
                                    patients[current_patient])
                            elif attribute == 'STATUS':
                                temp_dict[attribute] = 0

            list_of_dicts.append(temp_dict)
        patient_info = {patient: priority for patient, priority in zip(
            foreign_lists["PATIENT_ID"], priorities)}
        self.handleStatus(list_of_dicts, patient_info)
        self.write_to_csv("REQUEST", entity_diction, list_of_dicts)

    def fabricate(self, entity):

        match entity:
            case 'PERSON':
                self.fabricatePerson(self.quantities['PERSON'])
            case 'SPECIALTY':
                self.fabricateSpecialty(self.quantities['SPECIALTY'])
            case 'DOCTOR':
                self.fabricateDoctor(self.quantities['DOCTOR'])
            case 'PATIENT':
                self.fabricatePatient(self.quantities['PATIENT'])
            case 'TIMESLOT':
                self.fabricateTimeslot(self.quantities['TIMESLOT'])
            case 'REQUEST':
                self.fabricateRequest(self.quantities['REQUEST'])

        print(f'Data fabrication for table {entity} finished\n')
