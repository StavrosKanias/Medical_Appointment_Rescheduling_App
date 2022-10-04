import pandas as pd
import csv
import string
import random
from faker import Faker
from unidecode import unidecode
from datetime import date, timedelta
import calendar


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
        self.quantities['Chooses'] = int(
            demand * self.quantities['Timeslot'])
        for i in self.quantities.keys():
            print(i, self.quantities[i])

    def get_random_string(self, letters_count, digits_count=0):
        letters = ''.join((random.choice(string.ascii_letters)
                           for i in range(letters_count)))
        digits = ''.join((random.choice(string.digits)
                          for i in range(digits_count)))

        # Convert resultant string to list and shuffle it to mix letters and digits
        sample_list = list(letters + digits)
        random.shuffle(sample_list)
        # convert list to string
        final_string = ''.join(sample_list)
        return final_string

    def write_to_csv(self, entity, entity_diction, list_of_dicts):
        with open('data/{}.csv'.format(entity), 'w', encoding='utf8') as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=entity_diction.keys())
            writer.writeheader()
            writer.writerows(list_of_dicts)

    def make_without_foreign(self, entity, end):
        fake = Faker('el_GR')
        primaryKey = 1
        entity_diction = self.schema[entity]
        list_of_dicts = []
        primaries = []
        specialty_titles = ['Allergy_and_immunology', 'Anesthesiology', 'Dermatology', 'Diagnostic_radiology', 'Emergency_medicine',
                            'Family_medicine', 'Internal_medicine', 'Medical_genetics', 'Neurology', 'Nuclear_medicine', 'Obstetrics_and_gynecology',
                            'Ophthalmology', 'Pathology', 'Pediatrics', 'Physical_medicine_and_rehabilitation', 'Preventive_medicine', 'Psychiatry',
                            'Radiation_oncology', 'Surgery', 'Urology']

        for i in range(0, end):
            if not (i % 1000):
                print(i)
            temp_dict = {}
            for attribute in entity_diction.keys():
                type = entity_diction[attribute][0]
                primary = entity_diction[attribute][1]
                attribute = attribute

                if primary == False or primary == True and type != 'integer':
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
                        elif attribute == 'Title':
                            random.shuffle(specialty_titles)
                            temp_dict[attribute] = specialty_titles.pop()

                    elif type == 'date':
                        temp_dict[attribute] = fake.date_between(
                            start_date='-100y')

                    else:
                        print('error   ', type, entity)

                else:
                    while True:
                        temp = primaryKey
                        primaryKey += 1
                        if temp not in primaries:
                            temp_dict[attribute] = temp
                            primaries.append(temp)
                            break
            list_of_dicts.append(temp_dict)

        self.write_to_csv(entity, entity_diction, list_of_dicts)

    def make_with_foreign(self, entity, end):
        primaryKey = 1
        entity_diction = self.schema[entity]
        list_of_dicts = []

        primaries = []
        if entity == 'Chooses':
            patients = {}
            current_patient = None
        if entity == 'Doctor':
            upins = []
        if entity == 'Timeslot':
            start_date = date.today()
            final_date = start_date + timedelta(days=90)
            current_date = start_date
            appointment_times = ['10:00:00', '11:00:00', '12:00:00', '13:00:00',
                                 '14:00:00', '15:00:00', '16:00:00', '17:00:00', '18:00:00']
            available_appointments = appointment_times[:]
            doctor_finished = True
            current_appointment = None

        # contains the coresponding column to the referenced enity for each foreign
        foreign_lists = {}
        tempPrim = None
        tempFor = None
        for i in range(0, end):
            if entity == 'Timeslot':
                if len(available_appointments):
                    current_appointment = available_appointments.pop()
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
                name = attribute

                if len(entity_diction[attribute]) == 4:
                    if i == 0:
                        df = pd.read_csv(
                            "data/{}.csv".format(entity_diction[attribute][2]), dtype={entity_diction[attribute][3]: "string"})
                        foreign_lists[entity_diction[attribute][3]] = list(
                            df[entity_diction[attribute][3]])

                        if entity == 'Patient':
                            df = pd.read_csv("data/Doctor.csv",
                                             dtype={"Doctor_ID": "string"})
                            doctors = list(df['Doctor_ID'])
                            for d in doctors:
                                foreign_lists['SSN'].remove(d)

                    if entity != 'Timeslot' or entity == 'Timeslot' and doctor_finished:
                        tempFor = random.choice(
                            foreign_lists[entity_diction[attribute][3]])
                        if entity == 'Timeslot':
                            foreign_lists[entity_diction[attribute]
                                          [3]].remove(tempFor)
                            doctor_finished = False

                    if primary == True:
                        foreign_lists[entity_diction[attribute]
                                      [3]].remove(tempFor)
                    if entity_diction[attribute][3] == 'Patient_ID':
                        if i == 0:
                            df = pd.read_csv(
                                "data/Patient.csv", dtype={"Patient_ID": "string"})
                            for p in list(df[entity_diction[attribute][3]]):
                                patients[p] = 0
                        current_patient = tempFor
                        patients[tempFor] += 1
                    temp_dict[name] = tempFor

                elif len(entity_diction[attribute]) < 4:
                    if primary == False:

                        if type == 'text':
                            if name == 'UPIN':
                                while True:
                                    upin = self.get_random_string(5, 8)
                                    if upin not in upins:
                                        upins.append(upin)
                                        break
                                temp_dict[name] = upin

                        elif type == 'integer':
                            if name == 'Preference':
                                temp_dict[name] = patients[current_patient]

                        elif type == 'float':
                            if name == 'Priority':
                                temp_dict[name] = round(
                                    random.random() * 10.0, 2)

                        elif type == 'date':
                            temp_dict[name] = current_date

                        elif type == 'time':
                            temp_dict[name] = current_appointment

                        elif type == 'boolean':
                            temp_dict[name] = random.randint(0, 1)

                    else:
                        while True:
                            tempPrim = primaryKey
                            primaryKey += 1
                            if tempPrim not in primaries:
                                temp_dict[name] = tempPrim
                                primaries.append(tempPrim)
                                break

            list_of_dicts.append(temp_dict)
        self.write_to_csv(entity, entity_diction, list_of_dicts)

    def hasForeign(self, entity):
        entity_diction = self.schema[entity]
        for attribute in entity_diction.keys():
            if len(entity_diction[attribute]) == 4:
                return True
        return False

    def fabricate(self, entity):
        if self.hasForeign(entity):
            self.make_with_foreign(entity, self.quantities[entity])
        else:
            self.make_without_foreign(entity, self.quantities[entity])

        print(f'Data fabrication for table {entity} finished')
