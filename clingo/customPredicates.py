from datetime import datetime, date, time
from clorm import StringField, IntegerField, Predicate


class DateField(StringField):
    def pytocl(dt): return dt.strftime("%Y-%m-%d")
    def cltopy(s): return datetime.strptime(s, "%Y-%m-%d").date()


class TimeField(StringField):
    def pytocl(t): return t.strftime("%H:%M:%S")
    def cltopy(s): return datetime.strptime(s, "%H:%M:%S").time()


# class Doctor(Predicate):
#     id = StringField
#     upin = StringField
#     availabilty = StringField
#     specialty_title = StringField


# class Patient(Predicate):
#     id = StringField
#     priority = IntegerField


# class Person(Predicate):
#     ssn = StringField
#     firstname = StringField
#     lastname = StringField
#     phone = StringField
#     email = StringField
#     birth_date = DateField


# class Request(Predicate):
#     id = IntegerField
#     patient_id = StringField
#     timeslot_id = IntegerField
#     preference = IntegerField
#     score = IntegerField
#     status = IntegerField


# class Specialty(Predicate):
#     title = StringField


timeslot = type('Timeslot', (Predicate, ), {'id': IntegerField,
                                            'date': DateField,
                                            'time': TimeField,
                                            'availability': StringField,
                                            'doctor_id': StringField})


# # class Timeslot(Predicate):
# #     id = IntegerField
# #     date = DateField
# #     time = TimeField
# #     availability = StringField
# #     doctor_id = StringField


# d = Doctor('a', 'b', '1', 'c')
# p = Patient('a', 1)
# pe = Person('a', 's', 'k', '1412321', 'agsgacgz', datetime.date(2023, 1, 1))
# r = Request(1, 'asd', 1, 1, 1, 1)
# s = Specialty('lalala')
print(dir(timeslot))
t = timeslot(1, date(2023, 10, 12), time(22, 1, 1), '1', 'a')

# fb = FactBase([d, p, pe, r, s, t])
# print(FactBase.asp_str(fb))
