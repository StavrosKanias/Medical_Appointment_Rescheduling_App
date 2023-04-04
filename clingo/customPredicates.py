from datetime import datetime, date, time
from clorm import StringField, IntegerField, Predicate


class DateField(StringField):
    def pytocl(dt): return dt.strftime("%Y-%m-%d")
    def cltopy(s): return datetime.strptime(s, "%Y-%m-%d").date()


class TimeField(StringField):
    def pytocl(t): return t.strftime("%H:%M:%S")
    def cltopy(s): return datetime.strptime(s, "%H:%M:%S").time()
