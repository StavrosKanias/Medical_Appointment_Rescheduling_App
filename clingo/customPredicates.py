from datetime import datetime
from clorm import StringField


class DateField(StringField):
    def pytocl(dt): return dt.strftime("%Y-%m-%d")
    def cltopy(s): return datetime.strptime(s, "%Y-%m-%d").date()


class TimeField(StringField):
    def pytocl(t): return t.strftime("%H:%M:%S")
    def cltopy(s): return datetime.strptime(s, "%H:%M:%S").time()
