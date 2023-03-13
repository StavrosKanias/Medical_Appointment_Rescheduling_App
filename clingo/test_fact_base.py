import datetime
from clorm import FactBase, Predicate, IntegerField, StringField, ConstantField
from clorm import desc, path, alias


class Option(Predicate):
    oid = IntegerField
    name = StringField
    cost = IntegerField
    cat = StringField


class Chosen(Predicate):
    oid = IntegerField


fb = FactBase([Option(1, "Do A", 200, "foo"), Option(2, "Do B", 300, "bar"),
               Option(3, "Do C", 400, "foo"), Option(4, "Do D", 300, "bar"),
               Option(5, "Do E", 200, "foo"), Option(6, "Do F", 500, "bar"),
               Chosen(1), Chosen(3), Chosen(4), Chosen(6)])

q1 = fb.query(Chosen)     # Select all Chosen instances
result = set(q1.all())
assert result == set([Chosen(1), Chosen(3), Chosen(4), Chosen(6)])

q2 = fb.query(Option, Chosen).join(Option.oid == Chosen.oid)

result = set(q2.all())

assert result == set([(Option(1, "Do A", 200, "foo"), Chosen(1)),
                      (Option(3, "Do C", 400, "foo"), Chosen(3)),
                      (Option(4, "Do D", 300, "bar"), Chosen(4)),
                      (Option(6, "Do F", 500, "bar"), Chosen(6))])


q3 = q2.where(Option.cost > 200).order_by(desc(Option.cost))

result = list(q3.all())
assert result == [(Option(6, "Do F", 500, "bar"), Chosen(6)),
                  (Option(3, "Do C", 400, "foo"), Chosen(3)),
                  (Option(4, "Do D", 300, "bar"), Chosen(4))]

q4 = q3.select(Option)

result = list(q4.all())
assert result == [Option(6, "Do F", 500, "bar"),
                  Option(3, "Do C", 400, "foo"),
                  Option(4, "Do D", 300, "bar")]

q5 = q2.group_by(Option.cat).select(Option.cost)

result = [(cat, sum(list(it))) for cat, it in q5.all()]
assert result == [("bar", 800), ("foo", 600)]


class F(Predicate):
    a = ConstantField


fb = FactBase([F("foo"), F("bar")])

# qBad = fb.query(F).where(F == F("bar"))    # This won't do what you expect

qGood = fb.query(F).where(path(F) == F("bar"))


class F(Predicate):
    pid = IntegerField
    name = StringField
    fid = IntegerField


fb = FactBase([F(1, "Adam", 3), F(2, "Betty", 4),
              F(3, "Carol", 1), F(4, "Dan", 2)])

FA = alias(F)
q = fb.query(F, FA).join(F.pid == FA.fid).select(F.name, FA.name)

for p, f in q.all():
    print("Person {} => Friend {}".format(p, f))


class DateField(StringField):
    def pytocl(dt): return dt.strftime("%Y%m%d")
    def cltopy(s): return datetime.datetime.strptime(s, "%Y%m%d").date()
