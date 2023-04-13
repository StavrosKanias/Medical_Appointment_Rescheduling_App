from clorm import StringField, IntegerField, Predicate, path


class MyClass(Predicate):
    a = IntegerField


print(type(getattr(MyClass, 'a')))
