# Creates a new predicate from existing data
def compose(self, name, attributes, populate=True, merge=False):
     pname = self.getPredicateName(name)
      content = {}
       for e in attributes:
            for a in attributes[e]:
                field = self.predicates[e][a[1]]
                content[a[0].lower()] = field
        predicate = type(pname,
                         (Predicate, ), content)
        self.predicates[pname] = predicate

        if populate:
            for e in attributes:
                for a in a

        if merge:
            for e in attributes:
                for a in attributes[e]:
                    field = self.predicates[e][a]
                    self.kb.query(field).delete()
                    del self.predicates[field]
                del self.schema[e]
