from knowledgeBase import KnowledgeBase
from clorm import ph1_

TestSchema = {
    'DRIVER': {"NAME": ['text', 'primary']},

    'ITEM': {"NAME": ['integer', 'primary']},

    'TIME': {"TIME": ["integer", "primary"]},

    'ASSIGNMENT': {"ITEM": ['integer', 'primary'], "DRIVER": ['text', 'primary'], "TIME": ['integer', 'primary']},
}

data = {
    'DRIVER': [['dave'], ["morri"], ["michael"]],
    'ITEM': [[1], [2], [3], [4], [5], [6]],
    'TIME': [[1], [2], [3], [4]]
}

kb = KnowledgeBase(name='testKB', schema=TestSchema, data=data)
kb.toFile('clingo/')
solution = kb.run('clingo/test_clorm.lp')
print(solution)

# Do something with the solution - create a query so we can print out the
# assignments for each driver.
Assignment = kb.literals['ASSIGNMENT']
query = solution.query(Assignment).where(
    Assignment.driver == ph1_).order_by(Assignment.time)

for d in kb.select('DRIVER'):
    assignments = list(query.bind(d.name).all())
    if not assignments:
        print("Driver {} is not working today".format(d.name))
    else:
        print("Driver {} must deliver: ".format(d.name))
        for a in assignments:
            print("\t Item {} at time {}".format(a.item, a.time))
