from textwrap import dedent
import clingo


def run(string):
    control = clingo.Control()

    control.add("base", [], dedent(string))

    control.ground([("base", [])])

    control.configuration.solve.models = 0
    models = []
    with control.solve(yield_=True) as handle:
        # loop over all models and print them
        for model in handle:
            models.append(str(model))
    print(models)
