    # Do something with the solution - create a query so we can print out the
    # assignments for each driver.
    query = solution.query(Assignment).where(
        Assignment.driver == ph1_).order_by(Assignment.time)

    for d in drivers:
        assignments = list(query.bind(d.name).all())
        if not assignments:
            print("Driver {} is not working today".format(d.name))
        else:
            print("Driver {} must deliver: ".format(d.name))
            for a in assignments:
                print("\t Item {} at time {}".format(a.item, a.time))