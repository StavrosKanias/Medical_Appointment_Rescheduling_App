from clorm import IntegerField, Predicate
from knowledgeBase import KnowledgeBase
import sys
sys.path.append('./db')  # nopep8
from dbCreator import schema
from getpass import getpass


def main():
    # Get only future and available timeslots
    print('Initiating connection...\n')
    dbConditions = {'TIMESLOT': {
        "DATE": [('>', '+0')], "TIMESLOT_AVAILABLE": [('=', True)]}}
    dbConditions = {}
    kb = None
    while not kb or not kb.db.con: 
        name = input("\nInsert DB credentials\nDB name: \n")
        password = getpass("DB password: \n")
        db_info = ['Kanon2000', name, password]
        kb = KnowledgeBase(name, schema,
                        dbInfo=db_info, dbConditions=dbConditions)
    write = input(f"\nCreate {name}.lp?\n y/n:\n")
    if write == 'y':
        kb.toFile('clingo/')
    enc = input('Choose encoding, merged(m)/split(s): \n')
    if enc == 'm':
        merged = True
        asp = 'clingo/reschedulers/reschedulerMergedGrantGeneral.lp'
        subKB = {'REQUEST': ['ID', 'PATIENT_ID',
                             'TIMESLOT_ID', 'SCORE', 'STATUS'], 'TIMESLOT': ['ID', 'DOCTOR_ID'], 'DOCTOR': ['ID', 'SPECIALTY_TITLE']}
        class Grant(Predicate):
            request = IntegerField
            score = IntegerField
    else:
        merged = False
        asp = 'clingo/reschedulers/reschedulerGrantGeneral.lp'
        subKB = {'REQUEST': ['PATIENT_ID', 'TIMESLOT_ID', 'SCORE', 'STATUS'], 
                 'TIMESLOT': ['DOCTOR_ID'], 'DOCTOR': ['SPECIALTY_TITLE']}
        class Grant(Predicate):
            request = IntegerField

    class Claimed(Predicate):
        request = IntegerField
    batch = input("Use batching\n y/n:\n")
    if batch == 'y':
        while(True):
            batchent = input("Choose batching entity, doctor(d)/specialty(s): \n")
            if batchent == 's':
                be = 'SPECIALTY'
                ba = 'TITLE'
            elif batchent == 'd':
                be = 'DOCTOR'
                ba = 'ID'
            if be:
                bd = kb.select({be:[ba]})
                break
            else: 
                print('Invalid input\n')


    da = int(input('Enter canceled appointment id (0 to exit): \n'))
    while(da):
        kb.delete(['Request'], cond={'Request': {'id': [('=', da)]}})
        if batch == 'y':
            benefit = 0
            time = 0
            for b in bd:
                print(f'\n{be} {ba}: {b}')
                bcond = {be:{ba:[('=',b)]}}
                solution, stats = kb.run(asp, [Grant, Claimed], searchDuration=12, show=True, subKB=subKB, subKBCond=bcond, merged=merged)
                if solution:
                    benefit -= stats['summary']['lower'][0]
                    time += stats['summary']['times']['cpu']
                    # update(kb, solution,bcond=bcond)

        else:
            solution, stats = kb.run(asp, [Grant, Claimed], searchDuration=20, show=True, subKB=subKB, merged=merged)
            if solution:
                benefit = -stats['summary']['lower'][0]
                time = stats['summary']['times']['cpu']
                # update(kb, solution)
        print(f'RESCHEDULING COMPLETED\nExecution time: {time} s.\nTotal common benefit: {benefit}\n')
        da = int(input('Enter canceled appointment id (0 to disconnect): \n'))

 
def update(kb, solution, bcond = None):
    update = {}
    granted = {x.request: 1 for x in solution['Grant']}
    claimed = {x.request: 1 for x in solution['Claimed']}
    granted.update(claimed)
    update.update(granted)
    requestIDs = [x for x in kb.select(
        {'Request': ['id']}, cond=bcond, order={'Request': ['id']})]
    waiting = {x: 0 for x in list(set(requestIDs) ^ set(granted))}
    update.update(waiting)
    update = {x: update[x] for x in sorted(update)}

    for u in update:
        kb.update({'REQUEST': {'STATUS': update[u]}}, cond={'REQUEST': {'ID': [
            ('=', u)]}}, toDb=True)
        
if __name__ == "__main__":
    main()
