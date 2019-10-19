from src.RDBDataTable import RDBDataTable

import logging
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def table_people():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "dbuserdbuser",
        "db": "lahman2019raw"
    }

    return RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])


def t1():
    tbl = table_people()
    print("RDB table = ", tbl)


def t_find_by_template():
    tbl = table_people()
    print("People named Williams and born in California:")
    res = tbl.find_by_template({"nameLast": "Williams", "birthState": "CA"}, ["nameFirst", "birthCity"])
    for person in res:
        print(person)
    print()


def t_find_by_key():
    tbl = table_people()
    print("Finding Ted Williams by key:")
    print(tbl.find_by_primary_key(["willite01"], ["nameFirst", "nameLast"]))
    print()


def t_delete():
    tbl = table_people()
    print("Deleting everyone from New York:")
    print("Before: {} Williams".format(len(tbl.find_by_template({"nameLast": "Williams"}))))
    tbl.delete_by_template({"birthState": "WA"})
    print("After: {} Williams".format(len(tbl.find_by_template({"nameLast": "Williams"}))))
    print("Deleting Ace Williams: :'(")
    tbl.delete_by_key(["williac01"])
    print("After: {} Williams".format(len(tbl.find_by_template({"nameLast": "Williams"}))))
    print()


def t_update():
    tbl = table_people()
    print("Updating Ted's first name:")
    print("Before: {}".format(tbl.find_by_primary_key(["willite01"], ["nameFirst", "nameLast"])))
    tbl.update_by_template({"nameFirst": "Ted", "nameLast": "Williams"}, {"nameFirst": "Nabuchodonosor"})
    print("After: {}".format(tbl.find_by_primary_key(["willite01"], ["nameFirst", "nameLast"])))
    tbl.update_by_key(["willite01"], {"nameFirst": "Merlin"})
    print("After: {}".format(tbl.find_by_primary_key(["willite01"], ["nameFirst", "nameLast"])))
    print()


def t_insert():
    tbl = table_people()
    print("Adding Ted's little brother:")
    print("Before: {}".format(tbl.find_by_template({"nameLast": "Williams", "birthCity": "San Diego"}, ["nameFirst", "birthCity"])))
    tbl.insert({'playerID': 'willite02', 'birthYear': '1922', 'birthMonth': '8', 'birthDay': '30', 'birthCountry': 'USA', 'birthState': 'CA', 'birthCity': 'San Diego', 'deathYear': '2022', 'deathMonth': '7', 'deathDay': '5', 'deathCountry': 'USA', 'deathState': 'FL', 'deathCity': 'Inverness', 'nameFirst': 'Dick', 'nameLast': 'Williams', 'nameGiven': 'Richard', 'weight': '205', 'height': '75', 'bats': 'L', 'throws': 'R', 'debut': '1939-04-20', 'finalGame': '1960-09-28', 'retroID': 'willt104', 'bbrefID': 'willite02'})
    print("After: {}".format(tbl.find_by_template({"nameLast": "Williams", "birthCity": "San Diego"}, ["nameFirst", "birthCity"])))
    print()


t1()
t_find_by_template()
t_find_by_key()
t_delete()
t_update()
t_insert()
