from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from random import choice, choices, randint
from sys import argv
from time import time

cluster = Cluster(['localhost'])
session = cluster.connect('demoks')

USERS = [line.strip() for line in open('users.txt')]
TAGS = [line.strip() for line in open('tags.txt')]
YEARS = list(range(2013, 2020))
MONTHS = list(range(1, 12))
DAYS = list(range(1, 31))
UUIDS = [row.uuid for row in list(session.execute('SELECT uuid FROM tweets3_by_uuid LIMIT 10'))]

selects_1 = [
    'SELECT * FROM tweets1 WHERE user=%(user)s',
    'SELECT * FROM tweets2 WHERE user=%(user)s',
    'SELECT * FROM tweets3 WHERE user=%(user)s'
]

selects_2 = [
    'SELECT * FROM tweets1 WHERE user=%(user)s AND year=%(year)s',
    'SELECT * FROM tweets2 WHERE user=%(user)s AND year=%(year)s ALLOW FILTERING',
    'SELECT * FROM tweets3 WHERE user=%(user)s AND year=%(year)s ALLOW FILTERING'
]

selects_3 = [
    'SELECT * FROM tweets1 WHERE user=%(user)s AND month=%(month)s ALLOW FILTERING',
    'SELECT * FROM tweets2 WHERE user=%(user)s AND month=%(month)s',
    'SELECT * FROM tweets3 WHERE user=%(user)s AND month=%(month)s'
]

selects_4 = [
    'SELECT * FROM tweets1 WHERE user=%(user)s AND year=%(year)s AND month=%(month)s AND day=%(day)s ALLOW FILTERING',
    'SELECT * FROM tweets2 WHERE user=%(user)s AND month=%(month)s AND year=%(year)s AND day=%(day)s ALLOW FILTERING',
    'SELECT * FROM tweets3 WHERE user=%(user)s AND month=%(month)s AND year=%(year)s AND day=%(day)s ALLOW FILTERING'
]

selects_5 = [
    'SELECT * FROM tweets1 WHERE user=%(user)s AND uuid=%(uuid)s ALLOW FILTERING',
    'SELECT * FROM tweets2 WHERE user=%(user)s AND uuid=%(uuid)s ALLOW FILTERING',
    'SELECT * FROM tweets3 WHERE user=%(user)s AND uuid=%(uuid)s ALLOW FILTERING'
]

selects_6 = [
    'SELECT * FROM tweets1 WHERE uuid=%(uuid)s ALLOW FILTERING',
    'SELECT * FROM tweets2 WHERE uuid=%(uuid)s ALLOW FILTERING',
    'SELECT * FROM tweets3_by_uuid WHERE uuid=%(uuid)s'
]

selects_7 = [
    'SELECT * FROM tweets1 WHERE tags CONTAINS %(tag)s ALLOW FILTERING',
    'SELECT * FROM tweets2 WHERE tags CONTAINS %(tag)s',
    'SELECT * FROM tweets3 WHERE tags CONTAINS %(tag)s'
]

selects_8 = [
    'SELECT * FROM tweets1 WHERE year=%(year)s AND month=%(month)s AND day=%(day)s AND tags CONTAINS %(tag)s ALLOW FILTERING',
    'SELECT * FROM tweets2 WHERE month=%(month)s AND year=%(year)s AND day=%(day)s AND tags CONTAINS %(tag)s ALLOW FILTERING',
    'SELECT * FROM tweets3 WHERE month=%(month)s AND year=%(year)s AND day=%(day)s AND tags CONTAINS %(tag)s ALLOW FILTERING'
]

selects = [
    selects_1, selects_2, selects_3, selects_4, 
    selects_5, selects_6, selects_7, selects_8
]

if len(argv) == 2 and argv[1] == 'QUORUM':
    selects = [[SimpleStatement(s, consistency_level=ConsistencyLevel.QUORUM) for s in selects_sublist] for selects_sublist in selects]

seconds = [[0.0] * 8 for i in range(3)]

start_time = time()

for i in range(3):
    for k in range(10):
        user = choice(USERS)
        year = choice(YEARS)
        month = choice(MONTHS)
        day = choice(DAYS)
        uuid = choice(UUIDS)
        tag = choice(TAGS)
        params = {
            'user': user, 'year': year, 'month': month, 
            'day': day, 'uuid': uuid, 'tag': tag
        }
        for s in range(len(selects)):
            res = session.execute(selects[s][i], params, trace=True)
            seconds[i][s] += res.get_query_trace().duration.total_seconds()

seconds = [[s/10 for s in slist] for slist in seconds]

finish_time = time()

print('   tweets1  tweets2  tweets3')
print('   -------------------------')
for i in range(len(seconds[0])):
    print(f'{i+1}| ', end='')
    for j in range(len(seconds)):
        print(round(seconds[j][i], ndigits=5), end='  ')
    print()

print()
print(f'Execution time: {round(finish_time - start_time)} seconds')