from cassandra.cluster import Cluster
from lorem import paragraph
from random import choice, choices, randint
from uuid import uuid4

USERS = [line.strip() for line in open('users.txt')]
TAGS = [line.strip() for line in open('tags.txt')]
YEARS = list(range(2013, 2020))
MONTHS = list(range(1, 12))
DAYS = list(range(1, 31))

cluster = Cluster(['localhost'])
session = cluster.connect('demoks')
insert_statement_1 = session.prepare('INSERT INTO tweets1 (user, year, month, day, uuid, tags, content) VALUES (?, ?, ?, ?, ?, ?, ?)')
insert_statement_2 = session.prepare('INSERT INTO tweets2 (user, year, month, day, uuid, tags, content) VALUES (?, ?, ?, ?, ?, ?, ?)')
insert_statement_3_a = session.prepare('INSERT INTO tweets3 (user, year, month, day, uuid, tags, content) VALUES (?, ?, ?, ?, ?, ?, ?)')
insert_statement_3_b = session.prepare('INSERT INTO tweets3_by_uuid (uuid, user, year, month, day, tags, content) VALUES (?, ?, ?, ?, ?, ?, ?)')

COUNT = 1000000

for i in range(1, COUNT + 1):
    user = choice(USERS)
    year = choice(YEARS)
    month = choice(MONTHS)
    day = choice(DAYS)
    uuid = str(uuid4())
    tags = choices(TAGS, k=randint(0,5))
    content = paragraph()
    session.execute(insert_statement_1, (user, year, month, day, uuid, tags, content))
    session.execute(insert_statement_2, (user, year, month, day, uuid, tags, content))
    session.execute(insert_statement_3_a, (user, year, month, day, uuid, tags, content))
    session.execute(insert_statement_3_b, (uuid, user, year, month, day, tags, content))
    if i % 10000 == 0:
        print(f'{i}/{COUNT}')