import json
import pandas as pd
from urllib.request import urlopen
from urllib.parse import quote
import sqlalchemy as sql

import interview_credentials

internal_testing = False
Head = 'ALON_test_'
total_users = 4500
chunk_size = 4500  # scalability assurance
collected_so_far = 0

def import_data():
    global data, collected_so_far
    next_batch = min(chunk_size, total_users - collected_so_far)
    collected_so_far += next_batch
    raw_json = []
    while not raw_json:
        try:
            raw_json = json.loads(
                urlopen('https://randomuser.me/api/?results=' + str(next_batch) + '&format=pretty').read())
        except:
            print('failed to catch more data. Trying again')
    return pd.json_normalize(raw_json['results'])

def get_table(table_name):
    return sql.Table(Head + table_name, sql.MetaData(), autoload=True, autoload_with=conn)

def sql_select(db_name):
    return sql.select(get_table(db_name))

def to_sql(db, db_name, if_exists='append'):
    db.to_sql(name=Head + db_name, con=conn, if_exists=if_exists, index=False)

def format_results(results):
    return pd.DataFrame(results, columns=results[0].keys())

def db_execute(function):
    db_client = conn.connect()
    return db_client.execute(function).fetchall()

# Connect to SQL DB
if internal_testing:
    engine_string = 'mysql+mysqlconnector://alon:a@localhost:3306/wixTest'
elif input('This is not a test! Press \"Y\" and ENTER to continue').upper == 'Y':
    engine_string = 'mysql+mysqlconnector://' + interview_credentials.user + ':' + quote(
        interview_credentials.password) + '@' + interview_credentials.host + ':' + interview_credentials.port + '/' + interview_credentials.database
else:
    print('What were you thinking?')
    exit()

conn = sql.create_engine(engine_string)
db_client = conn.connect()

Genders = ['male', 'female']
age_interval = 10
Age_groups = range(10,101,age_interval)
while collected_so_far < total_users:
    data = import_data()

    # 2. Split DB by gender -> ALON_test_female/male
    for gender in Genders:
        to_sql(data.loc[data['gender'] == gender], gender)

    # 3,4. Split and store DB by age -> ALON_test_<age_group>
    for age_group in Age_groups:
        to_sql(data.loc[data['dob.age'].between(age_group, age_group + age_interval, inclusive='left')],
               '{:.0f}'.format(age_group/10))

# 5. top 20 last registered from each gender -> ALON_test_20
for gender in Genders:
    db = get_table(gender)
    to_sql(
        format_results(
            db_execute(sql.select(db).order_by(sql.desc(db.columns['registered.date'])).limit(20))
        ), '20')

# 6. combine ALON_test_20 with ALON_test_5, remove duplicates, store json locally as first.json
format_results(
    db_execute(sql.union(sql_select('5'), sql_select('20')))
).to_json('first.json')

# 7. combine ALON_test_20 with ALON_test_2, keep duplicates, store json locally as second.json
format_results(
    db_execute(sql.union_all(sql_select('2'), sql_select('20')))
).to_json('second.json')
