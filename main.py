import pandas as pd
from urllib.request import urlopen
from urllib.parse import quote
import json
import mysql.connector
import sqlalchemy as sql

import interview_credentials

Head = 'ALON_test_'
total_users = 4500
chunk_size = 4500  # scalability assurance
collected_so_far = 0


def import_data():
    global data, collected_so_far
    next_batch = min(chunk_size, total_users - collected_so_far)
    collected_so_far += next_batch
    raw_json =[]
    while not raw_json:
        try:
            raw_json = json.loads(urlopen('https://randomuser.me/api/?results=' + str(next_batch) + '&format=pretty').read())
        except:
            print('failed to catch more data. Trying again')
    return pd.json_normalize(raw_json['results'])

def sql_select(db_name):
    return sql.select(sql.Table(Head + db_name, sql.MetaData(), autoload=True, autoload_with=conn))

def format_results(results):
    return pd.DataFrame(results, columns=results[0].keys())


# Connect to SQL DB
engine_string = 'mysql+mysqlconnector://' + interview_credentials.user + ':' + quote(
    interview_credentials.password) + '@' + interview_credentials.host + ':' + interview_credentials.port + '/' + interview_credentials.database
conn = sql.create_engine(engine_string)
# for internal testing:
# engine_string = 'mysql+mysqlconnector://alon:a@localhost:3306/wixTest'
# conn = sql.create_engine(engine_string)
db_client = conn.connect()

while collected_so_far < total_users:
    data = import_data()

    # 2. Split DB by gender -> ALON_test_female/male
    Genders = ['male', 'female']
    for gender in Genders:
        temp = data.loc[data['gender'] == gender].to_sql(name=Head + gender, con=conn, if_exists='append')

    # 3,4. Split and store DB by age -> ALON_test_<age_group>
    for age_group in range(1, 11):
        data.loc[data['dob.age'].between(age_group * 10, (age_group + 1) * 10, inclusive='left')].to_sql(
            name=Head + str(age_group), con=conn, if_exists='append')

# 5. top 20 last registered from each gender -> ALON_test_20
meta_data = sql.MetaData()
for gender in Genders:
    db = sql.Table(Head + gender, sql.MetaData(), autoload=True, autoload_with=conn)
    format_results(db_client.execute(
        sql.select(db).order_by(sql.desc(db.columns['registered.date'])).limit(20)).fetchall()).to_sql(Head + '20',
                                                                                           con=conn,
                                                                                           index=False,
                                                                                           if_exists='append')

# 6. combine ALON_test_20 with ALON_test_5, remove duplicates, store json locally as first.json
format_results(db_client.execute(sql.union(sql_select('5'), sql_select('20'))).fetchall()).to_json('first.json')

# 7. combine ALON_test_20 with ALON_test_2, keep duplicates, store json locally as second.json
format_results(db_client.execute(sql.union_all(sql_select('2'), sql_select('20'))).fetchall()).to_json('second.json')
