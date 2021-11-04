import pandas as pd
from urllib.request import urlopen
from urllib.parse import quote
import json
import mysql.connector
import sqlalchemy as sql

import seperate_file as sf

Head = 'ALON_test_'

# 1. Create dataset of 4500 users
raw_json = json.loads(urlopen('https://randomuser.me/api/?results=4500&format=pretty').read())
data = pd.json_normalize(raw_json['results'])

# Connect to SQL DB
engine_string = 'mysql+mysqlconnector://' + sf.user + ':' + quote(sf.password) + '@' + sf.host + ':' + sf.port + '/' + sf.database
conn = sql.create_engine(engine_string)
# for internal testing purposes:
    # engine_string = 'mysql+mysqlconnector://alon:a@localhost:3306/wixTest'
    # conn = sql.create_engine(engine_string)
S = conn.connect()

# 2. Split DB by gender -> ALON_test_female/male
Genders = ['male','female']
for gender in Genders:
    temp = data.loc[data['gender'] == gender].to_sql(name=Head+gender,con=conn)

# 3,4. Split and store DB by age -> ALON_test_<age_group>
for age_group in range(1,11):
    temp = data.loc[data['dob.age'].between(age_group*10,(age_group+1)*10)]
    temp.to_sql(name=Head+str(age_group),con=conn)

# 5. top 20 last registered from each gender -> ALON_test_20
meta_data = sql.MetaData()
temp = pd.DataFrame()
for gender in Genders:
    db = sql.Table(Head+gender, sql.MetaData(), autoload=True, autoload_with=conn)
    cols = db.columns.values()
    # ensure registered_date kept in the same location across tables
    registered_date = 'registered.date'
    registered_date_locaion = 0
    for val in db.c:
        if registered_date in str(val):
            break
        else:
            registered_date_locaion += 1

    results = S.execute(sql.select(db).order_by(sql.desc(db.columns[registered_date_locaion])).limit(20)).fetchall()
    temp =  pd.concat([temp,pd.DataFrame(results)])

temp.columns = results[0].keys()
temp.to_sql(Head+'20',con=conn, index=False)

# 6. combine ALON_test_20 with ALON_test_5, remove duplicates, store json locally as first.json
first = pd.DataFrame()
db5 = sql.select(sql.Table(Head + '5', sql.MetaData(), autoload=True, autoload_with=conn))
db20 = sql.select(sql.Table(Head + '20', sql.MetaData(), autoload=True, autoload_with=conn))
temp = S.execute(sql.union(db5, db20)).fetchall()
first = pd.DataFrame(temp)
first.columns = temp[0].keys()
first.to_json('first.json')

# 7. combine ALON_test_20 with ALON_test_2, keep duplicates, store json locally as second.json
db2 = sql.select(sql.Table(Head + '2', sql.MetaData(), autoload=True, autoload_with=conn))
# db20 was already imported in 6
temp = S.execute(sql.union_all(db2, db20)).fetchall()
second = pd.DataFrame(temp)
second.columns = temp[0].keys()
second.to_json('second.json')