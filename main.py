from typing import List, Tuple, Callable
from urllib.parse import quote

import pandas as pd
import requests
import sqlalchemy as sql
import sqlalchemy.engine

import interview_credentials

internal_testing = True
Head = 'ALON_test_'
total_users = 4500
max_chunk_size = 4500  # scalability assurance
collected_so_far = 0

http_adapter = requests.adapters.HTTPAdapter(max_retries=20)
session = requests.session()
session.mount('https://', http_adapter)

genders = ['male', 'female']
age_interval = 10
age_groups = range(10, 101, age_interval)


def main():
    save_splitted_users()  # requirements #1-4
    save_newest_users()  # requirement #5
    union_5and20()  # requirement #6
    unionall_2and20()  # requirement #7


def save_splitted_users():
    while collected_so_far < total_users:
        users = import_random_users()
        # 2. Split and save users by gender -> ALON_test_female/male
        for gender in genders:
            save_to_db(users.loc[users['gender'] == gender], gender)
        # 3,4. Split and save users by age -> ALON_test_<age_group>
        for age_group in age_groups:
            save_to_db(users.loc[users['dob.age'].
                       between(age_group, age_group + age_interval, inclusive='left')],
                       f'{age_group / age_interval:.0f}')


def save_newest_users():
    # 5. top 20 last registered from each gender -> ALON_test_20
    for gender in genders:
        top_users = get_table(gender)
        query = query_db(
            sql.select(top_users).order_by(sql.desc(top_users.columns['registered.date'])).limit(20))
        save_to_db(
            as_data_frame(
                query.fetchall()
            ), '20')
        ## in case more than "top 20" required, this is the scalable implementation that avoids
        ## holding all data in memory at once.
        # while True:
        #     result = query.fetchmany(max_chunk_size)
        #     if not result:
        #         break
        #     save_to_db(as_data_frame(result), '20')


def union_5and20():
    # 6. combine ALON_test_20 with ALON_test_5, remove duplicates, save json locally as first.json
    query = query_db(
        sql.union(sql_select('5'), sql_select('20')))
    write_query_results_to_json(query, 'first.json')


def unionall_2and20():
    # 7. combine ALON_test_20 with ALON_test_2, keep duplicates, save json locally as second.json
    query = query_db(
        sql.union_all(sql_select('2'), sql_select('20')))
    write_query_results_to_json(query, 'second.json')


def import_random_users() -> pd.DataFrame:
    global collected_so_far
    next_batch_size = min(max_chunk_size, total_users - collected_so_far)
    collected_so_far += next_batch_size
    raw_json = requests.get(f'https://randomuser.me/api/?results={next_batch_size}&format=pretty').json()
    if collected_so_far < total_users:
        print(f'grabbed {collected_so_far} out of {total_users}')
    else:
        print('users collection done')
    return pd.json_normalize(raw_json['results'])


def save_to_db(data: pd.DataFrame, table_name: str):
    data.to_sql(name=Head + table_name, con=engine, if_exists='append', index=False)


def query_db(query: Callable) -> List[Tuple]:
    with engine.connect() as connection:
        return connection.execute(query)


def as_data_frame(results: List[Tuple]) -> pd.DataFrame:
    return pd.DataFrame(results, columns=results[0].keys())


def get_table(table_name_suffix: str) -> sql.Table:
    return sql.Table(Head + table_name_suffix, sql.MetaData(), autoload=True, autoload_with=engine)


def sql_select(table_name_suffix: str) -> sql.select:
    return sql.select(get_table(table_name_suffix))


## for scalability purposes - json rows aggregator
# def write_query_results_to_json(cursor: sqlalchemy.engine.CursorResult, filename: str):
#     first_run = True
#     with open(filename, 'w') as f:
#         f.write('[')
#         while True:
#             result = cursor.fetchmany(max_chunk_size)
#             if not result:
#                 break
#             if first_run:
#                 first_run = False
#             else:
#                 f.write(',')
#             data_frame = as_data_frame(result)
#             batch = data_frame.to_json(orient='records')[1:-1]
#             f.write(batch)
#         f.write(']')

def write_query_results_to_json(cursor: sqlalchemy.engine.CursorResult, filename: str):
    as_data_frame(cursor.fetchall()).to_json(filename, orient='records')


if internal_testing:
    engine_string = 'mysql+mysqlconnector://alon:a@localhost:3306/wixTest'
else:
    engine_string = 'mysql+mysqlconnector://' + interview_credentials.user + ':' + quote(
        interview_credentials.password) + '@' + interview_credentials.host + ':' + interview_credentials.port + '/' + interview_credentials.database

engine = sql.create_engine(engine_string)

if __name__ == '__main__':
    main()
    print('done')
