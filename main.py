import MySQLdb as sql
import pandas as pd
import json
import math
import os
from natsort import natsorted


def read_dataset(file_path):
    file_object = open(file_path, 'r', encoding="utf-8")
    file_content = file_object.read()
    dataset_content = json.loads(file_content)
    res = pd.DataFrame.from_dict(dataset_content, orient='index')
    res = res.reindex(index=natsorted(res.index))
    return res


def get_exists_users(con, df):
    u = pd.read_sql('SELECT * FROM mdl_user', con, index_col='id').reset_index()
    res = pd.merge(left=u, right=df, how='inner', on=['email', 'firstname', 'lastname']).set_index('id')
    res = res[['username', 'firstname', 'lastname', 'patronymic', 'email']]
    return res


def get_not_exists_users(con, df):
    u = pd.read_sql('SELECT * FROM mdl_user', con, index_col='id').reset_index()
    res = pd.merge(left=u, right=df, how='right', on=['email', 'firstname', 'lastname'])
    res = res[res.id.isnull()][['firstname', 'lastname', 'patronymic', 'email']].reset_index(drop=True)
    return res


def get_dirty_ip(con, users_id, action, ip, un):
    res = pd.read_sql(
        """
          SELECT
            ipun.ip,
            ipun.un,
            ipun.urs
          FROM
          (
            SELECT
                uip.ip,
                COUNT(uip.ip) un,
                GROUP_CONCAT(uip.uid SEPARATOR ', ') AS urs
            FROM
            (
                SELECT
                    l.userid AS uid,
                    l.ip
                FROM
                    mdl_log l
                WHERE
                    l.action LIKE %s AND
                    l.ip NOT LIKE %s AND
                    l.userid IN %s
                GROUP BY
                    l.userid,
                    l.ip
            ) uip
            GROUP BY
                uip.ip
          ) ipun
          WHERE
            ipun.un > %s
        """,
        con,
        index_col='ip',
        params=[action, ip, tuple(users_id), un]
    )
    return res


def match_user_ip(udf, ipdf):
    ip = []
    for uid in udf.index:
        res = ipdf.reset_index().apply(lambda y: y.ip if str(uid) in y.urs else None, axis=1)
        res = res[res.notnull()]
        res = ', '.join(list(res)) if not res.empty else ''
        ip.append(res)
    res = udf
    res['ip'] = ip
    return res


def get_courses(con, ip_list, id_list, action):
    res = pd.read_sql(
        """
          SELECT
            t.userid AS uid,
            GROUP_CONCAT(t.course SEPARATOR ', ') AS course,
            GROUP_CONCAT(t.ip SEPARATOR ', ') AS ip
          FROM
            (
                SELECT
                    *
                FROM
                    mdl_log AS l
                WHERE
                    l.ip IN %s AND
                    l.userid IN %s AND
                    l.action LIKE %s
                GROUP BY
                    l.ip,
                    l.userid,
                    l.course
            ) AS t
          GROUP BY
            t.userid
        """,
        con,
        index_col='uid',
        params=[tuple(ip_list), tuple(id_list), action]
    )
    return res


if __name__ == "__main__":
    online = sql.connect(
        host='172.16.8.31',
        port=3306,
        user='aio',
        passwd='acw-6l8q',
        db='online',
        charset='utf8',
        init_command='SET NAMES UTF8'
    )

    dir_root = os.path.dirname(__file__)
    file_path = os.path.join(dir_root, "dataset.json")
    output_path = os.path.join(dir_root, "output.json")

    df = read_dataset(file_path)
    eu = get_exists_users(online, df)
    neu = get_not_exists_users(online, df)

    action = '%attempt%'
    ip = '172.16.[89].%'
    users_id = eu.index
    un = 3

    ipdf = get_dirty_ip(online, users_id, action, ip, un)

    cdf = get_courses(online, list(ipdf.index), list(eu.index), action)

    res = pd.merge(left=eu, right=cdf, how='inner', left_on=eu.index, right_on=cdf.index, left_index=True)
    res.drop('key_0', axis='columns', inplace=True)

    with open(output_path, 'w', encoding='utf-8') as file:
        res.to_json(file, orient='index', force_ascii=False)
