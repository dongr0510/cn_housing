# encoding:utf-8

import pandas as pd
import time
import pymysql
import warnings
import logging
from datetime import date
from .beike import BeikeParser
from .anjuke import AnjukeParser
from .ganji import GanjiParser
from .lianjia import LianjiaParser
from .tongcheng import TongchengParser


class saveData():
    '''
    用于保存数据
    '''

    def __init__(self, config):
        self._config = config
        self.key_password_dict = {}
        self._get_user_password()
        self.gemini_conn, self.gemini_cursor = self._get_db_cursor(database='Gemini')

    def _get_user_password(self):
        file_path = "/root/Software/Config/mysql_password.txt"
        f = open(file_path, "r")
        for x in f:
            key_password_pair = x.strip().split(":")
            self.key_password_dict[key_password_pair[0]] = key_password_pair[1]
        self.host = self.key_password_dict['host']
        self.port = int(self.key_password_dict['port'])
        self.user = self.key_password_dict['user']
        self.passwd = self.key_password_dict['passwd']
        print(f"self.host is {self.host}")
        print(f"self.port is {self.port}")
        print(f"self.user is {self.user}")
        return
    
    def _get_db_cursor(self, database):
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            db=database,
            charset='utf8'
            )
        cursor = conn.cursor()
        return conn, cursor

    # 清理mysql数据
    def _delete_mysql(self, database):
        # 用于忽略表已存在的警告
        warnings.filterwarnings("ignore")
        host = self.key_password_dict['host']
        port = int(self.key_password_dict['port'])
        user = self.key_password_dict['user']
        passwd = self.key_password_dict['passwd']

        conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=database, charset='utf8')
        cursor = conn.cursor()
        timestring = time.strftime('%Y%m%d%H', time.localtime(time.time()))
        tablename = 'T' + timestring + 'TheFutureOfHome'
        drop_sql = """drop table IF EXISTS %s""" % (tablename)
        drop_rows = cursor.execute(drop_sql)
        print('delete ' + str(drop_rows) + ' rows.')

        conn.commit()
        cursor.close()
        conn.close()

    # 保存到mysql
    def _save_mysql(
            self,
            db_conn,
            db_cursor: pymysql.cursors.Cursor,
            table_name,
            data: pd.DataFrame,
            primary_key_columns,
            logger=logging.getLogger(),
            ):
        data_cols = data.columns.to_list()
        not_primary_key_columns = [
            x for x in data_cols if x not in primary_key_columns
            ]
        insert_sql = f"""
            insert into {table_name}
            (
                {",".join(data_cols)}
            )
                values """

        for row_num, col_value in data.iterrows():
            if row_num != 0:
                insert_sql += ","
            insert_sql += str(tuple(col_value.values.tolist()))

        insert_sql += 'AS new ON DUPLICATE KEY UPDATE '

        max_column_number = len(not_primary_key_columns) - 1
        for idx in range(len(not_primary_key_columns)):
            not_primary_key = not_primary_key_columns[idx]
            insert_sql += not_primary_key + "=" + f"new.{not_primary_key}"
            if idx != max_column_number:
                insert_sql += ','
        db_cursor.execute(insert_sql)
        db_conn.commit()
        return

    def _transfer_beike_saving_data(self, data_dict):
        current_date = str(date.today())
        res = pd.DataFrame(
            {
                'CalendarDate': current_date,
                'webName': '贝壳',
                'houseName': data_dict['houseName'],
                'villageName': data_dict['villageName'],
                'houseNote': data_dict['houseNote'],
                'houseTotalPrice': data_dict['houseTotalPrice'],
                'houseUnitPrice': data_dict['houseUnitPrice'],
                'houseLink': data_dict['houseLink'],
                'houseImg': data_dict['houseImg'],
                'followNum': data_dict['followNum'],
                'city': data_dict['city'],
            }
        )
        res['houseTotalPrice'] = res['houseTotalPrice'].apply(
            lambda x: float(x[:-1]) * 10000
            )
        res['houseUnitPrice'] = res['houseUnitPrice'].apply(
            lambda x: float(x[:-3].replace(',', ''))
            )
        return res

    def deleteOldData(self):
        # if self._config['savetype']['type'] == 'mysql':
        self._delete_mysql()
        # elif self._config['savetype']['type'] == 'leancloud':
        #     self._delete_leancloud()

    def _saveData(self, *args, **kwargs):
        # if self._config['savetype']['type'] == 'mysql':
        self._save_mysql(*args, **kwargs)
        # elif self._config['savetype']['type'] == 'leancloud':
        #     self._save_leancloud(*args)

    # 贝壳找房
    def beike_save(self, html, first_page_url, table_name, primary_key):
        beike = BeikeParser(
            html_data=html,
            first_page_url=first_page_url,
        )
        houseName, \
            villageName, \
            houseNote, \
            houseTotalPrice, \
            houseUnitPrice, \
            houseLink, \
            houseImg, \
            followNum, \
            city = beike.feed()
        data_dict = {
            'webName': '贝壳',
            'houseName': houseName,
            'villageName': villageName,
            'houseTotalPrice': houseTotalPrice,
            'houseUnitPrice': houseUnitPrice,
            'houseLink': houseLink,
            'houseImg': houseImg,
            'houseName': houseName,
            'followNum': followNum,
            'houseNote': houseNote,
            'city': city,
        }
        data_df = self._transfer_beike_saving_data(
            data_dict=data_dict,
        )
        print('Finish Parsing HTML Data')
        self._saveData(
                db_conn=self.gemini_conn,
                db_cursor=self.gemini_cursor,
                table_name=table_name,
                data=data_df,
                primary_key_columns=primary_key,
            )
        print("Finish Saving Data")

    # 链家
    def lianjia_save(self, html):
        lianjia = LianjiaParser()
        houseName, villageName, houseNote, houseTotlePrice, houseUnitPrice, houseLink, houseImg, followNum = lianjia.feed(html)
        self._saveData('链家', houseName, villageName, houseNote, houseTotlePrice, houseUnitPrice, houseLink, houseImg, followNum)

    # 58同城
    def tongcheng_save(self, html):
        tongcheng = TongchengParser()
        houseName, villageName, houseNote, houseTotlePrice, houseUnitPrice, houseLink, houseImg, followNum = tongcheng.feed(html)
        self._saveData('58同城', houseName, villageName, houseNote, houseTotlePrice, houseUnitPrice, houseLink, houseImg, followNum)

    # 安居客
    def anjuke_save(self, html):
        anjuke = AnjukeParser()
        houseName, villageName, houseNote, houseTotlePrice, houseUnitPrice, houseLink, houseImg, followNum = anjuke.feed(html)
        self._saveData('安居客', houseName, villageName, houseNote, houseTotlePrice, houseUnitPrice, houseLink, houseImg, followNum)

    # 赶集
    def ganji_save(self, html):
        ganji = GanjiParser()
        houseName, villageName, houseNote, houseTotlePrice, houseUnitPrice, houseLink, houseImg, followNum = ganji.feed(html)
        self._saveData('赶集', houseName, villageName, houseNote, houseTotlePrice, houseUnitPrice, houseLink, houseImg, followNum)
