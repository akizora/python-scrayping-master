import traceback
import bs4
import requests
import re
import sys
import config
import pymysql
import typing
from urllib.parse import urlparse


def lambda_handler(event, context):
    with ListCrawler() as lc:
        url_list = lc.get_url_list_from_xml()
        print(len(url_list))
        url_tuple_list = lc.convert_tuple_list(data_list=url_list)
        detail_url_list_group = lc.split_list(data_list=url_tuple_list, group_count=300)
        for detail_url_list in detail_url_list_group:
            lc.insert_urls(detail_url_list)


class ListCrawler():
    """スクレイピング対象のURLリストを取得する.
    """
    def __init__(self) -> None:
        self.DB_HOST = config.DB_HOST
        self.DB_USER = config.DB_USER
        self.DB_PW = config.DB_PW
        self.DB_NAME = config.DB_NAME
        self.DB_TABLE_1 = config.DB_TABLE_1
        self.XML_PATH = config.XML_PATH

        self.conn = pymysql.connect(
            host=self.DB_HOST,
            user=self.DB_USER, 
            password=self.DB_PW, 
            db=self.DB_NAME,
            charset='utf8'
        )
        return

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        print('exit')
        self.conn.close()
        return self
    
    def get_target_urls(self) ->typing.List[tuple]:
        print('get_target_urls')
        select_sql = "SELECT id, original_url FROM " \
        " " + self.DB_NAME + "."  + self.DB_TABLE_1 +  \
        " WHERE 1=1 " + \
        " AND " + self.DB_NAME + "." + self.DB_TABLE_1 + ".is_get=0" + \
        " ORDER BY prefecture desc ;"

        cur = self.conn.cursor()
        cur.execute(select_sql)
        results = cur.fetchall()

        target_urls = []
        for result in results:
            target_urls.append(result)
        return target_urls


    def get_url_list_from_xml(self):
        """XMlからURLリストを取得する.

        Returns:
            _type_: _description_
        """
        job_detail_url_list = []
        try:
            with open(self.XML_PATH) as doc:
                sitemap_soup = bs4.BeautifulSoup(doc, 'lxml-xml')
                job_detail_job_elem_list = sitemap_soup.find_all('job')
            for job_detail_job_elem in job_detail_job_elem_list:
                data = {}
                url = job_detail_job_elem.select_one('url').get_text()
                referencenumber = job_detail_job_elem.select_one('referencenumber').get_text()
                data['url'] = url
                data['referencenumber'] = referencenumber
                job_detail_url_list.append(data)
        except Exception as e:
            print(e)
        return job_detail_url_list


    def update_urls(self, db_id_list):
        print('Bulk Update!')
        sql = "UPDATE "  + self.DB_TABLE_1 + " SET is_get = 1 WHERE id IN (%s)"
        cur = self.conn.cursor()
        cur.executemany(sql, db_id_list)
        self.conn.commit()


    def split_list(self, data_list, group_count):
        splited_group_list = []
        for i in range(0, len(data_list), group_count):
            splited_group_list.append(data_list[i: i + group_count])
        return splited_group_list


    def convert_tuple_list(self, data_list):
        tuple_list = []
        for data in data_list:
            url = data['url']
            url = url.split('?')[0]
            referencenumber = data['referencenumber']
            parse = urlparse(url)
            prefecture = str(parse.path).split('/')[1]
            tuple_list.append((referencenumber, url, prefecture))
        return tuple_list


    def insert_urls(self, data_list):
        print("Insert Bulk Data!")
        insert_sql = "INSERT INTO " \
        " " + self.DB_NAME + "." + self.DB_TABLE_1 + \
        " (referencenumber, original_url, prefecture) values (%s, %s, %s);"
        cur = self.conn.cursor()
        print(cur)
        cur.executemany(insert_sql, data_list)
        self.conn.commit()


if __name__ == '__main__':
    lambda_handler({},{})
