# import sys
# import typing
# import traceback
# import requests
import bs4
import config
import pymysql

def lambda_handler(event, context):
    with DetailCrawler() as dc:
        job_list = dc.scrayping()
        print(len(job_list))
        job_list_splited = dc.split_list(data_list=job_list, group_count=300)
        for job_list in job_list_splited:
            dc.insert_jobs(job_list)


class DetailCrawler():
    """スクレイピング後情報を保存する.
    """
    def __init__(self) -> None:
        self.DB_HOST = config.DB_HOST
        self.DB_USER = config.DB_USER
        self.DB_PW = config.DB_PW
        self.DB_NAME = config.DB_NAME
        self.DB_TABLE_1 = config.DB_TABLE_1
        self.DB_TABLE_2 = config.DB_TABLE_2
        self.DB_COLUMN_LIST = config.DB_COLUMN_LIST
        self.XML_PATH = config.XML_PATH

        self.TEMPLATE_NAME = config.TEMPLATE_NAME
        self.AGENT_ID = config.AGENT_ID

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


    def scrayping(self):
        job_detail_job_elem_list = []
        with open(self.XML_PATH) as doc:
            sitemap_soup = bs4.BeautifulSoup(doc, 'lxml-xml')
            job_detail_job_elem_list = sitemap_soup.find_all('job')

        data_list = []

        for job_detail_job_elem in job_detail_job_elem_list:
            data = []
            title = ''
            stanby_title = ''
            date = ''
            referencenumber = ''
            url = ''
            company = ''
            city = ''
            state = ''
            country = 'JP'
            station = ''
            access = ''
            catchcopy = ''
            background = ''
            businessContents = ''
            companyAddress = ''
            description1 = ''
            description2 = ''
            description3 = ''
            description4 = ''
            description5 = ''
            description6 = ''
            salary = ''
            salary_shaped = ''
            education = config.EDUCATION_DEFAULT
            jobinformation = ''
            trafficaccess = ''
            jobtype = ''
            category = ''
            experience = config.EDUCATION_DEFAULT
            imageurls = ''
            extras = '{}'
            template = self.TEMPLATE_NAME
            industry = '0'
            job_cat = '0'
            agentid = self.AGENT_ID
            originalUrl = ''
            version = '1'
            indeedFlag = '0'
            stanby_flag = '1'
            repost_group = '1'
            indeed_sended_flag = '0'
            pop = self.TEMPLATE_NAME
            is_deleted = '0'
            pattern = '0'
            is_parrent = '1'


            title += job_detail_job_elem.select_one('title').get_text()
            stanby_title += title
            referencenumber += job_detail_job_elem.select_one('referencenumber').get_text()
            url += job_detail_job_elem.select_one('url').get_text()
            originalUrl = url
            company += job_detail_job_elem.select_one('company').get_text()
            city += job_detail_job_elem.select_one('city').get_text()
            state += job_detail_job_elem.select_one('state').get_text()
            companyAddress += state + city
            station += job_detail_job_elem.select_one('station').get_text()
            trafficaccess += station
            access += station
            description1 += job_detail_job_elem.select_one('description').get_text()
            jobinformation += description1
            catchcopy += description1
            salary += job_detail_job_elem.select_one('salary').get_text()
            salary_shaped += salary
            jobtype += job_detail_job_elem.select_one('jobtype').get_text()
            category += job_detail_job_elem.select_one('category').get_text()
            imageurls += job_detail_job_elem.select_one('imageUrls').get_text()

            data.append(title)
            data.append(stanby_title)
            data.append(date)
            data.append(referencenumber)
            data.append(url)
            data.append(company)
            data.append(city)
            data.append(state)
            data.append(country)
            data.append(station)
            data.append(access)
            data.append(catchcopy)
            data.append(background)
            data.append(businessContents)
            data.append(companyAddress)
            data.append(description1)
            data.append(description2)
            data.append(description3)
            data.append(description4)
            data.append(description5)
            data.append(description6)
            data.append(salary)
            data.append(salary_shaped)
            data.append(education)
            data.append(jobinformation)
            data.append(trafficaccess)
            data.append(jobtype)
            data.append(category)
            data.append(experience)
            data.append(imageurls)
            data.append(extras)
            data.append(template)
            data.append(industry)
            data.append(job_cat)
            data.append(agentid)
            data.append(originalUrl)
            data.append(version)
            data.append(indeedFlag)
            data.append(stanby_flag)
            data.append(repost_group)
            data.append(indeed_sended_flag)
            data.append(pop)
            data.append(is_deleted)
            data.append(pattern)
            data.append(is_parrent)

            data = tuple(data)
            data_list.append(data)

        return data_list


    def insert_jobs(self, data_list):
        """SQL生成してInsert

        Args:
            data_list (_type_): _description_
        """
        print("Insert Bulk Job Data!")
        sql = 'INSERT INTO ' + self.DB_NAME +  '.' + self.DB_TABLE_2 + ' ('
        for column in self.DB_COLUMN_LIST:
            sql += column
        sql += ') VALUES ('
        for i in range(len(self.DB_COLUMN_LIST)):
            sql += "%s,"
        sql += ';' 
        cur = self.conn.cursor()
        cur.executemany(sql, data_list)
        self.conn.commit()


    def split_list(self, data_list, group_count):
        splited_group_list = []
        for i in range(0, len(data_list), group_count):
            splited_group_list.append(data_list[i: i + group_count])
        return splited_group_list


