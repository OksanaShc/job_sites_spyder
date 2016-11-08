import re
import redis
import json
import multiprocessing
import time
import datetime
from selenium import webdriver
from base import Base
from xlsx import write_to_file


def worker_runner(url):
    return RabotauaWorker(read_contacts=True).read_resume(url)


class RabotaUaLogin(Base):
    LOGIN_FIELD = 'hr.rinasystems@gmail.com'
    PASSWORD_FIELD = '2016HR'

    def login(self):
        self.driver.get('http://rabota.ua/employer/login')
        self.do_input('#centerZone_ZoneLogin_txLogin', self.LOGIN_FIELD, delay=1)
        self.do_input('#centerZone_ZoneLogin_txPassword', self.PASSWORD_FIELD)
        self.do_click("#centerZone_ZoneLogin_btnLogin")

    def __del__(self):
        self.driver.quit()


class RabotaUAManager(RabotaUaLogin):

    def __init__(self, keyword, worker, read_contacts):
        self.data = {}
        self.keyword = keyword
        self.worker = worker
        self.driver = webdriver.Firefox()
        self.read_contacts = read_contacts
        self.redis = redis.Redis(db='10')
        self.redis_hashname = 'rabota'

    def start_search(self):
        self.do_click("//a[contains(text(), 'Найти резюме')]", type='xpath')
        self.do_input('#beforeContentZone_Main2_txtKeywords', self.keyword, delay=1)
        self.do_click('a#search')
        self.do_click('''select[data-bind="options: PeriodOptions, optionsText: 'Name', optionsValue: 'Key', value: SelectedPeriod"]''', delay=1)
        self.do_click('''select[data-bind="options: PeriodOptions, optionsText: 'Name', optionsValue: 'Key', value: SelectedPeriod"] option[value = '5']''', delay=1)

    def get_resume_urls_from_page(self):
        time.sleep(1)
        resumes = self._get_list(selector='h3 a[href^="/cv/"]')
        return [resume.get_attribute("href") for resume in resumes]

    def go_to_next_page(self):
        self.do_click('a.pager-next.pager-next-enabled', delay=1)

    def generate_urls(self):
        self.start_search()
        while True:
            for resume_url in self.get_resume_urls_from_page():
                if self.redis.hget(self.redis_hashname, resume_url):
                    continue
                yield resume_url
            try:
                self.go_to_next_page()
            except Exception as e:
                print(e)
                break

    def process(self):
        self.login()
        pool = multiprocessing.Pool(processes=5)
        urls_list = self.generate_urls()
        count = 0
        for resume, url in pool.imap(worker_runner, urls_list):
            self.redis.hset(self.redis_hashname, url, json.dumps(resume))
            # count +=1
            # if count == 5:
            #     break

        redis_data = self.redis.hgetall(self.redis_hashname)
        data = [json.loads(v.decode(encoding='utf-8')) for v in list(redis_data.values())]
        cols_set = set([k for d in data for k in d.keys()])
        print(cols_set)
        columns = list(cols_set)
        return data, columns


class RabotauaWorker(RabotaUaLogin):
    _instance = None
    data_reg = re.compile('\s*<span.*?>•</span>\s*', re.I + re.S)
    vocabulary = {
        'AimHolder': 'Aim',
        'SkillsHolder': 'Skills',
        'ExperienceHolder': 'Experience',
        'EducationHolder': 'Education',
        'LanguagesHolder': 'Languages',
        'TrainingsHolder': 'Trainings',
        'AdditionalInfoHolder': 'Additional Info'
    }

    def __new__(cls, read_contacts=False, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(RabotauaWorker, cls).__new__(cls)
            cls._instance.init(read_contacts)
        return cls._instance

    def init(self, read_contacts):
        self.driver = webdriver.Firefox()
        self.read_contacts = read_contacts
        self.login()
        time.sleep(1)
        print('worker loginned')

    def get_core_info(self):
        info = {}
        coreinfo = self._get_list('.rua-g-clearfix .rua-p-t_12')
        if not coreinfo:
            return info
        html = coreinfo[0].get_attribute('innerHTML')

        parts = self.data_reg.split(html.replace('&nbsp', '').replace(';', '').strip())
        for p in parts:
            p = ''.join(filter(lambda l: l.isalnum() or l == '$', p))
            if p.isalpha():
                info['city'] = p
            elif 'years' in p or 'лет' in p or 'год'in p or 'рок' in p or 'рiк':
                info['age'] = p
            else:
                info['payment'] = p
        return info

    def read_resume(self, url):
        print(url)
        info = {}
        self.driver.get(url)
        info['url'] = url
        info['name'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblName')
        info['position'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_txtJobName')

        info['cv_date'] = self._get_text('.cvheadnav .muted').replace('резюме обновлено ', '')
        info.update(self.get_core_info())

        fields = self._get_list('.cvtexts > div')

        for field in fields:
            if field.text and field.get_attribute('id'):
                info[field.get_attribute('id')] = field.text

        if self.read_contacts:
            try:
                self.do_click('#centerZone_BriefResume1_CvView1_cvHeader_lnkOpenContact')
            except:
                print('No id centerZone_BriefResume1_CvView1_cvHeader_lnkOpenContact in page')
            info['birthday'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblBirthDateValue')
            info['email'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblEmailValue')
            info['region'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblRegionValue')
            info['phone'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblPhoneValue')

        resume = {self.vocabulary.get(k, k): v for k, v in info.items()}
        return resume, url


if __name__ == "__main__":
    start = datetime.datetime.now()
    c = RabotaUAManager(keyword='python', worker=worker_runner, read_contacts=True)
    data, columns = c.process()
    write_to_file('rabota_%s.xlsx' % time.time(), data, columns)
    end = datetime.datetime.now()
    print('start: %s, end: %s. total: %s' % (start, end, end - start))
