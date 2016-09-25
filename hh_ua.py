import datetime
import multiprocessing
import time
from selenium import webdriver

from base import Base
from xlsx import write_to_file


def worker_runner(url):
    return HHWorker(read_contacts=False).read_resume(url)


class HHLogin(Base):
    LOGIN_FIELD = 'hr.rinasystems@gmail.com'
    PASSWORD_FIELD = 'H20R16'

    def login(self):
        self.driver.get('https://hh.ua/account/login')
        self.do_input('[data-qa="login-input-username"]', self.LOGIN_FIELD, delay=1)
        self.do_input('[data-qa="login-input-password"]', self.PASSWORD_FIELD)
        self.do_click('[data-qa="account-login-submit"]')


class HHManager(HHLogin):
    def __init__(self, keyword, worker, read_contacts):
        self.data = {}
        self.keyword = keyword
        self.worker = worker
        self.driver = webdriver.Firefox()
        self.read_contacts = read_contacts

    def start_search(self):
        self.do_input('[data-qa="resume-serp__query"]', self.keyword, delay=1)
        self.do_click('[data-hh-tab-id="resumeSearch"] button[data-qa="navi-search__button"]')
        self.do_click('[data-qa="serp-settings__search-period"]', delay=1)
        self.do_click('[data-qa="select-period-365"]', delay=1)

    def go_to_next_page(self):
        self.do_click('[data-qa="pager-next"]', delay=1)

    def get_resume_urls_from_page(self):
        time.sleep(1)
        resumes = self._get_list(selector='[data-qa="resume-serp__resume-title"]')
        return [resume.get_attribute("href") for resume in resumes]

    def generate_urls(self):
        self.start_search()
        while True:
            for resume_url in self.get_resume_urls_from_page():
                yield resume_url
            try:
                self.go_to_next_page()
            except Exception as e:
                print(e)
                break

    def process(self):
        self.login()
        pool = multiprocessing.Pool(processes=1)
        urls_list = self.generate_urls()
        count = 0
        for resume, url in pool.imap(worker_runner, urls_list):
            self.data[url] = resume
            # count += 1
            # if count == 20:
            #     break
        data = list(self.data.values())
        cols_set = set([k for d in data for k in d.keys()])
        print(cols_set)
        columns = list(cols_set)
        return data, columns


class HHWorker(HHLogin):
    _instance = None

    def __new__(cls, read_contacts=False, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(HHWorker, cls).__new__(cls)
            cls.driver = webdriver.Firefox()
            cls._instance.init(read_contacts)
            cls.counter = 0
        if cls.counter < 11:
            cls.counter += 1
            return cls._instance
        else:
            cls.driver.quit()
            cls.driver = webdriver.Firefox()
            cls._instance.init(read_contacts)
            cls.counter = 1
            return cls._instance

    def init(self, read_contacts):
        self.read_contacts = read_contacts
        time.sleep(1)
        self.login()
        print('worker loginned')

    def read_resume(self, url):
        print(url)
        self.driver.get(url)
        info = dict(
            age=self._get_text('[data-qa="resume-personal-age"]'),
            city=self._get_text('[data-qa="resume-personal-address"]'),
            cv_added=self._get_text('.resume-header-additional__update-date'),
            experience=self._get_text('[data-qa="resume-block-experience"]'),
            education=self._get_text('[data-qa="resume-block-education"]'),
            languages=self._get_text('[data-qa="resume-block-languages"]'),
            skills=self._get_text('[data-qa="skills-table""]'),
            about=self._get_text('[data-qa="resume-block-skills"]'),
            salary=self._get_text('[data-qa="resume-block-salary"]'),
            position=self._get_text('[data-qa="resume-block-title-position"]'),
        )
        resume = {k: v for k, v in info.items() if v is not None}
        return resume, url


if __name__ == "__main__":
    start = datetime.datetime.now()
    c = HHManager(keyword='python', worker=worker_runner, read_contacts=False)
    data, columns = c.process()
    write_to_file('hh.xlsx', data, columns)
    end = datetime.datetime.now()
    print('start: %s, end: %s. total: %s' % (start, end, end - start))
