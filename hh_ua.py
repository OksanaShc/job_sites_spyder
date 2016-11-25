import re
import multiprocessing
import time
from selenium import webdriver

from base import Base


# def worker_runner(url):
#     return HHWorker(read_contacts=False).read_resume(url)


class HHLogin(Base):
    LOGIN_FIELD = 'hr.rinasystems@gmail.com'
    PASSWORD_FIELD = 'H20R16'
    table = 'hh1'

    def login(self):
        self.driver.get('https://hh.ua/account/login')
        self.do_input('[data-qa="login-input-username"]', self.LOGIN_FIELD, delay=1)
        self.do_input('[data-qa="login-input-password"]', self.PASSWORD_FIELD)
        self.do_click('[data-qa="account-login-submit"]')


class HHManager(Base):

    table = 'hh1'
    _instance = None
    date_re = re.compile('\d+.\d+.\d+')
    LOGIN_FIELD = 'hr.rinasystems@gmail.com'
    PASSWORD_FIELD = 'H20R16'


    def __new__(cls, keyword='', read_contacts=False, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(HHManager, cls).__new__(cls)
            # cls.driver = webdriver.Firefox()
            cls._instance.init(keyword, read_contacts)
            cls.counter = 0
        if cls.counter < 11:
            cls.counter += 1
            return cls._instance
        else:
            cls.driver.quit()
            cls.driver = webdriver.Firefox()
            cls._instance.init(keyword, read_contacts)
            cls.counter = 1
            return cls._instance

    def __init__(self, keyword, read_contacts):
        pass

    def init(self, keyword, read_contacts):
        super().__init__()
        self.data = {}
        self.keyword = keyword
        self.read_contacts = read_contacts
        time.sleep(1)
        self.login()
        print('loginned')

    def read_resume(self, url):
        print(url)
        self.driver.get(url)
        cv_date = self.date_re.findall(self._get_text('.resume-header-additional__update-date') or '')

        info = dict(
            birthday=self._get_element_attribute('[data-qa="resume-personal-birthday"]', attribute='content', delay=2),
            age=self._get_text('[data-qa="resume-personal-age"]'),
            city=self._get_text('[data-qa="resume-personal-address"]'),
            cv_added= '' if not cv_date else cv_date[0],
            experience=self._get_text('[data-qa="resume-block-experience"]'),
            education=self._get_text('[data-qa="resume-block-education"]'),
            languages=self._get_text('[data-qa="resume-block-languages"]'),
            skills=self._get_text('[data-qa="skills-table"]'),
            about=self._get_text('[data-qa="resume-block-skills"]'),
            salary=self._get_text('[data-qa="resume-block-salary"]'),
            position=self._get_text('[data-qa="resume-block-title-position"]'),
            url=url
        )
        resume = {k: v for k, v in info.items() if v is not None}
        return resume, url

    def login(self):
        self.driver.get('https://hh.ua/account/login')
        self.do_input('[data-qa="login-input-username"]', self.LOGIN_FIELD, delay=1)
        self.do_input('[data-qa="login-input-password"]', self.PASSWORD_FIELD)
        self.do_click('[data-qa="account-login-submit"]')

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
        count = 0
        existing_urls = [url['url'] for url in self.db.find({}, projection={'_id': 0, 'url': 1})]
        while count < 2:
            for resume_url in self.get_resume_urls_from_page():
                if resume_url in existing_urls:
                    continue
                yield resume_url
            try:
                self.go_to_next_page()
                count +=1
            except Exception as e:
                print(e)
                break

    def process(self):
        urls_list = list(self.generate_urls())
        count = 0
        for url in urls_list:
            resume, url = self.read_resume(url)
            if resume:
                self.data[url] = resume
                count += 1
        for resume in self.data.values():
            self.save_item(resume)
        self.driver.close()

if __name__ == "__main__":
    start = time.time()
    c = HHManager(keyword='python', read_contacts=False)
    c.process()
    c.write_file()
    end = time.time()
    print('delay: %s' % round(end - start, 2))
