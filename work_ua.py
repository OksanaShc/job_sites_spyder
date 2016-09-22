import datetime
import multiprocessing
import time
from selenium import webdriver

from base import Base
from xlsx import write_to_file


def worker_runner(url):
    return WorkUaWorker(read_contacts=False).read_resume(url)


class WorkUaLogin(Base):
    LOGIN_FIELD = 'hr.rinasystems@gmail.com'
    PASSWORD_FIELD = 'HR2016'

    def login(self):
        self.driver.get('https://www.work.ua/employer/')
        self.do_click("a[href='/employer/login/'][type='button']")
        self.do_input('#email', self.LOGIN_FIELD, delay=1)
        self.do_input('#password', self.PASSWORD_FIELD)
        self.do_click("button[type='submit']")
        time.sleep(1)


class WorkUaManager(WorkUaLogin):
    def __init__(self, keyword, worker, read_contacts):
        self.data = {}
        self.keyword = keyword
        self.worker = worker
        self.driver = webdriver.Firefox()
        self.read_contacts = read_contacts

    def start_search(self):
        self.do_click("a[href='/resumes/']")
        self.do_click(".no-pull-xs.pull-right A.text-opacity")
        self.do_click("#f1", delay=1)
        self.do_click("#f2", delay=1)
        self.do_click("#f3", delay=1)
        self.do_input('#search', self.keyword, delay=1)
        # self.do_click('.input-search-city')
        # self.do_click('.js-region-reset', delay=1)
        self.do_click('button[type=submit]')

    def go_to_next_page(self):
        self.do_click('.pagination.hidden-xs li:last-child:not(.disabled)', delay=1)

    def get_resume_urls_from_page(self):
        time.sleep(1)
        resumes = self._get_list(selector='h2 a[href^="/resumes/"]')
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
        pool = multiprocessing.Pool(processes=4)
        urls_list = self.generate_urls()
        count = 0
        for resume, url in pool.imap(worker_runner, urls_list):
            self.data[url] = resume
        data = list(self.data.values())
        cols_set = set([k for d in data for k in d.keys()])
        print(cols_set)
        columns = list(cols_set)
        return data, columns


class WorkUaWorker(WorkUaLogin):
    counter = 0
    _instance = None
    vocabulary = {
        'Контактная информация': 'contacts',
        'Дополнительная информация': 'additional info',
        'Дополнительное образование': 'extra_education',
        'Образование': 'education',
        'Опыт работы': 'experience',
        'Знание языков': 'languages',
        'Профессиональные и другие навыки': 'skills and other',
        'Телефон:': 'phone',
        'Эл. почта:': 'email',
        'Готов к переезду:': 'relocate',
        'Дата рождения:': 'birthday',
        'Город:': 'city'
    }

    def __new__(cls, read_contacts=False, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(WorkUaWorker, cls).__new__(cls)
            cls.driver = webdriver.Firefox()
            cls._instance.init(read_contacts)
            cls.counter = 1
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
        # self.driver = webdriver.Firefox()
        self.read_contacts = read_contacts
        time.sleep(1)
        self.login()
        time.sleep(1)
        print('worker loginned')

    def read_resume(self, url):
        print(url)
        self.driver.get(url)
        info = dict(
            raw=self._get_text('.card.card-indent.wordwrap', delay=1),
            fullname=self._get_text('.card.card-indent.wordwrap H1.cut-top'),
            position=self._get_text('.card.card-indent.wordwrap H2'),
            cv_date=self._get_text('.card.card-indent.wordwrap .add-top span.text-muted'),
        )
        if self.read_contacts:
            self.do_click('#showContacts')
            self._get_element('.card.card-indent.wordwrap .list-inline')

        meta = self._get_list('.card.card-indent.wordwrap dl > *')
        for i in range(0, len(meta), 2):
            info[meta[i].text] = meta[i + 1].text

        children = self._get_list('.card.card-indent.wordwrap > *')
        block = ''
        text = ''
        for child in children:
            if child.tag_name.upper() == 'H2':
                if block:
                    info[block] = text
                text = ''
                block = child.text
            else:
                if block:
                    text += ' ' + child.text
        info[block] = text
        resume = {self.vocabulary.get(k, k): v for k, v in info.items()}
        return resume, url


if __name__ == "__main__":
    start = datetime.datetime.now()
    c = WorkUaManager(keyword='python', worker=worker_runner, read_contacts=False)
    data, columns = c.process()
    write_to_file('work.xlsx', data, columns)
    end = datetime.datetime.now()
    print('start: %s, end: %s. total: %s' % (start, end, end - start))
