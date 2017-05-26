import re
import time
import datetime
import multiprocessing
from base import Base


def worker_runner(url):
    return RabotauaWorker(read_contacts=True).read_resume(url)


def worker_close():
    RabotauaWorker().close()
    time.sleep(20)
    return multiprocessing.current_process()._identity[0]


class RabotaUaLogin(Base):

    LOGIN_FIELD = 'hr.rinasystems@gmail.com'
    PASSWORD_FIELD = '2016HR'

    def login(self):
        self.driver.get('http://rabota.ua/employer/login')
        self.do_input('#content_ZoneLogin_txLogin', self.LOGIN_FIELD, delay=1)
        self.do_input('#content_ZoneLogin_txPassword', self.PASSWORD_FIELD)
        self.do_click("#content_ZoneLogin_btnLogin")


class RabotaUAManager(RabotaUaLogin):
    table = 'rabota'
    process_count = 5

    def __init__(self, keyword, worker, read_contacts):
        super().__init__()
        self.data = {}
        self.keyword = keyword
        self.worker = worker

        self.read_contacts = read_contacts

        self.running = False

    def start_search(self):
        self.do_click("//a[contains(text(), 'Найти резюме')]", type='xpath')
        self.do_input('#beforeContentZone_Main2_txtKeywords', self.keyword, delay=1)
        self.do_click('a#search', delay=5)
        self.do_click('//div[contains(@class, "cv-list__period-select")]/select', delay=2, type='xpath')
        self.select_from_combobox('//div[contains(@class, "cv-list__period-select")]/select/option[@value="5"]', 'xpath', 2)
        self.do_click('a.js-newcvdb-close.newcvdb-notification__closebtn', delay=1)
        self.do_click("//span[text() = 'IT']", delay=5, type='xpath')

    def get_resume_urls_from_page(self):
        time.sleep(1)
        resumes = self._get_list(selector='h3 a[href^="/cv/"]')
        try:
            urls = [resume.get_attribute("href") for resume in resumes]
            return urls
        except Exception as e:
            print(e)
            return []

    def go_to_next_page(self):
        self.do_click('a.pager__button-next', delay=1)

    def generate_urls(self):
        self.start_search()
        existing_urls = [url['url'] for url in self.db.find({}, projection={'_id': 0, 'url': 1})]
        while self.running:
            for resume_url in self.get_resume_urls_from_page():
                if resume_url in existing_urls:
                    continue
                yield resume_url
            try:
                self.go_to_next_page()
            except Exception as e:
                print(e)
                break

    def process(self):
        self.login()
        self.running = True

        pool = multiprocessing.Pool(processes=self.process_count)
        i = 0
        try:
            urls_list = self.generate_urls()
            for i, (resume, url) in enumerate(pool.imap(worker_runner, urls_list), 1):
                if resume.get('error'):
                    self.running = False
                    break
                self.save_item({'sector': self.keyword, **resume})
        except Exception as e:
            print('Exception: %s' % e)
        finally:
            if i > 0:
                queue = {p._identity[0]: 1 for p in pool._pool}
                while queue:
                    ident = pool.apply_async(worker_close).get()
                    print('Ident', ident)
                    if ident in queue:
                        del queue[ident]
                    if not queue: break
            pool.close()
            pool.join()
            self.close()

    def get_all_urls(self):

        self.start_search()
        urls = {}
        while self.running:
            urls_list = self.get_resume_urls_from_page()
            urls.update({url: 1 for url in urls_list})
            try:
                self.go_to_next_page()
            except Exception as e:
                print(e)
                break
        return list(urls.keys())

    def mark_cv(self):
        self.login()
        self.running = True
        self.start_search()
        existing_urls = [url['url'] for url in self.db.find({'sector': {'$exists': False}}, projection={'_id': 0, 'url': 1})]
        urls = {}
        count = len(existing_urls)
        sector = self.keyword.replace(',', '')
        while count:
            for url in self.get_resume_urls_from_page():
                if url in existing_urls:
                    self.db.update({'url': url}, {'$set': {'sector': sector}})
                    count -= 1
            try:
                self.go_to_next_page()
            except Exception as e:
                print(e)
                break
        return list(urls.keys())


class RabotauaWorker(RabotaUaLogin):
    _instance = None
    data_reg = re.compile('\s*<span.*?>•</span>\s*', re.I + re.S)
    birthday_re = re.compile('\s*\(.*?\)')
    years_identifiers = [b'\xd1\x80\xd1\x96\xd0\xba', b'\xd0\xbb\xd0\xb5\xd1\x82', b'\xd0\xb3\xd0\xbe\xd0\xb4', b'\xd1\x80\xd0\xbe\xd0\xba', b'years']
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

    def __init__(self, read_contacts=False):
        pass

    def init(self, read_contacts):
        super().__init__()
        self.read_contacts = read_contacts
        self.stopped = False
        self.login()
        time.sleep(1)
        print('worker loginned')

    def is_date_string(self, string):
            return [b for b in self.years_identifiers if b in bytes(string, encoding='utf-8')]

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
            elif self.is_date_string(p):
                info['age'] = p
            else:
                info['payment'] = p
        return info

    def read_resume(self, url):
        if self.stopped:
            return (None, url)
        print(url)
        info = {}
        self.driver.get(url)
        if self.read_contacts:
            if self.check_element('#centerZone_BriefResume1_CvView1_cvHeader_plhNoTemporalCredits'):
                self.stopped = info['error'] = True
            else:
                try:
                    self.do_click('#centerZone_BriefResume1_CvView1_cvHeader_lnkOpenContact')
                except:
                    print('No id centerZone_BriefResume1_CvView1_cvHeader_lnkOpenContact in page')
                info['birthday'] = self.birthday_re.sub('', self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblBirthDateValue') or '')
                info['email'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblEmailValue')
                info['region'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblRegionValue')
                info['phone'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblPhoneValue')
        info['url'] = url
        info['name'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblName')
        info['position'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_txtJobName')
        info['cv_date'] = (self._get_text('.cvheadnav .muted') or '').replace('резюме обновлено ', '')
        info.update(self.get_core_info())

        fields = self._get_list('.cvtexts > div')

        for field in fields:
            if field.text and field.get_attribute('id'):
                info[field.get_attribute('id')] = field.text
        if info:
            info['date'] = datetime.datetime.now().strftime('%Y-%m-%d')
        resume = {self.vocabulary.get(k, k): v for k, v in info.items()}
        return resume, url


def run_rabota_ua():
    start = time.time()
    print('Start ')
    c = RabotaUAManager(keyword='javascript,', worker=worker_runner, read_contacts=True)
    c.process()
    c.write_file()
    end = time.time()
    print('delay: %s' % round(end - start))


if __name__ == "__main__":
    run_rabota_ua()
