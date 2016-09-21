import multiprocessing
import time
import datetime
from selenium import webdriver
from base import Crawler, Base
from xlsx import write_to_file


def worker_runner(url):
    return RabotauaWorker(read_contacts=False).read_resume(url)


class RabotaUaLogin(Base):
    LOGIN_FIELD = 'hr.rinasystems@gmail.com'
    PASSWORD_FIELD = '2016HR'

    def login(self):
        self.driver.get('http://rabota.ua/employer/login')
        self.do_input('#centerZone_ZoneLogin_txLogin', self.LOGIN_FIELD, delay=1)
        self.do_input('#centerZone_ZoneLogin_txPassword', self.PASSWORD_FIELD)
        self.do_click("#centerZone_ZoneLogin_btnLogin")


class RabotaUAManager(RabotaUaLogin):
    def __init__(self, keyword, worker, read_contacts):
        self.data = {}
        self.keyword = keyword
        self.worker = worker
        self.driver = webdriver.Firefox()
        self.read_contacts = read_contacts

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
            self.data[url] = resume
            # count +=1
            # if count ==20:
            #     break
        data = list(self.data.values())
        cols_set = set([k for d in data for k in d.keys()])
        print(cols_set)
        columns = list(cols_set)
        return data, columns


class RabotauaWorker(RabotaUaLogin):
    _instance = None
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

    def read_resume(self, url):
        print(url)
        info = {}
        self.driver.get(url)
        info['name'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblName')
        info['position'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_txtJobName')
        info['coreinfo'] = self._get_text('.rua-g-clearfix .rua-p-t_12')
        info['cv_date'] = self._get_text('.cvheadnav .muted')

        fields = self._get_list('.cvtexts > div')

        for field in fields:
            if field.text and field.get_attribute('id'):
                info[field.get_attribute('id')] = field.text

        if self.read_contacts:
            self.do_click('#centerZone_BriefResume1_CvView1_cvHeader_lnkBuyCv')
            info['birthday'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblBirthDateValue')
            info['email'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblEmailValue')
            info['region'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblRegionValue')
            info['phone'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblPhoneValue')

        resume = {self.vocabulary.get(k, k): v for k, v in info.items()}
        return resume, url


if __name__ == "__main__":
    start = datetime.datetime.now()
    c = RabotaUAManager(keyword='python', worker=worker_runner, read_contacts=False)
    data, columns = c.process()
    write_to_file('rabota.xlsx', data, columns)
    end = datetime.datetime.now()
    print('start: %s, end: %s. total: %s' % (start, end, end - start))




# class Rabotaua(Crawler):
#     url = 'http://rabota.ua/employer'
#     LOGIN_FIELD = 'hr.rinasystems@gmail.com'
#     PASSWORD_FIELD = '2016HR'
#     vocabulary = {
#         'AimHolder': 'Aim',
#         'SkillsHolder': 'Skills',
#         'ExperienceHolder': 'Experience',
#         'EducationHolder': 'Education',
#         'LanguagesHolder': 'Languages',
#         'TrainingsHolder': 'Trainings',
#         'AdditionalInfoHolder': 'Additional Info'
#     }
#
#     def __init__(self, keyword, read_contacts=False):
#         super().__init__(keyword)
#         self.read_contacts = read_contacts
#
#
#
#
#
#     def get_resume(self):
#         info = {}
#         info['name'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblName')
#         info['position'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_txtJobName')
#         info['coreinfo'] = self._get_text('.rua-g-clearfix .rua-p-t_12')
#
#         fields = self._get_list('.cvtexts > div')
#
#         for field in fields:
#             if field.text and field.get_attribute('id'):
#                 info[field.get_attribute('id')] = field.text.split('\n')
#
#         if self.read_contacts:
#             self.do_click('#centerZone_BriefResume1_CvView1_cvHeader_lnkBuyCv')
#             info['birthday'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblBirthDateValue')
#             info['email'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblEmailValue')
#             info['region'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblRegionValue')
#             info['phone'] = self._get_text('#centerZone_BriefResume1_CvView1_cvHeader_lblPhoneValue')
#
#         return {self.vocabulary.get(k, k): v for k, v in info.items()}
#
#     def read_page(self):
#         time.sleep(1)
#         resumes = self._get_list(selector='h3 a[href^="/cv/"]')
#         start_url = self.driver.current_url
#         count = 0
#         resume_links = [resume.get_attribute("href") for resume in resumes]
#         for url in resume_links:
#             if url in self.data:
#                 continue
#             time.sleep(1)
#             count += 1
#             self.driver.get(url)
#             resume = self.get_resume()
#             self.data[url] = resume
#         self.driver.get(start_url)
#         return count
#
#     def goto_next_page(self):
#         self.do_click('a.pager-next.pager-next-enabled', delay=1)
#
#     def process(self):
#         self.login()
#         self.find_resumes()
#         while True:
#             count = self.read_page()
#             if count == 0:
#                 break
#             self.goto_next_page()
#             break
#         data = list(self.data.values())
#         cols_set = set([k for d in data for k in d.keys()])
#         print(cols_set)
#         columns = list(cols_set)
#         return data, columns
#
#
# if __name__ == "__main__":
#     import datetime
#     start = datetime.datetime.now()
#     c = Rabotaua(keyword='python', read_contacts=False)
#     data, columns = c.process()
#     write_to_file('rabota.xlsx', data, columns)
#     end = datetime.datetime.now()
#     print('start: %s, end: %s. total: %s' % (start, end, end - start))