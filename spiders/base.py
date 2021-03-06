import time
import pymongo
import psutil
import datetime
import pyvirtualdisplay
import multiprocessing
import selenium.webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from settings import geckodriver_path
from tools.xlsx import write_to_file


class Base(object):
    table = None

    def __init__(self):
        try:
            self.display = pyvirtualdisplay.Display(visible=0, size=(1920, 1080))
            self.display.start()
        except:
            pass
        self.is_closed = False
        if self.table:
            self.dbconn = pymongo.MongoClient(host='127.0.0.1')
            self.db = getattr(self.dbconn.cv_base, self.table)
        self.driver = selenium.webdriver.Firefox(executable_path=geckodriver_path)

    def close(self):
        if not self.is_closed:
            try:
                self.driver.quit()
                self.is_closed = True
            except:
                process = psutil.Process(self.driver.service.process.pid)
                for p in process.children(recursive=True):
                    p.kill()
                process.kill()
        print('Close', multiprocessing.current_process())

    def _get_element(self, selector, type='css', delay=0):
        self.wait = WebDriverWait(self.driver, 5)
        self.wait2 = WebDriverWait(self.driver, 2)
        time.sleep(delay)
        if selector.startswith('xpath'):
            type, selector = selector.split('//', 1)
        type = By.CSS_SELECTOR if type == 'css' else By.XPATH
        element = (self.wait if delay else self.wait2).until(EC.presence_of_element_located((type, selector)))
        return element

    def _get_element_attribute(self, selector, attribute, type='css', delay=0):
        try:
            element = self._get_element(selector, type, delay)
            return element.get_attribute(attribute)
        except:
            print('Error: selector not found element: {}, url: {}'.format(selector, self.driver.current_url))

    def _get_text(self, selector, type='css', delay=0):
        try:
            element = self._get_element(selector, type, delay)
            return element.text
        except:
            print('Error: selector not found element: {}, url: {}'.format(selector, self.driver.current_url))

    def _get_element_safe(self, selector, type='css', delay=0):
        try:
            element = self._get_element(selector, type, delay)
            return element
        except:
            print('Error: selector not found element: {}, url: {}'.format(selector, self.driver.current_url))

    def _get_list(self, selector, type='css', delay=0):
        time.sleep(delay)
        if selector.startswith('xpath'):
            type, selector = selector.split('//', 1)
        type = By.CSS_SELECTOR if type == 'css' else By.XPATH
        elements_list = self.driver.find_elements(by=type, value=selector)
        return elements_list

    def do_click(self, selector, type='css', delay=0):
        element = self._get_element(selector, type, delay)
        element.click()
        return element

    def select_from_combobox(self, selector, type='css', delay=0):
        element = self._get_element(selector, type, delay)
        element.send_keys(Keys.ENTER)
        element.click()
        return element

    def do_input(self, selector, value, type='css', delay=0):
        element = self._get_element(selector, type, delay)
        element.send_keys(value)
        return element

    def check_element(self, selector, type='css', delay=0):
        try:
            return self._get_element(selector, type, delay)
        except:
            return False

    def save_item(self, row):
        self.db.insert(row)

    def get_data(self):
        mongo_data = list(self.db.find({}, projection={'_id': 0}))
        cols_set = set([k for d in mongo_data for k in d.keys()])
        columns = list(cols_set)
        return mongo_data, columns

    def write_file(self, filename=None):
        filename = filename or 'var/data/%s_%s.xlsx' % (self.table, datetime.datetime.now().strftime("%Y-%m-%d"))
        data, columns = self.get_data()

        write_to_file(filename, data, columns)


class BaseManager(Base):
    pass


class BaseWorker(Base):
    pass


