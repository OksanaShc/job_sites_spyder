import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Base(object):

    def _get_element(self, selector, type='css', delay=0):
        self.wait = WebDriverWait(self.driver, 5)
        self.wait2 = WebDriverWait(self.driver, 2)
        time.sleep(delay)
        if selector.startswith('xpath'):
            type, selector = selector.split('//', 1)
        type = By.CSS_SELECTOR if type == 'css' else By.XPATH
        element = (self.wait if delay else self.wait2).until(EC.element_to_be_clickable((type, selector)))
        return element

    def _get_text(self, selector, type='css', delay=0):
        try:
            element = self._get_element(selector, type, delay)
            return element.text
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

    def do_input(self, selector, value, type='css', delay=0):
        element = self._get_element(selector, type, delay)
        element.send_keys(value)
        return element


class BaseManager(Base):
    pass


class BaseWorker(Base):
    pass




class Crawler():
    url = 'https://www.work.ua/employer/'
    LOGIN_FIELD = 'hr.rinasystems@gmail.com'
    PASSWORD_FIELD = 'HR2016'
    vocabulary = {}

    def __init__(self, keyword):
        self.init_driver()
        self.data = {}
        self.keyword = keyword

    def init_driver(self):
        self.driver = webdriver.Firefox()
        self.driver.maximize_window()
        self.wait = WebDriverWait(self.driver, 5)
        self.wait2 = WebDriverWait(self.driver, 2)



    def login(self):
        pass

    def process(self):
        pass
