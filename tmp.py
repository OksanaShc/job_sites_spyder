# import xlrd

#
# def get_used_urls(self):
#
#     rb = xlrd.open_workbook('D:\\Projects\\job_sites_spider_mult\\rabota_12_11.xlsx')
#     sheet = rb.sheet_by_index(0)
#     used_urls_list = []
#     for i in range(1, sheet.nrows):
#         row = sheet.row_values(i)
#         variable = row[15]
#         used_urls_list.append(variable)
#     return used_urls_list
#
#
# def read_excel(file_name):
#     wb = xlrd.open_workbook(file_name)
#
#     new_tickers_list = []
#
#     first_page = wb.sheet_by_index(0)
#     header_cells = first_page.row(0)
#     num_rows = first_page.nrows - 1
#     curr_row = 0
#     header = [each.value for each in header_cells]
#     while curr_row < num_rows:
#         curr_row += 1
#         row = [each.value
#                for each in first_page.row(curr_row)]
#         value_dict = dict(zip(header, row))
#
#         new_tickers_list.append(value_dict)
#
#     return new_tickers_list
#
# l_1 = read_excel('D:\\Projects\\job_sites_spider_mult\\rabota_12_11.xlsx')
# l_2 = read_excel('D:\\Projects\\job_sites_spider_mult\\rabota_1479048621.409127.xlsx')
# l = l_1 + l_2
# new ={data['url']:data for data in l}
# print(new)

import multiprocessing

def new(n):
    return New().test(n)

class New():
    def test(self, n):
        return n
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __del__(self):
        print(multiprocessing.current_process(), 'del')

if __name__ == "__main__":
    p = multiprocessing.Pool(processes=3)
    for n in p.imap(new, range(10)):
        print(n)
