import xlsxwriter


def write_to_file(filename, data, columns):
    # Create an new Excel file and add a worksheet.
    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet()

    for j, col in enumerate(columns):
        worksheet.write(0, j, col)
    try:
        for i, row in enumerate(data):
            for j, col in enumerate(columns):
                worksheet.write(i+1, j, row.get(col) or '')
    except Exception as ex:
        print(ex)
    workbook.close()