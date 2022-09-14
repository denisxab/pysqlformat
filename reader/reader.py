import sys
import typing
from abc import abstractstaticmethod
from pathlib import Path

sys.path.insert(0, Path(__file__).parent.parent.__str__())
from base import get_encoding
# pip install dbfread==2.0.7
from dbfread import DBF
from xlrd import Book, open_workbook
# pip install xlrd==1.2.0
from xlrd.sheet import Sheet


class BaseParse:

    @abstractstaticmethod
    def goParse(path: str, template: str, **kwargs)->typing.Iterator[str]:
        ...

    def escaSql(text:str)->str:
        """
        Экранирование текста для SQL
        """
        return text.replace("'", "''")

class XlsxParse(BaseParse):

    @staticmethod
    def goParse(path: str, template: str, sheet=0, esce_call:typing.Callable = BaseParse.escaSql)->typing.Iterator[str]:
        """

        Конвертировать данный из XLSX файл в шаблон


        Получить данные из `xlsx` файла по пути `path`. Первая строка этого файла будут считаться заголовками, следующие строки
        будут считаться как обычные данные. Вы можете обращаться к данным через шаблон `template`, в котором нужно указать имя заголовка.


        :param path: Путь к файлу
        :param template: Шаблон построения ответа `insert into Таблиц value ({ИмяСтолбца_1},{ИмяСтолбца_N})`
        :param sheet: Номер листа с которого читать
        :param out_path: Путь к файлу в который нужно записать ответ
        :param esce_call: Функция для экранирование значения 
        -------------------------------------------------------------------------------------------
        for x in XlsxParse.goParse(r'Файл.xlsx', 'select * from dual where id = {ID}'):
            print(x)
        -------------------------------------------------------------------------------------------

        """

        # Выбрать файл
        workbook: Book = open_workbook(path)
        # Выбрать страницу
        worksheet: Sheet = workbook.sheet_by_index(sheet)
        """
        - worksheet
            - .ncols - Всего не пустых столбцов
            - .nrows - Всего не пустых строчек
            - .name - Имя текущей страницы
            - .cell_value(строка,столбец) - Выбрать ячейку по указанным координатам
        """
        # Список заголовков
        head: list[str] = [worksheet.cell_value(0, col) for col in range(worksheet.ncols)]
        # Хранения временного результата {ИмяСтолба:ЗначениеСтолбца}
        tmp: dict[str, str]
        for row in range(1, worksheet.nrows):
            tmp = {name: esce_call(str(worksheet.cell_value(row, col))) for name, col in zip(head, range(0, worksheet.ncols))}
            try:
                # Формируем ответ на основе переданной шаблонной строки
                yield template.format(**tmp)
            except KeyError as e:
                raise KeyError(f"{e}, Не найден столбец с указаны заголовком")


class DbfPasre(BaseParse):

    class Record(object):
        """
        Класс для того чтобы можно было получать значение стобца через точку

        :Использование:

        for record in DBF(Путь, encoding=get_encoding(Путь), recfactory=Record):
            record.ИмяСтолбца
        """

        def __init__(self, items):
            for (name, value) in items:
                setattr(self, name, value)

    @staticmethod
    def goParse(path: str, template: str, esce_call:typing.Callable = BaseParse.escaSql)->typing.Iterator[str] :
        """
        Чтение DBF файлов

        Получить данные из `dbf` файла по пути `path`.Вы можете обращаться к данным через шаблон `template`, в котором нужно указать имя заголовка.

        :param path: Путь к файлу
        :param template: Шаблон построения ответа `insert into Таблиц value ({ИмяСтолбца_1},{ИмяСтолбца_N})`
        :param esce_call: Функция для экранирование значения 

        ----------------------------------------------------------------------------------
        for x in DbfPasre.goParse(r'SPRAV_CC_DIRECTIONS.DBF', 'select * from dual where id = {ID_D_GROUP} ref = {CNT_MIN}'):
            print(x)
        ----------------------------------------------------------------------------------
        """
        # Хранения временного результата {ИмяСтолба:ЗначениеСтолбца}
        tmp: dict[str, str]
        # Обрабатываем DBF файл по строчно
        for record in DBF(path, encoding=get_encoding(path)):
            # Работа со строками
            tmp = {k: esce_call(str(v)) for k, v in dict(record).items()}
            try:
                # Формируем ответ на основе переданной шаблонной строки
                yield template.format(**tmp)
            except KeyError as e:
                raise KeyError(f"{e}, Не найден столбец с указаны заголовком")



if __name__ == '__main__':

    # for x in XlsxParse.goParse(r'w2.xlsx', 'select * from dual where id = {ID}'):
    #     print(x)
    
    # for x in DbfPasre.goParse(r'SPRAV_CC_DIRECTIONS.DBF', 'select * from dual where id ={ID_D_GROUP} {CNT_MIN}'):
    #     print(x)

    ...
