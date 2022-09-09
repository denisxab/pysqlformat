from abc import abstractstaticmethod
from pathlib import Path
import sys
from typing import Optional

import chardet
from dbfread import DBF
# pip install xlrd==1.2.0
from xlrd import Book, open_workbook
from xlrd.sheet import Sheet


sys.path.insert(0,Path(__file__).parent.parent.__str__())
from base import get_encoding


class BaseParse:
    
    @abstractstaticmethod
    def toSQL(path: str, template: str, *args,**kwargs):
        ...


class XlsxParse(BaseParse):

    @staticmethod
    def toSQL(path: str, template: str, sheet=0, out_path: Optional[str] = None):
        """

        Конвертировать данный из XLSX файл в SQL команду

        :param path: Путь к файлу
        :param template: Шаблон построения SQL команды
        :param sheet: Номер листа с которого читать
        :param out_path: Путь к файлу в который нужно записать ответ

        -------------------------------------------------------------------------------------------
        res = XlsxParse.toSQL("s.xlsx", 'insert into clients (id,uuid,count) value ({a},{a},{c});')
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
        # Для хранения ответа
        result: list[str] = []
        # Хранения временного результата {ИмяСтолба:ЗначениеСтолбца}
        tmp: dict[str, str]
        for row in range(1, worksheet.nrows):
            tmp = {
                # Экранируем одинарные кавычки для исполнения SQL команд
                name: str(worksheet.cell_value(row, col)).replace("'", "''")
                for name, col in zip(head, range(1, worksheet.ncols))
            }
            try:
                # Формируем ответ на основе переданной шаблонной строки
                result.append(template.format(**tmp))
            except KeyError as e:
                raise KeyError(f"{e}, Не найден столбец с указаны заголовком")
        # Конвертируем ответ в строку через перенос строки
        res2 = '\n'.join(result)
        # Если передан файл в который нужно записать ответ
        if out_path:
            Path(out_path).write_text(res2)
            return f"Записано в файл: {out_path}"
        else:
            return res2

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
    def toSQL(path:str,args:list[str],template:str):
        """
        Чтение DBF файлов и конвертация их в sql запросы на запись
        
        ----------------------------------------------------------------------------------
        path = r'SPRAV_LPU.DBF'
        o_path = Path(f'{Path(path).stem }.sql')
        res = base_parse_sql(
            path,
            ['ID_LPU','LPU_S_NAME','LPU_P_NAME'],
            template='''INSERT INTO JPERSONS (JID,LPUCODE,SHORTNAME,JNAME,JNAME2, JPTCODE,KODTER,JCODE,LPU) VALUES (next value for  JP_GEN,'{ID_LPU}','{LPU_S_NAME}','{LPU_P_NAME}','{LPU_P_NAME}',5,78,'EIS_LOAD',1);'''
        )
        print(res)
        o_path.write_text(res,encoding='utf-8')
        ----------------------------------------------------------------------------------
        
        """

        res:list[str] = []
        dargs:dict[str,str] = {}
        # Обрабатываем DBF файл по строчно
        for record in DBF(path,encoding=get_encoding(path)):
            # Работа со строками
            for x in args:
                # Экранируем одинарные кавычки, для SQL команд
                dargs[x]=str(record[x]).replace("'","''") 
            # Формируем строку по переданому шаблону
            res.append(template.format(**dargs))
        # Переводим список в строку через перенос строки
        return '\n'.join(res)         
             
             
if __name__ == '__main__':
    path = r'SPRAV_LPU.DBF'
    o_path = Path(f'{Path(path).stem }.sql')
    res = DbfPasre.toSQL(
        path,
        ['ID_LPU','LPU_S_NAME','LPU_P_NAME'],
        template='''INSERT INTO JPERSONS (JID,LPUCODE,SHORTNAME,JNAME,JNAME2, JPTCODE,KODTER,JCODE,LPU) VALUES (next value for  JP_GEN,'{ID_LPU}','{LPU_S_NAME}','{LPU_P_NAME}','{LPU_P_NAME}',5,78,'EIS_LOAD',1);'''
    )
    print(res)
    o_path.write_text(res,encoding='utf-8')
