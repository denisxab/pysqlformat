"""

Чтение DBF файлов и конвертация их в sql запросы на запись

"""

from dbfread import DBF
import chardet
from pathlib import Path

def get_encoding(path:str)->str:
    """
    Получить кодировку файла, чтобы его можно было коректно прочитать
    """
    return chardet.detect(Path(path).read_bytes()).get('encoding')


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
      

def base_parse_sql(path:str,args:list[str],template:str):
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
             
def main():    
    
    path = r'SPRAV_LPU.DBF'
    o_path = Path(f'{Path(path).stem }.sql')
    res = base_parse_sql(
        path,
        ['ID_LPU','LPU_S_NAME','LPU_P_NAME'],
        template='''INSERT INTO JPERSONS (LPUCODESHORTNAME,SHORTNAME,JNAME,JNAME2, JPTCODE,KODTER,LPU) VALUES ('{ID_LPU}','{LPU_S_NAME}','{LPU_P_NAME}','{LPU_P_NAME}',5,78,'EIS_LOAD',1);'''
    )
    print(res)
    o_path.write_text(res,encoding='utf-8')
    
if __name__ == '__main__':
    main()
