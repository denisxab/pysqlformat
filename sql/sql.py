from pathlib import Path
import re
import chardet



class SQLFormatter:
    def hide_comment(text:str)->str:
        """Скрыть коментарии из SQL"""
        # TODO: реализовать !
        ...
    
    def sub_space(text:str)->str:
        """Удалить лишнии пробелы из SQL"""
        res = re.sub(' {2,}',' ',text)
        res = re.sub('\n{3,}','\n',res)
        return res   
        

    def conver_pont_to_uderline(text:str)->str:
        """Заменить обращения по точке, на нижнии подчеркивания"""
        res = re.sub('(\w+)\.(\w+)',lambda m: f'{m.group(1)}_{m.group(2)}',text)
        return res   
    
    def пренос_для_скобок(text:str)->str:
        res = re.sub('\(','(\n',text)
        res = re.sub('\)','\n)',res)
        return res
    
    def Выравнять_кометнтарии(text:str)->str:
        """
        
        ::IN::
        
        res -- Откуда ответ
        ,fullname-- ФИО пациента 	
        ,KODOPER || ' : ' || SCHNAME || ' : ' || SCHID as SCHNAME  -- Услуга 
        ,d_fullname -- ФИО врача 	
        ,(select DEPNAME from DEPARTMENTS where DEPNUM=O_DEPNUM),O_DEPNUM -- Отделение 	
        ,cast(TREATDATE as date) as TREATDATE-- Дата приема 
        ,RUB_TO -- Получено
        ,RUB_FROM -- Выдано    
            
        ::OUT::
        
        res                                                               -- Откуда ответ
        ,fullname                                                         -- ФИО пациента 	
        ,KODOPER || ' : ' || SCHNAME || ' : ' || SCHID as SCHNAME         -- Услуга 
        ,d_fullname                                                       -- ФИО врача 	
        ,(select DEPNAME from DEPARTMENTS where DEPNUM=O_DEPNUM),O_DEPNUM -- Отделение 	
        ,cast(TREATDATE as date) as TREATDATE                             -- Дата приема 
        ,RUB_TO                                                           -- Получено
        ,RUB_FROM                                                         -- Выдано


        """
        pt:re.Pattern = re.compile('([ \t]*)(?P<text>(?:.(?!--))+.)(--.+)')
        max_len:int = max(len(x['text']) for x in  pt.finditer(text))

        def _self(m:re.Match):
            x_text =  m['text'].ljust(max_len)
            return f"{m.group(1)}{x_text}{m.group(3)}"
    
        res = pt.sub(_self,text)
        return res
        
            

def get_encoding(path:str)->str:
    """
    Получить кодировку файла, чтобы его можно было коректно прочитать
    """
    return chardet.detect(Path(path).read_bytes()).get('encoding')



if __name__ == '__main__':
    pat = Path('sql\in_sql.sql')
    path_out= Path('sql\out_sql.sql')
    
    #############################################
    text= pat.read_text(encoding=get_encoding(str(pat)))
    res= SQLFormatter.Выравнять_кометнтарии(text)
    
    # res = text#SQLFormatter.sub_space(text)
    # res = SQLFormatter.пренос_для_скобок(res)
    #############################################
    
    path_out.write_text(res,encoding='utf-8')
    print(res)