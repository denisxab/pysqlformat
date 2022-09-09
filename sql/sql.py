from pathlib import Path
import re
import sys


sys.path.insert(0,Path(__file__).parent.parent.__str__())
from base import get_encoding



class SQLFormatter:
    def hide_comment(text:str)->str:
        """
        Скрыть коментарии из SQL
        
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        ,cast(sd.sdid as numeric(11,0)) as SERV_ID
        ,cast(1 as numeric(3,0)) as ID_IN_CASE
        ,cast(coalesce(left(clr_in.extrefid,20),clr_in.refid) as char(20)) as D_NUMBER
        /*  ,cast(coalesce(clr_in.extrefdate,clr_in.treatdate,sd.planstart) as date) as */

        DATE_ISSUE
        /*
        ,cast(null as date) as as as DATE_PLANG
        ,cast(coalesce(rpv.propvalueint,9248) as numeric (11,0)) as ID_LPU_F
        ,cast(10278 as numeric (11,0)) as ID_LPU_T
        ,cast(19 as numeric(11,0)) as ID_D_TYPE
        ,cast(11 as numeric(11,0)) as ID_D_GROUP
        ,cast(null as numeric(11,0)) as ID_PRVS
        ,cast(null as numeric(11,0)) as ID_OB_TYPE
        ,cast(null as numeric(11,0)) as ID_PRMP
        ,cast(null as numeric(11,0)) as ID_B_PROF
        ,cast(null as numeric(11,0)) as ID_DN
        */
        ,cast(null as numeric(11,0)) as ID_GOAL
        ,cast(null as numeric(11,0)) as ID_DIAGNOS
        ,cast(null as numeric(11,0)) as ID_LPU_RF
        ,cast(null as numeric(11,0)) as ID_LPU_TO
        --     ,cast(null as numeric(11,0)) as ID_NMKL
        cast(null as char(200)) ERROR
        -- info --
        ,cast(dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
        
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        
        ,cast(sd.sdid as numeric(11,0)) as SERV_ID
        ,cast(1 as numeric(3,0)) as ID_IN_CASE
        ,cast(coalesce(left(clr_in.extrefid,20),clr_in.refid) as char(20)) as D_NUMBER
        

        DATE_ISSUE
        
        ,cast(null as numeric(11,0)) as ID_GOAL
        ,cast(null as numeric(11,0)) as ID_DIAGNOS
        ,cast(null as numeric(11,0)) as ID_LPU_RF
        ,cast(null as numeric(11,0)) as ID_LPU_TO
        
        cast(null as char(200)) ERROR
        
        ,cast(dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
        
        """        
        # Многострочный коментарий
        m_c = "(\/\*\s*(?:.\s*(?!\*\/))+[\W\w]\*\/)"
        # Однострочный коментарий
        s_c = "(--.+)"
        # Удаляем коментарии 
        res = re.sub(f"{m_c}|{s_c}",'',text)
        return res
    
    def sub_space(text:str)->str:
        """
        Удалить лишнии пробелы из SQL
        
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
                 t.treatcode    -- Коментарий
        
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        t.treatcode -- Коментарий
        
        """
        res = re.sub(' {2,}',' ',text)
        res = re.sub('\n{3,}','\n',res)
        return res   
        
    def conver_pont_to_uderline(text:str)->str:
        """
        Заменить обращения по точке, на нижнии подчеркивания
        
        ::IN::
        
        t.treatcode
        
        ::OUT::
        
        t_treatcode
        """
        res = re.sub('(\w+)\.(\w+)',lambda m: f'{m.group(1)}_{m.group(2)}',text)
        return res   
        
    def Конвертировать_Квадратные_Скобки_В_Двоеточие(text:str)->str:
        
        """

        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

        select
            TYPE_RES,SERV_ID,SURNAME,NAME,S_NAME,BIRTHDAY,SEX,ID_PAT_CAT,LGOTS,DOC_TYPE,SER_L,SER_R,DOC_NUMBER,ISSUE_DATE,DOCORG_ASMEMO,SNILS,C_OKSM,IS_SMP,POLIS_TYPE,CLH_NSP,POLIS_S,POLIS_N,ID_SMO,POLIS_BD,POLIS_ED,ID_SMO_REG,ADDR_TYPE,IDOKATOREG,IDOBLTOWN,ID_PREFIX,ID_HOUSE,HOUSE,KORPUS,FLAT,U_ADDRESS,KLADR_CODE,STREET,IDSTRTYPE,ADDRTYPE_L,OKATOREG_L,OBLTOWN_L,PREFIX_L,ID_HOUSE_L,HOUSE_L,KORPUS_L,FLAT_L,U_ADDR_L,KLADR_L,STREET_L,STRTYPE_L,PLACE_WORK,ADDR_WORK,ADDR_PLACE,REMARK,B_PLACE,VNOV_D,ID_G_TYPE,G_SURNAME,G_NAME,G_S_NAME,G_BIRTHDAY,G_SEX,G_DOC_TYPE,G_SERIA_L,G_SERIA_R,G_DOC_NUM,G_ISSUE_D,G_DOCORG_ASMEMO,G_B_PLACE,N_BORN,SEND,ERROR,ID_MIS,null as ID_PATIENT,T_TREATCODE,D_DCODE
        from ITS_EIS_AMBL_P([bdate],[fdate],'[treat_depnum]','[dcode]')
   
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

        select
            TYPE_RES,SERV_ID,SURNAME,NAME,S_NAME,BIRTHDAY,SEX,ID_PAT_CAT,LGOTS,DOC_TYPE,SER_L,SER_R,DOC_NUMBER,ISSUE_DATE,DOCORG_ASMEMO,SNILS,C_OKSM,IS_SMP,POLIS_TYPE,CLH_NSP,POLIS_S,POLIS_N,ID_SMO,POLIS_BD,POLIS_ED,ID_SMO_REG,ADDR_TYPE,IDOKATOREG,IDOBLTOWN,ID_PREFIX,ID_HOUSE,HOUSE,KORPUS,FLAT,U_ADDRESS,KLADR_CODE,STREET,IDSTRTYPE,ADDRTYPE_L,OKATOREG_L,OBLTOWN_L,PREFIX_L,ID_HOUSE_L,HOUSE_L,KORPUS_L,FLAT_L,U_ADDR_L,KLADR_L,STREET_L,STRTYPE_L,PLACE_WORK,ADDR_WORK,ADDR_PLACE,REMARK,B_PLACE,VNOV_D,ID_G_TYPE,G_SURNAME,G_NAME,G_S_NAME,G_BIRTHDAY,G_SEX,G_DOC_TYPE,G_SERIA_L,G_SERIA_R,G_DOC_NUM,G_ISSUE_D,G_DOCORG_ASMEMO,G_B_PLACE,N_BORN,SEND,ERROR,ID_MIS,null as ID_PATIENT,T_TREATCODE,D_DCODE
        from ITS_EIS_AMBL_P(:bdate,:fdate,:treat_depnum,:dcode)

        """

        res = re.sub("'?\[([^]]+)\]'?",lambda m: f':{m.group(1)}',text)
        return res
    
    def Поставить_Переносы_Перед_Запятой(text:str)->str:
        """
        
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        TYPE_RES,SERV_ID,SURNAME,NAME,S_NAME,BIRTHDAY,SEX,ID_PAT_CAT
        
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        TYPE_RES
        ,SERV_ID
        ,SURNAME
        ,NAME
        ,S_NAME
        ,BIRTHDAY
        ,SEX
        ,ID_PAT_CAT

        """
        
        res = re.sub(',[^,]+',lambda m: f"\n{m.group(0)}",text)
        return res
     
    def Выравнять_кометнтарии(text:str)->str:
        """
        Выравнять кометнтарии
        
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        res -- Откуда ответ
        ,fullname-- ФИО пациента 	
        ,KODOPER || ' : ' || SCHNAME || ' : ' || SCHID as SCHNAME  -- Услуга 
        ,d_fullname -- ФИО врача 	
        ,(select DEPNAME from DEPARTMENTS where DEPNUM=O_DEPNUM),O_DEPNUM -- Отделение 	
        ,cast(TREATDATE as date) as TREATDATE-- Дата приема 
        ,RUB_TO -- Получено
        ,RUB_FROM -- Выдано    
            
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
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
    
    def Перенос_запятой_в_начало(text:str)->str:
        
        """
        Перенос запятой в начало
        
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        cast(sd.sdid as numeric(11,0)) SERV_ID,
        cast(1 as numeric(3,0)) ID_IN_CASE,
        cast(coalesce(left(clr_in.extrefid,20),clr_in.refid) as char(20)) D_NUMBER,
        cast(coalesce(clr_in.extrefdate,clr_in.treatdate,sd.planstart) as date) DATE_ISSUE,
        cast(null as date) DATE_PLANG,
        cast(coalesce(rpv.propvalueint,9248) as numeric (11,0)) ID_LPU_F,
        cast(10278 as numeric (11,0)) ID_LPU_T,
        cast(19 as numeric(11,0)) ID_D_TYPE,
        cast(11 as numeric(11,0)) ID_D_GROUP,
        cast(null as numeric(11,0)) ID_PRVS,
        cast(null as numeric(11,0)) ID_OB_TYPE,
        cast(null as numeric(11,0)) ID_PRMP,
        cast(null as numeric(11,0)) ID_B_PROF,
        cast(null as numeric(11,0)) ID_DN,
        cast(null as numeric(11,0)) ID_GOAL,
        cast(null as numeric(11,0)) ID_DIAGNOS,
        cast(null as numeric(11,0)) ID_LPU_RF,
        cast(null as numeric(11,0)) ID_LPU_TO,
        cast(null as numeric(11,0)) ID_NMKL,
        cast(null as char(200)) ERROR
        -- info --
        ,cast(dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
        
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    
        ,cast(sd.sdid as numeric(11,0)) SERV_ID
        ,cast(1 as numeric(3,0)) ID_IN_CASE
        ,cast(coalesce(left(clr_in.extrefid,20),clr_in.refid) as char(20)) D_NUMBER
        ,cast(coalesce(clr_in.extrefdate,clr_in.treatdate,sd.planstart) as date) DATE_ISSUE
        ,cast(null as date) DATE_PLANG
        ,cast(coalesce(rpv.propvalueint,9248) as numeric (11,0)) ID_LPU_F
        ,cast(10278 as numeric (11,0)) ID_LPU_T
        ,cast(19 as numeric(11,0)) ID_D_TYPE
        ,cast(11 as numeric(11,0)) ID_D_GROUP
        ,cast(null as numeric(11,0)) ID_PRVS
        ,cast(null as numeric(11,0)) ID_OB_TYPE
        ,cast(null as numeric(11,0)) ID_PRMP
        ,cast(null as numeric(11,0)) ID_B_PROF
        ,cast(null as numeric(11,0)) ID_DN
        ,cast(null as numeric(11,0)) ID_GOAL
        ,cast(null as numeric(11,0)) ID_DIAGNOS
        ,cast(null as numeric(11,0)) ID_LPU_RF
        ,cast(null as numeric(11,0)) ID_LPU_TO
        ,cast(null as numeric(11,0)) ID_NMKL
        cast(null as char(200)) ERROR
        -- info --
        ,cast(dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
        
        """
        res = re.sub("([ \t]+)(.+),\n",lambda m: f"{m.group(1)},{m.group(2)}\n",text)
        return res
        
    def Поставить_Алиасы(text:str)->str:
        """
        
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        ,cast(sd.sdid as numeric(11,0)) SERV_ID
        ,cast(1 as numeric(3,0)) ID_IN_CASE
        ,cast(coalesce(left(clr_in.extrefid,20),clr_in.refid) as char(20)) D_NUMBER
        ,cast(coalesce(clr_in.extrefdate,clr_in.treatdate,sd.planstart) as date) DATE_ISSUE
        ,cast(null as date) as DATE_PLANG
        ,cast(coalesce(rpv.propvalueint,9248) as numeric (11,0)) ID_LPU_F
        ,cast(10278 as numeric (11,0)) ID_LPU_T
        ,cast(19 as numeric(11,0)) ID_D_TYPE
        ,cast(11 as numeric(11,0)) ID_D_GROUP
        ,cast(null as numeric(11,0)) ID_PRVS
        ,cast(null as numeric(11,0)) ID_OB_TYPE
        ,cast(null as numeric(11,0)) ID_PRMP
        ,cast(null as numeric(11,0)) ID_B_PROF
        ,cast(null as numeric(11,0)) ID_DN
        ,cast(null as numeric(11,0)) ID_GOAL
        ,cast(null as numeric(11,0)) ID_DIAGNOS
        ,cast(null as numeric(11,0)) ID_LPU_RF
        ,cast(null as numeric(11,0)) ID_LPU_TO
        ,cast(null as numeric(11,0)) ID_NMKL
        cast(null as char(200)) ERROR
        -- info --
        ,cast(dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
        
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        ,cast(sd.sdid as numeric(11,0)) as SERV_ID
        ,cast(1 as numeric(3,0)) as ID_IN_CASE
        ,cast(coalesce(left(clr_in.extrefid,20),clr_in.refid) as char(20)) as D_NUMBER
        ,cast(coalesce(clr_in.extrefdate,clr_in.treatdate,sd.planstart) as date) as DATE_ISSUE
        ,cast(null as date) as DATE_PLANG
        ,cast(coalesce(rpv.propvalueint,9248) as numeric (11,0)) as ID_LPU_F
        ,cast(10278 as numeric (11,0)) as ID_LPU_T
        ,cast(19 as numeric(11,0)) as ID_D_TYPE
        ,cast(11 as numeric(11,0)) as ID_D_GROUP
        ,cast(null as numeric(11,0)) as ID_PRVS
        ,cast(null as numeric(11,0)) as ID_OB_TYPE
        ,cast(null as numeric(11,0)) as ID_PRMP
        ,cast(null as numeric(11,0)) as ID_B_PROF
        ,cast(null as numeric(11,0)) as ID_DN
        ,cast(null as numeric(11,0)) as ID_GOAL
        ,cast(null as numeric(11,0)) as ID_DIAGNOS
        ,cast(null as numeric(11,0)) as ID_LPU_RF
        ,cast(null as numeric(11,0)) as ID_LPU_TO
        ,cast(null as numeric(11,0)) as ID_NMKL
        cast(null as char(200)) ERROR
        -- info --
        ,cast(dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
            
        """

        # Ставим алиасы
        res = re.sub('([\t ]+,.+[ \t]+)([\w\d]+)\n',lambda m:f"{m.group(1)}as {m.group(2)}\n",text)
        # Удаляем дубли алиасов
        res = re.sub('(?:as ){2,}','as ',res)
        return res


if __name__ == '__main__':
    pat = Path('sql\in_sql.sql')
    path_out= Path('sql\out_sql.sql')
    
    #############################################
    text= pat.read_text(encoding=get_encoding(str(pat)))
    res= SQLFormatter.Поставить_Переносы_Перед_Запятой(text)
    
    # res = text#SQLFormatter.sub_space(text)
    # res = SQLFormatter.пренос_для_скобок(res)
    #############################################
    
    path_out.write_text(res,encoding='utf-8')
    print(res)