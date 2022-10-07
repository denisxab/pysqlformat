from pathlib import Path
import re
import sys
import typing
from unittest.util import strclass


sys.path.insert(0,Path(__file__).parent.parent.__str__())
from base import get_encoding



class SQLFormatter:

    def Remove_Comments(text:str)->str:
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
    
    def Remove_Dub_Spase(text:str)->str:
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
        def _self(m:re.Match):
          return f"{m.group(1)},{m.group(2)}{m.group(3)}"
        
        res = re.sub("([ \t]+)(.+),(\n|([\t ]*--.+))",_self,text)
        return res
        
    def Format_AS(text:str)->str:
        """
        Поставить Алиасы
                
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

    def Format_CASE(text:str)->str:
        """
        
        Форматировать CASE
        
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        
        ,u.fullname uname
        ,i.orderno numdoc
        ,iif(ir.ir>0,ir.irname,
                trim(case i.paycode
                        when 1 then 'Оплата лечения!' when 2 then 'Внесение аванса'
                                when 3 then 'Оплата долга' when 5 then 'Возврат личного аванса'
                        when 6 then 'Выдача денег из кассы'  end)) text
        ,iif(i.paycode in (1,2,3        
        
        
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        ,u.fullname uname
        ,i.orderno numdoc
        ,iif(ir.ir>0,ir.irname,
                trim(case i.paycode                        
                        when 1 then 'Оплата лечения!' 
                        when 2 then 'Внесение аванса'                                
                        when 3 then 'Оплата долга' 
                        when 5 then 'Возврат личного аванса'                        
                        when 6 then 'Выдача денег из кассы'  
                     end)) text
        ,iif(i.paycode in (1,2,3        
        
        
        """
        
        def _self(m:re.Match)->str:
            NL = '\n' 
            text  = m.group(0)
            # Получаем оступы
            сколько_отступов = re.search('([\t ]+)when',text).group(1)
            # Получаем все WHEN _ THEN           
            m2 = re.search("case (?P<case>(?:.\s*(?!when))+[\w\W])(?P<when>(when((?:.\s*(?!then)))+[\w\W]then((?:.\s*(?!when))+)[\w\W])+)end",text)
            # Перебераем элементы WHEN _ THEN
            pt2 = re.compile("when(?P<when>(?:.\s*(?!then))+[\w\W])then(?P<then>(?:.\s*(?!when))+[\w\W])")
            # Форматируем WHEN _ THEN
            res2='\n'.join(
                f"{сколько_отступов}when{x['when'].replace(NL,'')}then{x['then'].replace(NL,'')}" 
                for x in pt2.finditer(m2['when'])
            )
            # Формируем ответ
            return f"""case {m2['case'].replace(NL,'')}\n{res2}\n{сколько_отступов[:-3]}end"""
        
        res =  re.sub("case(?:.\s*(?!end))+[\w\W]end",_self,text)
        return res

    def Format_UNION_ALL(text:str)->str:
        """
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
        
        left join doctor u on u.dcode = coalesce(r2.uid,i.uid)
        left join incomref ir on ir.ir = i.incref
        join MONEYCASHREF union mf on mf.MONEYCASHID = i.moneycashid -- По всем касам
        where
            i.paydate between x.bdate and x.fdate
            and i.smccode is null       union all        select
            2 ptype
            ,w.schid
            ,w.kodoper
            ,w.schname
            
        ::OUT::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

        left join doctor u on u.dcode = coalesce(r2.uid,i.uid)
        left join incomref ir on ir.ir = i.incref
        join MONEYCASHREF 

        union

        mf on mf.MONEYCASHID = i.moneycashid -- По всем касам
                where
                    i.paydate between x.bdate and x.fdate
                    and i.smccode is null       

        union all

        select
            2 ptype
            ,w.schid
            ,w.kodoper
            ,w.schname
        """
        
        res = re.sub('union( all)?',lambda m:f"\n\n{m.group(0)}\n\n",text)
        return res
        
    def ПарсингПроцедуры(text:str)->str:
        """
        ::IN::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

create or alter procedure MAIL_CLIENT_REPLACE
(
  PCODE type of column MAIL_LOG.PCODE,
  AREFID type of column MAIL_LOG.MAILTYPEID,
  AMAILHEADER type of column MAIL_LOG.MAILHEADER,
  AMAILTEXT type of column MAIL_LOG_DATA.MAILDATA = null,
  IS_SMS integer = 1
)
returns (
  MAILHEADER type of column MAIL_LOG.MAILHEADER,
  MAILTEXT type of column MAIL_LOG_DATA.MAILDATA,
  UID type of column MAIL_LOG.UID,
  PROTOCOLID type of column MAIL_LOG.PROTOCOLID,
  FULLNAME type of column CLIENTS.FULLNAME,
  LASTNAME type of column CLIENTS.LASTNAME,
  FIRSTNAME type of column CLIENTS.FIRSTNAME,
  MIDNAME type of column CLIENTS.MIDNAME,
  CLBDATE type of column CLIENTS.BDATE,
  CLMAIL type of column CLIENTS.CLMAIL,
  REFUSECLMAIL type of column CLIENTS.REFUSECLMAIL,
  REFUSESMS type of column CLIENTS.REFUSESMS,
  AGRID type of column CLHISTNUM.AGRID,
  LSTID type of column CLHISTNUM.LSTID,
  BDATE type of column CLHISTNUM.BDATE,
  FDATE type of column CLHISTNUM.FDATE,
  DATECANCEL type of column CLHISTNUM.DATECANCEL,
  NSP type of column CLHISTNUM.NSP,
  HISTID type of column CLHISTNUM.HISTID,
  PHONE1 type of column CLIENTS.PHONE1,
  PHONE2 type of column CLIENTS.PHONE2,
  PHONE3 type of column CLIENTS.PHONE3,
  JNAME type of column JPERSONS.JNAME,
  JNAME2 type of column JPERSONS.JNAME2,
  AGNUM type of column JPAGREEMENT.AGNUM,
  AGNAME type of column JPAGREEMENT.AGNAME,
  AGRNAME type of column JPAGREEMENT.AGNAME,
  LISTNAME type of column JPLISTS.SHORTNAME,
  KATEG type of column CLGROUP.GRCOD,
  GRNAME type of column CLGROUP.GRNAME,
  JID type of column JPERSONS.JID,
  DNAME type of column DOCTOR.FULLNAME,
  NAMEPLACE type of column WORKPLACEDOC.NAMEPLACE,
  VALUE_FIELD type of column MAIL_LOG.MAILHEADER,
  CLLOGIN type of column CLIENTS.CLLOGIN,
  CLPASSTYPE type of column CLIENTS.CLPASSTYPE,
  CLPASSWORD type of column CLIENTS.CLPASSWORD,
  FULLNAME2 type of column CLIENTS.FULLNAME2,
  SUFFIX TTEXT12,
  CLBDATE_TXT TTEXT24,
  BDATE_TXT TTEXT24,
  FDATE_TXT TTEXT24,
  DATECANCEL_TXT TTEXT24,
  AGRFULLNAME TTEXT1024,
  TRADEMARK TTEXT1024,
  AGRNAMETYPE TICODE,
  JNAMETYPE TICODE,
  RECTYPE integer,
  CHECKCODE integer,
  JTSHORTNAME type of column JPAGRTYPES.SHORTNAME,
  JSHORTNAME2 type of column JPERSONS.SHORTNAME2,
  JSHORTNAME type of column JPERSONS.SHORTNAME,
  FILID type of column FILIALS.FILID,
  FILIALNAME type of column FILIALS.FULLNAME,
  FIL_ADDR TTEXT1024,
  FIL_ADDR_SHORT TTEXT1024,
  ERRORTEXT type of column SMS_LOG.ERRORTEXT,
  AGEINYEARS integer,
  YEARSOLD_FROM integer,
  YEARSOLD_TO integer,
  STATE type of column SMS_LOG.STATE 
)
as 
declare variable REQUIRED type of column MAIL_TEMPLATE_WORDS.REQUIRED;
declare variable SQLTEXT type of column MAIL_TEMPLATE_WORDS.SQLTEXT;
declare variable WORDNAME type of column MAIL_TEMPLATE_WORDS.MAILWORDNAME;
declare variable WORDCOMMENT type of column MAIL_TEMPLATE_WORDS.comment;
begin
  if (coalesce(pcode, 0) = 0) then exit;

  mailheader = amailheader;
  mailtext = amailtext;
  errortext = '';

  is_sms = coalesce(is_sms, 0);
  if (is_sms = 1) then rectype = 501; else rectype = 550;

  select first 1 c.fullname, c.lastname, c.firstname, c.midname,
    c.bdate, c.clmail, c.refuseclmail, c.refusesms, hist1.agrid,hist1.lstid, hist1.bdate, hist1.fdate,
    hist1.datecancel, hist1.nsp, hist1.histid, c.phone1,c.phone2,c.phone3,
    jpers.jname, jpers.jname2, jpers.shortname, jpers.shortname2, jt.shortname, jagr.agnum, jagr.agname, jlst.shortname,
    cgr.grcod kateg, cgr.grname, jpers.jid, decode(c.pol, 1, 'ый', 2, 'ая', 'ый(я)'), c.fullname2,
    c.cllogin, c.clpasstype, c.clpassword,  maxvalue(chk_124.checkcode, chk_125.checkcode, chk_126.checkcode)
  from clients c
    left join clhistnum hist1 on (hist1.pcode = c.pcode and hist1.mainjid = 1)
    left join jpagreement jagr on jagr.agrid = hist1.agrid
    left join jpersons jpers on jpers.jid = jagr.jid
    left join jplists jlst on jlst.lid = hist1.lstid
    left join jpagrtypes jt on jagr.agrtype = jt.agrtype
    left join clgroup cgr on cgr.grcod = c.grtype

    left join recgroup_checkrights(:rectype, :arefid, 124, c.cstatus) chk_124 on 1 = 1
    left join recgroup_checkrights(:rectype, :arefid, 125, c.typestatus) chk_125 on 1 = 1
    left join recgroup_checkrights(:rectype, :arefid, 126, c.agestatus) chk_126 on 1 = 1
  where c.pcode = :pcode
  order by hist1.modifydate desc
  into fullname, lastname, firstname, midname,
    clbdate, clmail, refuseclmail, refusesms, agrid, lstid, bdate, fdate,
    datecancel, nsp, histid, phone1, phone2, phone3,
    jname, jname2, jshortname, jshortname2, jtshortname, agnum, agname, listname,
    kateg, grname, jid, suffix, fullname2,
    cllogin, clpasstype, clpassword, checkcode;

  if (row_count = 0) then exit;

  select datestr from formatdate(:clbdate) into clbdate_txt;
  select datestr from formatdate(:bdate) into bdate_txt;
  select datestr from formatdate(:fdate) into fdate_txt;
  select datestr from formatdate(:datecancel) into datecancel_txt;

  select keyvalue from getconfig('TradeMark','') into TradeMark;

  select keyvalue from getconfig('JNameType', '0') into JNameType;
  AGRFULLNAME = coalesce(case JNameType when 1 then jname2 when 3 then jshortname when 4 then jshortname2 else jname2 end, '');

  select keyvalue from getconfig('AgrNameType', '0') into AgrNameType;
  if (AgrNameType <> 2) then
    agrname = coalesce(case AgrNameType when 1 then agname else agnum end, '');
  else
    agrname = '';

  if (agrname > '') then
    AGRFULLNAME = AGRFULLNAME || ' Дог. ' || agrname;

  if (jtshortname > '') then
    AGRFULLNAME = AGRFULLNAME || ' ' || jtshortname;

  AGRFULLNAME = AGRFULLNAME || coalesce(' ' || listname, '');

  if (coalesce(amailheader, '') = '' and coalesce(amailtext, '') = '') then
  begin
    suspend;
    exit;
  end

  if (coalesce(fullname, '') = '') then
    fullname = coalesce(lastname || ' ', '') || coalesce(firstname || ' ', '') || coalesce(midname, '');

  select current_filial from s_session_info into filid;
  select fullname from filials where filid = :filid into filialname;

  select fa.addrfull, fja.addrshort from jpaddress fja
    left join getaddress(fja.addrid) fa on 1 = 1
  where fja.RecId = :filid and fja.RecType = 1 and fja.addrtype = 0
  into fil_addr, fil_addr_short;

  for
  select mailwordname, sqltext, required, comment from mail_template_words
  where mailtypeid = 0 and :is_sms = 0
  union
  select t.smswordname, sqltext, required, t.comment from sms_template_words t
    inner join sms_type_template tt on tt.smstemplid = t.smstemplid
  where tt.smstypeid = 0 and :is_sms = 1
  into wordname, sqltext, required, wordcomment do
  begin
    value_field = '';
    if (sqltext > '') then
    begin
      if (position(':internalid', lower(sqltext)) = 0) then
        value_field = value_field || ' and ' || coalesce(pcode, '0') || ' = coalesce(:internalid, 0)';
      if (position(':rectype', lower(sqltext)) = 0) then
        value_field = value_field || ' and ' || coalesce(rectype, '0') || ' = coalesce(:rectype, 0)';

      if (value_field > '') then
        sqltext = 'select * from (' || sqltext || ') where' || substring(value_field from 5);

      execute statement (sqltext) (internalid := :pcode, rectype := :rectype)
      into value_field;
    end
    else
      value_field = case upper(wordname)
        when 'ФИО' then fullname
        when 'ФАМИЛИЯ' then lastname
        when 'ИМЯ' then firstname
        when 'ОТЧЕСТВО' then midname
        when 'ОКОНЧАНИЕ ЫЙ ИЛИ АЯ' then suffix
        when 'ИНОСТРАННОЕ ФИО' then fullname2
        when 'ДАТА РОЖДЕНИЯ' then clbdate_txt

        when '№ ПОЛИСА' then nsp
        when 'ДАТА НАЧАЛА ОБСЛУЖИВАНИЯ' then bdate_txt
        when 'ДАТА ОКОНЧАНИЯ ОБСЛУЖИВАНИЯ' then fdate_txt
        when 'ДАТА АННУЛИРОВАНИЯ' then datecancel_txt

        when 'НАЗВАНИЕ ДОГОВОРА' then AGRFULLNAME
        when 'НАЗВАНИЕ ПРОГРАММЫ ПРИКРЕПЛЕНИЯ' then listname
        when 'НАИМЕНОВАНИЕ КЛИНИКИ' then TradeMark

        when 'ФИЛИАЛ' then filialname
        when 'АДРЕС ФИЛИАЛА' then fil_addr
        when 'КРАТКИЙ АДРЕС ФИЛИАЛА' then fil_addr_short

        when 'САЙТ ЛИЧНОГО КАБИНЕТА' then
          (select keyvalue from getconfig('IcRuSiteAddr', ''))
        when 'ЛОГИН И ПАРОЛЬ' then
          'логин ' || coalesce(cllogin, '') || ' и ' ||
          trim(iif(clpasstype > 0, 'пароль ' || coalesce(clpassword, ''), 'Ваш пароль'))
        else ''
      end;

    if (value_field > '' or coalesce(required, 0) = 0) then
    begin
      mailheader = replace(mailheader, '[' || wordname || ']', coalesce(value_field, ''));
      mailtext = replace(mailtext, '[' || wordname || ']', coalesce(value_field, ''));
    end
    -- если обязательное поле не заполнено и оно есть в шаблоне, то ругань
    else if (position('[' || wordname || ']', mailheader) + position('[' || wordname || ']', mailtext) > 0) then
      if (wordcomment > '') then
        errortext = errortext = ', ' || wordcomment;
      else
        errortext = errortext = ', ' || wordname;
  end

  if (is_sms = 1) then
  begin
    mailheader = trim(replace(mailheader, '  ', ' '));
    mailtext = trim(replace(mailtext, '  ', ' '));
  end

  if (errortext > '') then
    errortext = substring('Не заполнены значения: ' || substring(errortext from 3) from 1 for 1024);

  if (coalesce(state, 0) = 0) then
  begin
    -- Возраст, от
    select propvalueint from getrecpropvalues(1502019, :arefid, :rectype, 1) into yearsold_from;
    yearsold_from = coalesce(yearsold_from, 0);

    -- Возраст, до
    select propvalueint from getrecpropvalues(1502020, :arefid, :rectype, 1) into yearsold_to;
    yearsold_to = coalesce(yearsold_to, 0);

    if (yearsold_from > 0 or yearsold_to > 0) then
    begin
      select ageinyears from getage(:bdate, current_date) into ageinyears;
      if (yearsold_from > 0 and ageinyears < yearsold_from
      or  yearsold_to > 0 and ageinyears > yearsold_to) then
        state = -12;
    end
  end

  suspend;
end
        """
        
        def _self(m:re.Match):
            m_p = Procedure(m.groupdict())    
            return ''
        
        Procedure.regex.sub(_self,text)
            

class NT_Regex():
    """
    Базовый класс для хранения и использования регялрного выражения
    
    
    ::Прмиер испоьзования::::::::::::::::::::::::
    
    def _self(m:re.Match):
        m_p = Procedure(m.groupdict())    
        return ''
    
    Procedure.regex.sub(_self,text)
    """
    def __init__(self,kwargs):
        for k,v_type in self.__class__.__dict__['__annotations__'].items():     
            # Если имя параметра не начинается на `regex` то заносим такой параметер в self.__dict__    
            if not re.match('regex',k):
                self.__dict__[k] = v_type(kwargs[k])
            else:
                # Проверка типа для параметров которые начинаются на `regex`
                if v_type !=  re.Pattern:
                    raise TypeError(f'Параметер {k} должен быть типа re.Pattern')

class Procedure(NT_Regex):
    
    regex:re.Pattern = re.compile("create(?: or alter)? procedure (?P<nmae_proc>.+)\s*\(\n*(?P<in_params>(?:.\s*(?!returns))+)\)\s*returns\s*\(\n*(?P<returns>(?:.\s*(?!as))+)\)\s*as\s*(?P<declare>\s*(?:.\s*(?!begin))+[\w\W])begin\n*(?P<code>\s*(?:.\s*(?!\nend))+)end")
        
    nmae_proc:str
    in_params:str
    returns:str
    declare:str
    code:str

"""
TODO:::
Не верно работает Format_CASE

        ,cast(left(coalesce(clh_nspnum, ''), 20) as CHAR(20)) POLIS_N
                ,cast(left(jp_lpucode2,3) as numeric(3,0)) ID_SMO
                ,cast(clh_bdate as date) POLIS_BD
                ,cast(iif(coalesce(clh_datecancel, '01.01.3000') > clh_fdate, clh_fdate, clh_datecancel) as date) POLIS_ED
                ,null ID_SMO_REG
                cast((case when cl_KLCODE_REG like '78%' then 'г'                 
 when not di_SIMPLENAME containing 'РОССИ' THEN 'п'                when (not cl_KLCODE_REG is null or (not ter_reg_TCODE is null and cl_KLCODE_REG is null)) then 'р'                ,else '' 
end) as Character (1)) ADDR_TYPE
                ,iif(cl_kodter_reg is not null, cast(cl_kodter_reg as smallint), cast(cklr_tercode as smallint)) IDOKATOREG
                ,null IDOBLTOWN
                ,null ID_PREFIX
                
    

"""


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