
import re 

text = """
with eis as (
    select
        cast(iif(diag.mkbcode containing 'C', left(t.orderno || right(coalesce(p_kt.organid, p_mrt.organid),2),11),t.orderno) as numeric(11,0)) SERV_ID,
        cast(eis_diag.id_diagnosis as numeric(11,0)) ID_DIAGNOS,
        cast(3388 as numeric(11,0)) DS1_T,
        cast(null as numeric(11,0)) ID_ST,
        cast(null as numeric(11,0)) ID_T,
        cast(null as numeric(11,0)) ID_N,
        cast(null as numeric(11,0)) ID_M,
        cast(null as numeric(6,0)) MTSTZ,
        cast(null as numeric(10,0)) SOD,
        cast(null as numeric(2,0)) K_FR,
        cast(null as numeric(5,0)) WEI,
        cast(null as numeric(3,0)) HEI,
        cast(null as numeric(4,0)) BSA,
        cast(null as char(200)) ERROR
    from ITS_EIS_A_L_KT_MRT
    where
        t.treatdate between [bdate and :fdate
        and t.depnum = 10000123 -- отделение "Отдел лучевой диагностики"
        and coalesce(rpv3.propvalueint,0) = 0 --признак ЛПУ "не включать в выгрузку"
        and dic_goal.rekvint3 in (36,37) -- цель направления только КТ/МРТ
        and diag.mkbcode containing 'C' -- вкладка нужна только для онко
    group by cl.lastname, cl.pcode, cl.firstname, cl.midname, c.bdate, c.sex, ch.nspser, ch.nspnum, w2.kodoper, w2.schid, c.ageinyears, t.treatdate, c.doctypeid, c.paspser,  c.paspnum, t.orderno, c.histnum, w2.kodoper, ga.ageinyears, sl.kolvo, diag.mkbcode,c.doctypeid, t.treatcode, dic_prvs.rekvint2, w.treattype, ddl.extcode, dp.speccode, rpv2.propvalueint, dic_c_zab.rekvint2,dic_c_zab.rekvint3, dic_goal.rekvint3, dic_goal.rekvint2, clr.extrefid,clr.refid,clr.extrefdate,clr.treatdate, rpv.propvalueint, diag_main.mkbcode, p_kt.organid, p_mrt.organid, eis_diag.id_diagnosis
)
"""

res = re.sub('(\w+)\.(\w+)',lambda m: f'{m.group(1)}_{m.group(2)}',text)
print(res)