select
    1 as type_res,
    cast(t_orderno as numeric(11,0)) SERV_ID,
    cast(left(coalesce(cl_lastname, ''),40) as char(40)) SURNAME,
    cast(left(coalesce(cl_firstname, ''),40) as char(40)) NAME,
    cast(left(coalesce(cl_midname, ''),40) as char(40)) S_NAME,
    c_bdate BIRTHDAY,
    c_sex SEX,
    cast(2 as numeric(6,0)) ID_PAT_CAT,
    null LGOTS,
    cast(
    (case cl_PASPTYPE
    when 1 then 1
    when 5 then 3
    when 12 then 16
    else 22 -- вариант "без граж документ", до этого стояло "прочее", еис не принимал
    end) as smallint) DOC_TYPE,
    cast(
    iif(left(right(cl_paspser, 3), 1) = '-' or left(right(cl_paspser, 3), 1) = ' ',
    left(trim(left(cl_paspser, iif((CHAR_LENGTH(cl_paspser) - 3) < 0, 0, CHAR_LENGTH(cl_paspser) - 3))), 6),
    left(trim(left(cl_paspser, iif((CHAR_LENGTH(cl_paspser) - 2) < 0, 0, CHAR_LENGTH(cl_paspser) - 2))), 6))
    as char(6)) SER_L,
    cast(right(coalesce(cl_paspser, ''), 2) as char(2)) SER_R,
    cast(left(coalesce(cl_paspnum, ''), 12) as char (12)) DOC_NUMBER,
    cl_paspdate ISSUE_DATE,
    cl_paspplace as DOCORG_ASMEMO,
    cast(left(coalesce(cl_snils, ''), 14) as char (14)) SNILS,
    coalesce(di_REKVTEXT1,'000') C_OKSM,
    null IS_SMP,
    cast(clh_nsptype as smallint) POLIS_TYPE,
    clh_nsp,
    cast(left(coalesce(clh_nspser, ''), 20) as CHAR(20)) POLIS_S,
    cast(left(coalesce(clh_nspnum, ''), 20) as CHAR(20)) POLIS_N,
    cast(left(jp_lpucode2,3) as numeric(3,0)) ID_SMO,
    cast(clh_bdate as date) POLIS_BD,
    cast(iif(coalesce(clh_datecancel, '01.01.3000') > clh_fdate, clh_fdate, clh_datecancel) as date) POLIS_ED,
    null ID_SMO_REG,
    cast((case when cl_KLCODE_REG like '78%' then 'г'
    when not di_SIMPLENAME containing 'РОССИ' THEN 'п'
    when (not cl_KLCODE_REG is null or (not ter_reg_TCODE is null and cl_KLCODE_REG is null)) then 'р'
    else '' end) as Character (1)) ADDR_TYPE,
    iif(cl_kodter_reg is not null, cast(cl_kodter_reg as smallint), cast(cklr_tercode as smallint)) IDOKATOREG,
    null IDOBLTOWN,
    null ID_PREFIX,
    cast(left(gh_id_house,9) as Numeric (9,0)) ID_HOUSE,
    iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like '78%', cast(left(coalesce(cl_addr_reg, ''), 10) as char(10)), null) HOUSE,
    iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like '78%', cast(left(coalesce(cl_corp_reg, ''), 5) as char(5)), null) KORPUS,
    iif(coalesce(cklr_klcode, 0) > 0 , cast(left(coalesce(cl_flat_reg, ''), 5) as char(5)), null) FLAT,
    iif(coalesce(cklr_klcode, 0) > 0 and not di_SIMPLENAME containing 'РОССИ', cast(left(coalesce(c_addr_reg, ''), 200) as char(200)),null) U_ADDRESS,
    iif(coalesce(klr_code, 0) > 0 and not cl_KLCODE_REG like '78%', cast(left(coalesce(klr_code, ''), 13) as char(13)), null) KLADR_CODE,
    iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like '78%', cast(left(coalesce(cklr_stname, ''), 150) as char(150)), null) STREET,
    iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like '78%', gh_idstrtype, null) IDSTRTYPE,
    cast((case when cl_KLCODE like '78%' then 'г'
    when not di_SIMPLENAME containing 'РОССИ' THEN 'п'
    when (not cl_KLCODE is null or (not ter_reg_TCODE is null and cl_KLCODE is null)) then 'р'
    else '' end) as Character (1)) ADDRTYPE_L,
    iif(cl_kodter is not null, cast(cl_kodter as smallint), cast(cklf_tercode as smallint)) OKATOREG_L,
    null OBLTOWN_L,
    null PREFIX_L,
    cast(left(gh_fact_id_house,9) as Numeric (9,0)) ID_HOUSE_L,
    iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like '78%', cast(left(coalesce(cl_addr, ''), 10) as char(10)), null) HOUSE_L,
    iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like '78%', cast(left(coalesce(cl_corp, ''), 5) as char(5)), null) KORPUS_L,
    iif(coalesce(cklf_klcode, 0) > 0 , cast(left(coalesce(cl_flat, ''), 5) as char(5)), null) FLAT_L,
    iif(coalesce(cklf_klcode, 0) > 0 and not di_SIMPLENAME containing 'РОССИ', cast(left(coalesce(c_addr_fact, ''), 200) as char(200)),null) U_ADDR_L,
    iif(coalesce(klf_code, 0) > 0 and not cl_KLCODE like '78%', cast(left(coalesce(klf_code, ''), 13) as char(13)), null) KLADR_L,
    iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like '78%', cast(left(coalesce(cklf_stname, ''), 150) as char(150)), null) STREET_L,
    iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like '78%', gh_fact_idstrtype,null) STRTYPE_L,
    cast(left(coalesce(c_workplace, ''), 254) as CHAR(254)) PLACE_WORK,
    null ADDR_WORK,
    null ADDR_PLACE,
    null REMARK,
    cast(left(coalesce(c_BIRTHPLACE, ''), 100) as char(100)) B_PLACE,
    null VNOV_D,
    null ID_G_TYPE,
    null G_SURNAME,
    null G_NAME,
    null G_S_NAME,
    null G_BIRTHDAY,
    null G_SEX,
    null G_DOC_TYPE,
    null G_SERIA_L,
    null G_SERIA_R,
    null G_DOC_NUM,
    null G_ISSUE_D,
    null G_DOCORG_ASMEMO,
    null G_B_PLACE,
    cast(0 as INTEGER) N_BORN, -- не исправлять на numeric !
    null SEND,
    null ERROR,
    null ID_MIS,
    null ID_PATIENT,
    T_TREATCODE
    ,D_DCODE           
from ITS_EIS_A_L_O as eis
where
    eis.t_treatdate between :bdate and :fdate
    and eis.t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
    and (eis.dic_goal_dicid is not null or (eis.clr_goal = 0 or eis.clr_goal is null))   -- цель направления (НЕ КТ/МРТ) или отсутвие цели
    and eis.w_KODOPER  != '0011' -- Услуга в приеме должна быть не КТ/МРТ