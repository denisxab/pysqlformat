    SCHID --  id услуги
    ,KODOPER -- код услуги
    ,SCHNAME -- имя услуг
    ,SNAME -- специальность услуг
    ,count(SCHID) as s_count -- количество услуг 
    ,sum(COST_US) as s_sum -- сумма услуг
    ----- DEBUG ----------------------------
    ,list(PRICE_P_RUB) as PRICE_P_RUB
    ,list(SCOUNT) as SCOUNT
    ,list(HOW_GET_WSHEMA) as HOW_GET_WSHEMA
    ,list(IN_DATE) as IN_DATE
    ,list(FULLNAME) as FULLNAME