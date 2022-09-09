        ,u.fullname uname
        ,i.orderno numdoc
        ,iif(ir.ir>0,ir.irname,
                trim(case i.paycode
                        when 1 then 'Оплата лечения!' when 2 then 'Внесение аванса'
                                when 3 then 'Оплата долга' 
when 5 then 'Возврат личного аван!са'
                        when 6 then 'Выдача денег из кассы'  end)) text
        ,iif(i.paycode in (1,2,3        
        