select info.pcode as p, info.*

    res                                                               -- Откуда ответ
    ,fullname                                                         -- ФИО пациента 	
    ,KODOPER || ' : ' || SCHNAME || ' : ' || SCHID as SCHNAME         -- Услуга 
    ,d_fullname                                                       -- ФИО врача 	
    ,(select DEPNAME from DEPARTMENTS where DEPNUM=O_DEPNUM),O_DEPNUM -- Отделение 	
    ,cast(TREATDATE as date) as TREATDATE                             -- Дата приема 
    ,RUB_TO                                                           -- Получено
    ,RUB_FROM                                                         -- Выдано


       , iif((select result from mds_check_terms(info.rep_yellow
                                                , info.rep_light_orange
                                                , info.rep_blue
                                                , info.rep_green
                                                , info.rep_orange
                                                , info.rep_purple
                                                , info.rep_purple_2
                                                )) = 1
            , 'Да'
            , 'Нет'
            ) bad
from
( select all_clr.refid, all_clr.pcode, all_clr.fullname, all_clr.histnum, all_clr.bdate, all_clr.rtreatcode, all_clr.todepart, all_clr.mkbcode, all_clr.mkbcode1,
all_clr.ext_comment, all_clr.dname, all_clr.extrefdate, all_clr.createdate, all_clr.treatdate, all_clr.rtreatdate, all_clr.ref_bio_date, all_clr.ref_bio_date_resp,
all_clr.date_sched_rep, all_clr.date_rep, all_clr.date_out
, coalesce(dg2.mkbcode,all_clr.diag_out) diag_out
, ref_rep_refid
, all_clr.date_old
, cast(((select sum((select is_working_day
						from its_rep_is_working_day(days_in_range)))
				from mds_rep_date_range(cast(createdate as date), cast(treatdate as date))) - 2) as int)  rep_yellow


		, cast(((select sum((select is_working_day
						from its_rep_is_working_day(days_in_range)))
				from mds_rep_date_range(cast(treatdate as date), cast(rtreatdate as date))) - 1)as int)  rep_light_orange


		, cast(((select sum((select is_working_day
						from its_rep_is_working_day(days_in_range)))
				from mds_rep_date_range(cast(ref_bio_date as date), cast(ref_bio_date_resp as date))) - 1)as int)  rep_blue


		, cast(((select sum((select is_working_day
						from its_rep_is_working_day(days_in_range)))
				from mds_rep_date_range(cast(rtreatdate as date), cast(ref_bio_date as date))) - 1)as int)  rep_green


  	, cast(((select sum((select is_working_day
						from its_rep_is_working_day(days_in_range)))
				from mds_rep_date_range(cast(date_rep as date), cast(date_out as date))) - 1)as int)  rep_orange



		, cast(((select sum((select is_working_day
						from its_rep_is_working_day(days_in_range)))
				from mds_rep_date_range(cast(rtreatdate as date), cast(date_rep as date))) - 1) as int) rep_purple


		, cast(((select sum((select is_working_day
						from its_rep_is_working_day(days_in_range)))
				from mds_rep_date_range(cast(rtreatdate as date), cast(date_sched_rep as date))) - 1) as int) as rep_purple_2
from
(select
     cl_ref.refid
		, cl_ref.pcode
		, cl.fullname
		, cl.histnum
		, cl.bdate
		, cl_ref.rtreatcode
		, cl_ref.todepart

		, dg.mkbcode

		, dg1.mkbcode mkbcode1
		, cl_ref.comment ext_comment


    , d.dname


		, cl_ref.extrefdate


		, cast(cl_ref.treatdate as date) as createdate


		, cl_ref.scheddate as treatdate


		, cl_ref.rtreatdate

    , cl_ref_bio.treatdate as ref_bio_date

		, tp_bio_resp.treatdate as ref_bio_date_resp


    , max(iif(ref_out.treatdate > t.treatdate or ref_out.treatdate is null,coalesce(iif(t2.treatcode <> t.treatcode, ref_out.treatdate, null),s.workdate,p_povt.valuedate),null)) as date_sched_rep


		, max(iif(ref_out.treatdate > t.treatdate or ref_out.treatdate is null,coalesce(iif(t2.treatcode <> t.treatcode, ref_out.treatdate,null),t2.treatdate),null)) as date_rep

    , max(t2.treatcode) as treatcode_rep

    , max(iif(ref_out.treatdate < t2.treatdate,t2.treatdate,null)) date_old

		, ref_out.treatdate  date_out
    , ref_rep.refid ref_rep_refid

    , coalesce(diag_out.mkbcode, dg1.mkbcode) as diag_out

from clreferrals cl_ref
left join treat t on cl_ref.rtreatcode = t.treatcode
left join doctor as d on t.dcode = d.dcode
left join diagnosis dg on cl_ref.dgcode = dg.dgcode
left join clients as cl on cl_ref.pcode = cl.pcode
inner join diagclients  as dcl on cl_ref.rtreatcode = dcl.objcode
                                  and dcl.objtype = 1
                                  and dcl.dgtypecode = 1
left join diagnosis dg1 on dcl.dgcode = dg1.dgcode
left join treatplace tp_onko1 on t.treatcode = tp_onko1.treatcode and tp_onko1.placeid = 10000370 and tp_onko1.pstate = 1 -!- протокол онколога в первичном приеме
left join paramsinfo p_povt on p_povt.protocolid = tp_onko1.protocolid and p_povt.codeparams = 10007160 and p_povt.ver_no = 0 -!- запись на повторный прием
left join paramsinfo p_bio on p_bio.protocolid = tp_onko1.protocolid and p_bio.codeparams = 10016069 and p_bio.ver_no = 0 -!- запись на биопсию
left join clreferrals as cl_ref_bio on cl_ref.pcode = cl_ref_bio.pcode

								and cl_ref_bio.reftype = 10000778
                and cl_ref_bio.treatdate >= cl_ref.rtreatdate
left join treatplace tp_bio_resp on cl_ref.pcode = tp_bio_resp.pcode


								and tp_bio_resp.placeid = 10000403
                and tp_bio_resp.treatdate >= cl_ref.rtreatdate
left join schedule s on s.pcode = t.pcode and s.dcode = t.dcode and s.workdate > t.treatdate and s.workdate between [bdate] and [fdate]
left join treat t2 on t2.treatcode = s.treatcode
left join diagclients  as dc2 on t2.treatcode = dc2.objcode
                                  and dc2.objtype = 1
                                  and dc2.dgtypecode = 1
left join diagnosis dg2 on dc2.dgcode = dg2.dgcode
left join clreferrals as ref_rep on ref_rep.rtreatcode = t2.treatcode
                                                                                --and ref_rep.reftype in (970000487, 10000834)

left join clreferrals ref_out on cl_ref.pcode = ref_out.pcode

								and ref_out.reftype in (10000858,10018280)

								and ref_out.treatdate > cl_ref.treatdate
                and ref_out.refid = (select min(refid) from clreferrals ref_out_temp where cl_ref.pcode = ref_out_temp.pcode

								and ref_out_temp.reftype in (10000858,10018280)

								and ref_out_temp.treatdate > cl_ref.treatdate)
left join diagnosis diag_out on ref_out.dgcode = diag_out.dgcode
where cl_ref.reftype in (970000487, 10000834)
	and  cl_ref.treatdate between [bdate] and [fdate]
    and (((dg.mkbcode between 'C00' and 'C97.99')
			or (dg.mkbcode between 'D00' and 'D89.99')
			or (dg.mkbcode = 'Z03.1'))
      and
      ((dg1.mkbcode between 'C00' and 'C97.99')
			or (dg1.mkbcode between 'D00' and 'D89.99')
			or (dg1.mkbcode = 'Z03.1'))
		)

  and cl_ref.todepart not in (10000120, 10000125, 10000123)
  /*and not exists (select a.acdid from accident a
                  join diagnosis dg_temp on a.finaldiag = dg_temp.dgcode
                  where a.acdpcode = t.pcode and a.finaldate < cl_ref.treatdate
                  and (dg_temp.grpcode = dg.grpcode or dg_temp.grpcode = dg1.grpcode or dg_temp.grpcode = diag_out.grpcode) and dg1.mkbcode <> 'Z03.1')*/
  and ref_rep.refid is null

group by 3,2,1,4,5,6,7,8,9,10,11,12,13,14,15,16,17,22,23,24) all_clr
left join diagclients  as dc2 on all_clr.treatcode_rep = dc2.objcode
                                  and dc2.objtype = 1
                                  and dc2.dgtypecode = 1
left join diagnosis dg2 on dc2.dgcode = dg2.dgcode
where (dg2.mkbcode between 'C00' and 'C97.99')
			or (dg2.mkbcode between 'D00' and 'D89.99')
			or (dg2.mkbcode = 'Z03.1') or dg2.mkbcode is null
)   info