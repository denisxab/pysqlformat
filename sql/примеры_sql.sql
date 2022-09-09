'ITS_NMQ_PROC_REFERRAL            declare node tblob;
declare subnode tblob;
declare cnt bigint;
declare sub_error_code bigint;
declare sub_error_text varchar(1024);
declare refid_in_journal type of column mds_nmq_referral_journal.refid;
declare dcode type of column doctor.dcode;
declare eventsinfo ttext4096;
declare diagtext type of column clreferrals.diagtext;
declare extdepnum type of column clreferrals.extdepnum;
declare comment type of column clreferrals.comment;
declare log_detailed_info type of column mds_nmq_log.detailed_info;
declare dummy_mo_jid type of column jpersons.jid;
-- -- -- данные об аннулировании направления
declare ei_cancellation_reason type of column mds_nterm_cancellation_reason.code;
declare ei_cancellation_date date;
declare ei_cancellation_reasoncomment ttext1024;
declare ei_source_referraloutdate date;
declare ei_source_planneddate date;
declare old_cancellation_status type of column mds_nmq_referral_journal.cancellation_status;
declare new_cancellation_status type of column mds_nmq_referral_journal.cancellation_status;

-- -- данные о пациенте
-- -- -- узлы
declare patient tblob;

-- -- данные о направлении
declare referralinfo tblob;
declare ext_reason type of column mds_nmq_referral_journal.ext_reason;
declare ext_comment type of column mds_nmq_referral_journal.ext_comment;
declare ext_type type of column mds_nmq_referral_journal.ext_type;
declare ext_profile_med_service type of column mds_nmq_referral_journal.ext_profile_med_service;
declare ext_profilemedservice_code type of column mds_nmq_referral_journal.ext_status_code;
declare ext_source_lpu type of column mds_nmq_referral_journal.ext_source_lpu;
declare ext_priority ttext512;

-- -- данные о событиях в направляющей МО
declare source tblob;
declare source_doctors_node tblob;
declare source_doctor_familyname ttext64;
declare source_doctor_givenname ttext64;
declare source_doctor_middlename ttext64;
declare source_diag_code ttext16;

-- -- данные о событиях в целевой МО
declare target tblob;

declare filial bigint;

-- константы
declare end_l varchar(2) = ''
'';
-- -- Спецификаторы номера узла
declare ALL_NODES smallint = null;
declare SECOND_NODE smallint = 2;
-- -- обязательность узла
declare OPTIONAL smallint = 0;
declare REQUIRED smallint = 1;
declare REQUIRED_NILL smallint = 2;
declare REQ_EXCEPTION smallint = 2;
-- -- Статус аннулирования в журнале направлений
declare CANCELLATION_UNEXIST type of column mds_nmq_referral_journal.cancellation_status = 0; -- Нет данных об аннулировании
declare CANCELLATION_CHANGED type of column mds_nmq_referral_journal.cancellation_status = 1; -- Есть неотправленные изменения
declare CANCELLATION_SENT type of column mds_nmq_referral_journal.cancellation_status = 2; -- Сведения об аннулировании отправлены
-- Тип ЛПУ (для типа направления, CLREFERRALS.EXTMEDICAL)
declare FROM_EXTERNAL_LPU type of column CLREFERRALS.EXTMEDICAL = 2; -- 2 - направление из внешнего ЛПУ
-- -- статусы сессий обмена
declare AWAITING_ANSWER smallint = 0;
declare SECCESS smallint = 1;
declare ERROR smallint = 2;
-- -- Признак "глубокого" парсинга узла Coding (параметр `deep_parse` процедуры `mds_nterm_Coding_parse`)
declare DEEP_PARSE smallint = 1;
begin
-- generated on 2020-12-28 12:05:44.571466
-- git branch: release-candidate/0.8.0-rc8
-- git SHA-1: 2735dc8380cda6edb68562b30f6c4d6b0b76477b
-- git last changes author: atronah (atronah.ds@gmail.com)
-- git last changes date: Sat Dec 12 14:19:43 2020 +0300
-- git last changes message: fx(api/proc_Referral): правит проблему отсутствия замены фиктивного направляющего МО на реальное
-- 		-
-- 		-Так как расчет идентификатора фиктивной направляющей МО происходил только в случае
-- 		-отсутствия связи МО из направления с МО в МИС,
-- 		-то в случае появления такой связи  идентификатор не вычислялся и потом при проверке, что направляющая МО в направлении МИС является фиктивной и требует замены на реальную - не проходила и замены не проводилось
-- 		-
--

    context = ''mds_nmq_Referral_proc'';
    error_code = 0;
    error_text = '''';

    -- Очистка от псевдонимов пространств имен
    select out_xml from mds_xml_remove_ns(:referral_node) into :referral_node;

    select val from mds_nmq_get_setting(''changes_uid'') into dcode;
    select keyvalue from getconfig(''filial'', null) into filial;


    -- ##############################
    -- ##   Данные о направлении   ##
    -- ##  (Referral/ReferralInfo) ##
    -- ##############################
    -- АйТи Системс - добавила информацию по коду вида исследования
    select
            ext_idmq, ext_date, ext_priority, ext_reason, ext_comment
            , reftype
            , left(''['' || ext_referraltype_code || '']'' || coalesce('' '' || ext_referraltype_name, ''''), 255) as ext_type
            , idpr, extdepnum
            , left(''['' || ext_profilemedservice_code || '']'' || coalesce('' '' || ext_profilemedservice_name, ''''), 255) as ext_profile_med_service
            , ext_profilemedservice_code
            , ext_mqreferralstatus_code, ext_mqreferralstatus_name
            , error_code, error_text, error_details
        from mds_nmq_dec_ReferralInfo((select val from mds_xml_get_value(:referral_node, ''ReferralInfo'', ''node'', :OPTIONAL)))
     into ext_id, ext_date, ext_priority, ext_reason, ext_comment
            , reftype
            , ext_type
            , idpr, extdepnum
            , ext_profile_med_service
            , ext_profilemedservice_code
            , ext_status_code, ext_status_name
            , error_code, error_text, log_detailed_info;
    select first 1 dicid from dicinfo where refid = 48 and extcode = :ext_profilemedservice_code and (disdate < current_date or disdate is null) into ext_profilemedservice_code;
    if (error_code > 0) then
    begin
        execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                            , :log_detailed_info
                                            , :context, :msgid, :referral_journal_id);
    end
    -- ##############################

    -- ###########################
    -- ##   Поиск направления   ##
    -- ###########################
    -- Поиск направления в журнале направлений интеграции
    select
            referral_journal_id, refid
        from mds_nmq_referral_journal
        where ext_id = :ext_id
        into referral_journal_id, refid_in_journal;

    if (coalesce(refid_in_journal, 0) > 0) then
    begin
        if (not exists(select refid from clreferrals where refid = :refid_in_journal)
        ) then
        begin
            error_code = 1;
            error_text = ''Ранее загруженное направление из РЕГИЗ связанно с несуществующим направлнием МИС'';
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , ''Идентификатор направления МИС, указанный в направлении РЕГИЗ: '' || :refid_in_journal
                                                , :context, :msgid, :referral_journal_id);
        end
        else refid = refid_in_journal;
    end

    if (error_code = 0 and coalesce(refid, 0) <= 0) then
    begin
        -- Поиск направления в МИС
        select max(refid), count(refid), max(comment)
            from clreferrals
            where extrefid = :ext_id
            into refid, cnt, comment;

        -- Если было найденно больше одного направления
        if (cnt > 1) then
        begin
            refid = null;
            comment = null;
            error_code = 2;
            error_text = ''Найдено несколько направлений с одинаковым идентификатором УО'';
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , ''Найдено '' || :cnt || '' направлений с кодом '' || ext_id
                                                , :context, :msgid, :referral_journal_id);
        end
    end


    if (referral_journal_id is null)
        then referral_journal_id = next value for mds_nmq_referral_journal_seq;

    if (error_code = 0
        and refid is distinct from refid_in_journal
    ) then
    begin
        if (refid_in_journal is null) then
        begin
            error_code = 3;
            error_text = ''Найденная запись в журнале не имеет ссылки на найденное по внешему номеру направление МИС'';
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , null
                                                , :context, :msgid, :referral_journal_id);
        end
        else
        begin
            error_code = 4;
            error_text = ''Найденная запись в журнале ссылается на направление МИС, отличающееся от найденного по внешнему номеру'';
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , ''mds_nmq_referral_journal.refid = '' || coalesce(:refid_in_journal, ''null'') || :end_l
                                                    || ''clreferrals.refid = '' || coalesce(:refid, ''null'')
                                                , :context, :msgid, :referral_journal_id);
        end
    end
    -- ###########################


    -- ###################################
    -- ##   Расчет целевого отделения   ##
    -- ##          (todepart)           ##
    -- ###################################
    if (error_code = 0) then
    begin
        select first 1 depnum
            from departments
            where idpr = :idpr
            into todepart;

        if (todepart is null) then
        begin
            error_code = 3;
            error_text = ''Не удалось определить целевое отделение по профилю из направления.'';
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , ''departments.idpr = '' || coalesce(:idpr, ''null'')
                                                , :context, :msgid, :referral_journal_id);
        end
    end
    -- ###################################


    -- #######################################
    -- ## Данные о событиях  по направлению ##
    -- ##       (Referral/EventsInfo)       ##
    -- #######################################
    if (error_code = 0) then
    begin
        select val, error_code, error_text
            from mds_xml_get_value(:referral_node, ''EventsInfo'', ''node'', :OPTIONAL)
            into eventsinfo, error_code, error_text;
        -- -- Обработка сведений об аннулировании
        node = null;
        if (error_code = 0)
            then select val, error_code, error_text
                    from mds_xml_get_value(:eventsinfo
                                            , ''Cancellation''
                                            , ''node'', :OPTIONAL)
                    into node, error_code, error_text;

        if (error_code = 0 and coalesce(node, '''') <> '''') then
        begin
            if (error_code = 0)
                then select val, error_code, error_text
                        from mds_xml_get_value(:node
                                                , ''CancellationReason''
                                                , ''str'', :OPTIONAL)
                        into ei_cancellation_reason, error_code, error_text;

            if (error_code = 0)
                then select
                        refvalue, error_code
                        , left(error_text || '' (oid='' || coalesce(oid, ''null'') || coalesce('', code ='' || code, '''') || '')''
                                , 1024)
                    from mds_nterm_coding_to_refvalue(:node)
                    into refcanceltype, error_code, error_text;

            if (error_code = 0)
                then select val_date, error_code, error_text
                        from mds_xml_get_value(:node
                                                , ''Date''
                                                , ''date'', :OPTIONAL)
                        into ei_cancellation_date, error_code, error_text;

            if (error_code = 0)
                then select val, error_code, error_text
                        from mds_xml_get_value(:node
                                                , ''ReasonComment''
                                                , ''str'', :OPTIONAL)
                        into ei_cancellation_reasoncomment, error_code, error_text;
        end

        if (error_code > 0) then
        begin
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , left(''Причина аннулирования (EventsInfo/Cancellation/CancellationReason):'' || coalesce(:ei_cancellation_reason, ''null'') || :end_l
                                                        || ''Дата аннулирования (EventsInfo/Cancellation/Date):'' || coalesce(:ei_cancellation_date, ''null'') || :end_l
                                                        || ''Комментарий аннулирования (EventsInfo/Cancellation/ReasonComment):'' || coalesce(:ei_cancellation_reasoncomment, ''null'')
                                                        , 4096)
                                                , :context, :msgid, :referral_journal_id);
        end
    end

    -- -- данные узла EventsInfo/Source
    node = null;
    if (error_code = 0) then
    begin
        select val, error_code, error_text
                from mds_xml_get_value(:eventsinfo
                                        , ''Source''
                                        , ''node'', :OPTIONAL)
                into node, error_code, error_text;
        if (error_code = 0 and coalesce(node, '''') <> '''') then
        begin
            if (error_code = 0)
                then select val_date, error_code, error_text
                        from mds_xml_get_value(:node
                                                , ''PlannedDate''
                                                , ''date'', :OPTIONAL)
                        into ei_source_planneddate, error_code, error_text;

            if (error_code = 0)
                then select val_date, error_code, error_text
                        from mds_xml_get_value(:node
                                                , ''ReferralCreateDate''
                                                , ''date'', :OPTIONAL)
                        into ext_createdate, error_code, error_text;

            if (error_code = 0)
                then select val_date, error_code, error_text
                        from mds_xml_get_value(:node
                                                , ''ReferralOutDate''
                                                , ''date'', :OPTIONAL)
                        into ei_source_referraloutdate, error_code, error_text;
        end

        if (error_code > 0) then
        begin
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , left(''Плановая дата (EventsInfo/Source/PlannedDate):'' || coalesce(:ei_source_planneddate, ''null'') || :end_l
                                                        || ''Дата создания (EventsInfo/Source/ReferralCreateDate):'' || coalesce(:ext_createdate, ''null'') || :end_l
                                                        || ''Дата выдачи (EventsInfo/Source/ReferralOutDate):'' || coalesce(:ei_source_referraloutdate, ''null'')
                                                        , 4096)
                                                , :context, :msgid, :referral_journal_id);
        end
    end
    -- #######################################


    -- #########################
    -- ##  Данные о пациенте  ##
    -- ##  (Referral/Patient) ##
    -- #########################
    if (error_code = 0) then
    begin
        select val, error_code, error_text
                from mds_xml_get_value(:referral_node, ''Patient'', ''node'', :OPTIONAL)
                into patient, error_code, error_text;

        if (error_code = 0) then
        begin
            select patient_info.pcode
                    , patient_info.error_code
                    , patient_info.error_text
                from mds_nmq_proc_Patient(:patient, :msgid, :referral_journal_id) as patient_info
                into pcode, error_code, error_text;
        end
    end
    -- #########################


    -- ##############################
    -- ## Данные о направляющей МО ##
    -- ##    (Referral/Source)     ##
    -- ##############################
    dummy_mo_jid = (select dummy_mo_jid from mds_nmq_get_dummy_source_mo);
    if (error_code = 0) then
    begin
        select val, error_code, error_text
                from mds_xml_get_value(:referral_node, ''Source'', ''node'', :OPTIONAL, :SECOND_NODE)
                into source, error_code, error_text;

        -- -- Узел с данными ЛПУ
        if (error_code = 0) then
        begin
            select content
                from mds_xml_all_nodes(:source)
                where coalesce(path, '''') = '''' and name = ''Lpu''
                into node;

            select
                    code
                    , left(''['' || code || '']'' || coalesce('' '' || name, ''''), 255) as ext_source_lpu
                    , error_code
                    , left(error_text || '' (oid='' || coalesce(oid, ''null'') || coalesce('', code ='' || code, '''') || '')''
                            , 1024)
                from mds_nterm_coding_parse(:node, :DEEP_PARSE)
                into source_mo_code, ext_source_lpu, error_code, error_text;
            if (error_code = 0) then
            begin
                select max(jid), count(jid)
                    from jpersons
                    where rir_guid = :source_mo_code
                    into fromjid, cnt;
                if (cnt = 0) then
                begin
                    error_code = 1;
                    error_text = ''Не удалось найти МО в справочнике МИС'';
                    log_detailed_info = ''jpersons.rir_guid = '' || coalesce(:source_mo_code, ''null'');
                end
                else if (cnt > 1) then
                begin
                    error_code = 2;
                    error_text = ''Найдено более одного МО в справочнике МИС с одинаковым кодом'';
                    log_detailed_info = ''jpersons.rir_guid = '' || coalesce(:source_mo_code, ''null'');
                end
            end

            if (error_code > 0) then
            begin
                execute procedure mds_nmq_log_add(''warning'', :error_code, :error_text
                                                , ''Произведена замена на фиктивную направляющую МО.'' || :end_l
                                                 || :log_detailed_info
                                                , :context, :msgid, :referral_journal_id);


                fromjid = dummy_mo_jid;
                error_code = 0;
                error_text = '''';
            end
        end
        else
        begin
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , null
                                                , :context, :msgid, :referral_journal_id);
        end
    end

    -- -- Узел с данными диагноза
    if (error_code = 0) then
    begin
        node = null;
        select content
            from mds_xml_all_nodes(:source)
            where name = ''DiagnosisInfo''
                and path = ''MainDiagnosis/MainDiagnosis/''
            into node;

        subnode = null;
        if (error_code = 0)
            then select val, error_code, error_text
                    from mds_xml_get_value(:node
                                            , ''MkbCode''
                                            , ''node'', :OPTIONAL)
                    into subnode, error_code, error_text;

        if (error_code = 0 and coalesce(subnode, '''') <> '''') then
        begin
            select code
                from mds_nterm_coding_parse(:subnode)
                into source_diag_code;

            select first 1 dgcode from diagnosis
                where mkbcode = trim(:source_diag_code)
                into dgcode;

            if (dgcode is null) then
            begin
                execute procedure mds_nmq_log_add(''warning'', 1
                                                    , ''Не удалось найти диагноз по точному соответствию кода.''
                                                    , trim(:source_diag_code)
                                                    , :context, null, :referral_journal_id);
                select first 1 dgcode
                    from diagnosis
                     where mkbcode = trim(:source_diag_code) || ''.0''
                     into dgcode;
            end


            if (dgcode is null) then
            begin
                error_code = 1;
                error_text = ''Не удалось найти диагноз'';
            end
        end

        if (error_code = 0) then
        begin
            select left(val, 4096), error_code, error_text
                    from mds_xml_get_value(:node
                                            , ''Comment''
                                            , ''str'', :OPTIONAL)
                    into diagtext, error_code, error_text;
        end

        if (error_code > 0) then
        begin
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , left(''Код диагноза (MainDiagnosis/MainDiagnosis/DiagnosisInfo/MkbCode):'' || coalesce(:source_diag_code, ''null'') || :end_l
                                                        || ''Комментарий диагноза (MainDiagnosis/MainDiagnosis/DiagnosisInfo/Comment):'' || coalesce(:diagtext, ''null'')
                                                        , 4096)
                                                , :context, :msgid, :referral_journal_id);
        end
    end

    -- -- Узел с данными доктора
    if (error_code = 0) then
    begin
        select val, error_code, error_text
                from mds_xml_get_value(:source
                                        , ''Doctors''
                                        , ''node'', :OPTIONAL)
                into node, error_code, error_text;

        -- -- -- персональные данные доктора
        if (error_code = 0)
            then select val, error_code, error_text
                    from mds_xml_get_value(:node
                                            , ''Person''
                                            , ''node'', :OPTIONAL)
                    into subnode, error_code, error_text;


        if (error_code = 0)
            then select coalesce(familyname, '''')
                        , coalesce(givenname, '''')
                        , coalesce(middlename, '''')
                        , error_code, error_text
                    from mds_nmq_dec_Person(:subnode)
                    into :source_doctor_familyname, :source_doctor_givenname, :source_doctor_middlename
                        , error_code, error_text;

        if (error_code > 0) then
        begin
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , left(''ФИО доктора:'' || coalesce(:source_doctor_familyname, ''null'')
                                                                        || trim(coalesce('' '' || :source_doctor_givenname, ''null''))
                                                                        || trim(coalesce('' '' || :source_doctor_middlename, ''null''))
                                                        , 4096)
                                                , :context, :msgid, :referral_journal_id);
        end
    end
    -- ##############################


    -- #########################
    -- ## Данные о целевой МО ##
    -- ##  (Referral/Target)  ##
    -- #########################
    if (error_code = 0)then
    begin
        select val, error_code, error_text
                from mds_xml_get_value(:referral_node, ''Target'', ''node'', :OPTIONAL, :SECOND_NODE)
                into target, error_code, error_text;

        -- -- Узел с данными ЛПУ
        if (error_code > 0) then
        begin
            execute procedure mds_nmq_log_add(''error'', :error_code, :error_text, null
                                                , :context, :msgid, :referral_journal_id);
        end
        else
        begin
            select content
                from mds_xml_all_nodes(:target)
                where coalesce(path, '''') = '''' and name = ''Lpu''
                into node;

            select
                    code
                    , error_code
                    , left(error_text || '' (oid='' || coalesce(oid, ''null'') || coalesce('', code ='' || code, '''') || '')''
                            , 1024)
                from mds_nterm_coding_parse(:node, :DEEP_PARSE)
                into target_mo_code, error_code, error_text;

            if (error_code > 0) then
            begin
                execute procedure mds_nmq_log_add(''error'', :error_code, :error_text, null
                                                    , :context, :msgid, :referral_journal_id);
            end
            else
            begin
                select max(cashid), count(jid)
                    from cashref
                    where rir_guid = :target_mo_code
                    into tocashid, cnt;

                if (cnt > 1) then
                begin
                    error_code = 2;
                    error_text = ''В справочнике МИС найдено более одного подразделение с одинаковым кодом'';
                    execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                    , ''cashref.rir_guid = '' || coalesce(:target_mo_code, ''null'')
                                                    , :context, :msgid, :referral_journal_id);

                end
                else if (tocashid is not null) then
                begin
                    select jid from cashref where cashid = :tocashid into tojid;
                end
                else
                begin
                    -- Для случаев, когда в одном филиале несколько подразделений,
                    -- можно создать триггер на clreferrals, который будет обновлять tocashid
                    -- на основе всех необходимых для этого данных по направлению
                    select first 1 cashid from cashref where filial = :filial order by cashid asc into tocashid;
                end
            end

            if (error_code = 0) then
            begin
                if (tojid is null) then
                begin
                    select max(jid), count(jid)
                        from jpersons
                        where rir_guid = :target_mo_code
                        into tojid, cnt;

                    if (cnt > 1) then
                    begin
                        error_code = 3;
                        error_text = ''В справочнике МИС найдено более одного МО с одинаковым кодом'';
                        execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                    , ''jpersons.rir_guid = '' || coalesce(:target_mo_code, ''null'')
                                                    , :context, :msgid, :referral_journal_id);

                    end
                    else if (tojid is null) then
                    begin
                        select jid from filials where filid = :filial into :tojid;
                    end
                end
            end
        end


    end
    -- #########################


    -- ###########################################
    -- ## Создание/обновление направления в МИС ##
    -- ###########################################
    if(error_code = 0) then
    begin
        comment = (select val from mds_nmq_get_setting(''clreferrals_comment_template''));
        comment = left(replace(comment, '':referral_comment:'', coalesce(ext_comment, '''')), 4096);
        comment = left(replace(comment, '':referral_reason:'', coalesce(ext_reason, '''')), 4096);
        comment = left(replace(comment, '':referral_priority:'', coalesce(ext_priority, '''')), 4096);
        comment = left(replace(comment, '':main_diagnosis_comment:'', diagtext), 4096);
        comment = left(replace(comment, '':source_doctor:'', coalesce(:source_doctor_familyname, '''')
                                                        || coalesce('' '' || :source_doctor_givenname, '''')
                                                        || coalesce('' '' || source_doctor_middlename, '''')), 4096);
        comment = left(replace(comment, '':planned_date:'', coalesce(:ei_source_planneddate, '''')), 4096);

        if (fromjid = dummy_mo_jid and comment not containing ''Направляющее МО'') then
        begin
            comment = left(coalesce(comment, '''') || :end_l
                            || ''Направляющее МО: ''
                            || trim((select
                                        ''['' || coalesce(left(code, 4) || ''..'' || right(code, 2), ''<пусто>'') || ''] ''
                                            || coalesce(nullif(alias, ''''), name)
                                            || coalesce('' ('' || head_mo || '')'', '''')
                                    from mds_nterm_mo_info
                                    where code = :source_mo_code))
                            , 4096);
        end

        -- Если не удалось найти направление в МИС
        if (refid is null) then
        begin

            -- АйТи Системс - добавление вида исследования в clreferrals
            refid = next value for refer_gen;
            insert into clreferrals
                    (refid, reftype, createdate, modifydate, uid, filial
                    , extrefid, extrefdate
                    , pcode, treatdate, treatcode
                    , dgcode, diagtext
                    , fromjid, dcode
                    , tojid, todepart, todcode
                    , tocashid, tofilial
                    , comment
                    , distribtype
                    , extmedical
                    , exportstate
                    , extdepnum
                    , goal
                    )
                values
                    (:refid, :reftype, ''now'', ''now'', :dcode, :filial
                    , :ext_id, :ext_date
                    , :pcode, :ei_source_referraloutdate, -1 -- treatcode, -1 - направление создано без сзязи с лечением
                    , :dgcode, :diagtext
                    , :fromjid, :dcode
                    , :tojid, :todepart, 0 -- todcode, 0 - без указания целевого врача
                    , :tocashid, :filial
                    , :comment
                    , 0 -- distribtype
                    , :FROM_EXTERNAL_LPU -- extmedical, 2 - из внешнего ЛПУ
                    , 1 -- Признак наличия направления во внешней системе для запрета его редактирования через интерфейс МИС
                    , :extdepnum
                    , :ext_profilemedservice_code
                    );
        end
        -- Если направление в МИС было найдено, то проверить, что оно соответствует обрабатываемому направлению
        else
        begin
            sub_error_text = '''';
            select clr.refid
                    , left(iif(clr.pcode is distinct from :pcode
                                , ''Пациент (УИП в направлении МИС: '' || coalesce(clr.pcode, ''null'')
                                    || ''; УИП, полученный в ходе обработки направления РЕГИЗ: '' || coalesce(:pcode, ''null'')
                                    || '')'' || :end_l
                                , '''')
                            || iif(clr.extrefdate is distinct from :ext_date
                                , ''Дата направления (в направлении МИС (extrefdate): '' || coalesce(clr.extrefdate, ''null'')
                                    || ''; в направлении РЕГИЗ (ReferralInfo/Date): '' || coalesce(:ext_date, ''null'')
                                    || '')'' || :end_l
                                , '''')
                            || iif(clr.fromjid is not null and :fromjid is not null
                                    and clr.fromjid is distinct from :fromjid
                                    -- если у направления стоит фиктивное направляющее МО,
                                    -- и при этом удалось найти идентификатор МО (то есть настроили связь)
                                    -- то не считать это ошибкой расхождений данных,
                                    -- а просто заменить фиктивную МО на реальную
                                    and clr.fromjid is distinct from :dummy_mo_jid
                                    , ''Направляющее МО (в направлении МИС (fromjid): '' || coalesce(clr.fromjid, ''null'')
                                    || ''; в направлении РЕГИЗ (Source/Lpu/Code): '' || coalesce(:source_mo_code, ''null'')
                                    || ''; найденное МО из справочника МИС (jid): '' || coalesce(:fromjid, ''null'')
                                    || '')'' || :end_l
                                    , '''')
                            , 1024)
                from clreferrals as clr
                where refid = :refid
                into refid, sub_error_text;
            if (coalesce(sub_error_text, '''') <> '''') then
            begin
                error_code = 6;
                error_text = ''Расхождения в данных между направлением в МИС и направлением РЕГИЗ'';
                execute procedure mds_nmq_log_add(''error'', :error_code, :error_text
                                                , coalesce(:sub_error_text, '''')
                                                , :context, :msgid, :referral_journal_id);

            end

            if (error_code = 0) then
            begin
                select error_code, error_text, detailed_info, changed_fields_number
                    from mds_nmq_update_referral_data(:refid
                                                        , :dgcode, :diagtext
                                                        , :fromjid
                                                        , :tojid, :filial, :tocashid, :todepart
                                                        , :comment)
                    into error_code, error_text, log_detailed_info, cnt;
                if (error_code = 0 and cnt > 0) then
                begin
                    execute procedure mds_nmq_log_add(''info'', 0
                                                    , ''Обновление направления МИС. Добавление недостающих сведений''
                                                    , :log_detailed_info
                                                    , :context, :msgid, :referral_journal_id);
                end
            end
        end
    end

    -- Обновление сведений об аннулировании направления,
    -- (только при наличии этих сведеений в РЕГИЗ.УО,
    -- чтобы не сбросить еще не отправленные в УО сведения об аннулировании из МИС)
    if (error_code = 0 and refid is not null and coalesce(refcanceltype, 0) > 0) then
    begin
        update clreferrals
            set refcancel = 1
                , refcanceltype = :refcanceltype
                , refcanceluid = :dcode
                , refcanceldate = :ei_cancellation_date
                , refcancelcomment = :ei_cancellation_reasoncomment
            where refid = :refid;
    end
    -- ###########################################


    -- #####################################
    -- ## Создание/обновление направления ##
    -- ##      в журнале направлений      ##
    -- #####################################
    -- Создание/обновление записи в журнале направлений

    select cancellation_status
        from mds_nmq_referral_journal
        where referral_journal_id = :referral_journal_id
        into old_cancellation_status;
    new_cancellation_status = iif(coalesce(:refcanceltype, 0) > 0, :CANCELLATION_SENT, :old_cancellation_status);

    update or insert into mds_nmq_referral_journal
            (referral_journal_id, refid, ext_id
            , ext_createdate, ext_date, ext_status_code, ext_status_name
            , ext_profile_med_service, ext_type, ext_reason, ext_comment
            , ext_source_lpu_code, ext_source_lpu
            , ext_source_doctor
            , node
            , cancellation_status
            , is_incoming
            )
        values
            (:referral_journal_id, :refid, :ext_id
            , :ext_createdate, :ext_date, :ext_status_code, :ext_status_name
            , :ext_profile_med_service, :ext_type, :ext_reason, :ext_comment
            , :source_mo_code, :ext_source_lpu
            , :source_doctor_familyname || '' '' || :source_doctor_givenname || '' '' || :source_doctor_middlename
            , :referral_node
            , :new_cancellation_status
            , 1
            );
    -- #####################################
    if (coalesce(msgid, '''') <> '''') then
    begin
        insert into mds_nmq_exchange_journal(exchange_journal_id
                                            , referral_journal_id, msgid, response_processed
                                            , status, info)
                values (next value for mds_nmq_exchange_journal_seq
                        , :referral_journal_id, :msgid, ''now''
                        , iif(:error_code > 0, 2, 0), left(:error_text, 1024));
    end

    suspend;
end',
'ITS_GET_PARAMS_FROM_PROTOCOLID   begin
	/*
	Получить данные параметра из истории болезни
	по коду
	*/
	FOR
		SELECT
			p.VALUEINT
			,p.VALUETEXT
	  		,p.VALUEREAL
	  		,p.VALUEDATE
		FROM
			paramsinfo p
		WHERE
			p.protocolid = :protocolid
			and p.codeparams = :CODEPARAMS
			and p.ver_no = 0
	into :VALUEINT,:VALUETEXT,:VALUEREAL,:VALUEDATE
	do begin
		suspend;
	end
end',
'ITS_GET_PARAMS_FROM_PR_TP        begin
	/*
	Получить данные параметра из истории болезни по
	TEMPLATECODE - Псевдоним
	*/
	FOR
	SELECT
		p.VALUEINT
		,p.VALUETEXT
		,p.VALUEREAL
		,p.VALUEDATE
    ,p.organid
	FROM
		paramsinfo p
	join groupsparams gp
		on gp.codeparams = p.codeparams
	WHERE
		p.protocolid = :protocolid
		and gp.TEMPLATECODE = :TEMPLATECODE
		and p.ver_no = 0
	into :VALUEINT,:VALUETEXT,:VALUEREAL,:VALUEDATE,:organid
	do begin
		suspend;
	end
end',
'ITS_GET_CODEPARAMS_FROM_GROUPID  BEGIN
    /*
    Получить все идентификаторы параметров которые находятся в одной группе
    */
    FOR
    SELECT
        grl.CODEPARAMS
    FROM GROUPSDICT grd
    JOIN GROUPSDICTLINKS grl
        ON grl.GROUPID = grd.GROUPID
    where
        grd.GROUPID = :GROUPID
    INTO :CODEPARAMS
    do BEGIN
        suspend;
    end
END',
'ITS_UNLOADING_DIRECTIONS_0       DECLARE dispa type of column PARAMSINFO.valueint;
DECLARE TREATDATE type of column TREAT.TREATDATE;
DECLARE DIC_ID_DN_REKVINT2 type of column DICINFO.REKVINT2;
DECLARE eis_diag_id_diagnosis type of column mds_eis_diagnosis.id_diagnosis;
DECLARE id_CLREFERRALS type of column CLREFERRALS.REFID;
BEGIN
    /*
    Назвать отчет "Выгрузка направлений файл *_D.DBF"

    select * from ITS_UNLOADING_DIRECTIONS_0([bdate],[fdate])
    */
    FOR
    SELECT DISTINCT
        eis.dispa
        ,eis.TREATDATE
        ,eis.DIC_ID_DN_REKVINT2
        ,eis.EIS_DIAG_ID_DIAGNOSIS
        ,eis.id_CLREFERRALS as id_CLREFERRALS
        ,eis.SERV_ID
        ,cast(1 as numeric(3,0)) as ID_IN_CASE
        ,eis.D_NUMBER
        ,eis.DATE_ISSUE
        ,eis.DATE_PLANG
        ,eis.ID_LPU_F
        ,eis.ID_LPU_T
        ,eis.ID_D_TYPE
        ,eis.ID_D_GROUP
        ,eis.ID_PRVS
        ,eis.ID_OB_TYPE
        ,eis.ID_PRMP
        ,eis.ID_B_PROF
        ,eis.ID_DN
        ,eis.ID_GOAL
        ,eis.ID_DIAGNOS
        ,eis.ID_LPU_RF
        ,eis.ID_LPU_TO
        ,eis.ID_NMKL
        ,eis.ID_DOC
        ,eis.IDDOCPRVS
        ,eis.ERROR
        ---
        ,eis.ID_C_ZAB
        ,eis.TREATCODE
    FROM
    (
        /*
        Если в протоколе "Осмотр онколога[10000370]", значение параметра Диспансерный учет (10019211)  = "Постановка на учет" или "Находится на учете", то в рамках одного случая формируем еще одну строку
        p_QRESULT.valueint in (
            10046482, -- постановка на учет
            10046483 -- Находится на учете
        )
        */
        SELECT
            p_QRESULT.valueint as dispa
            , t.TREATDATE as TREATDATE
            , DIC_ID_DN.REKVINT2 as DIC_ID_DN_REKVINT2
            , eis_diag.id_diagnosis as EIS_DIAG_ID_DIAGNOSIS
            , clr.REFID as id_CLREFERRALS
            , cast(
                    iif(
                        diag_main.mkbcode containing ''C''
                        , left(
                            t.orderno, --|| right(COALESCE(p_kt.organid, p_mrt.organid), 2),
                            11
                        )
                        , t.orderno
                    ) AS numeric(11, 0)
                ) AS SERV_ID
            , cast(COALESCE(left(clr.extrefid, 20), clr.refid) AS CHAR(20)) AS D_NUMBER
            , cast(COALESCE(clr.extrefdate, clr.treatdate) AS DATE) AS DATE_ISSUE
            , cast(NULL AS DATE) AS DATE_PLANG
            , cast(COALESCE(rpv.propvalueint, 9248) AS numeric (11, 0)) AS ID_LPU_F
            , cast(9248 AS numeric (11, 0)) AS ID_LPU_T
            , cast(19 AS numeric(11, 0)) AS ID_D_TYPE
            , cast(14 AS numeric(11, 0)) AS ID_D_GROUP
            , cast(NULL AS numeric(11, 0)) AS ID_PRVS
            , cast(NULL AS numeric(11, 0)) AS ID_OB_TYPE
            , cast(NULL AS numeric(11, 0)) AS ID_PRMP
            , cast(NULL AS numeric(11, 0)) AS ID_B_PROF
            , cast(NULL AS numeric(11, 0)) AS ID_DN
            , cast(null AS numeric(11, 0)) AS ID_GOAL
            , cast(null AS numeric(11, 0)) AS ID_DIAGNOS
            , cast(NULL AS numeric(11, 0)) AS ID_LPU_RF
            , cast(NULL AS numeric(11, 0)) AS ID_LPU_TO
            , cast(NULL AS numeric(11, 0)) AS ID_NMKL
            , cast('''' AS CHAR(20)) AS ID_DOC
            , cast(NULL AS numeric(11, 0)) AS IDDOCPRVS
            , cast(NULL AS CHAR(200)) AS ERROR
            ---- info ----
            , cl.fullname
            , t.TREATCODE
            , cast (dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
        FROM treat t
            LEFT JOIN departments dp
                ON t.depnum = dp.depnum
            JOIN clients cl
                ON cl.pcode = t.pcode
            LEFT JOIN cl_get_profileinfo (t.pcode) AS c
                ON 1 = 1
            LEFT JOIN getage(c.bdate, t.treatdate) AS ga
                ON 1 = 1
            JOIN clhistnum ch
                ON ch.histid =  t.histid
            JOIN jpagreement jpa
                ON t.jid = jpa.agrid AND jpa.agrtype = 1 -- омс
            lefT JOIN treatplace tp
                ON t.treatcode = tp.treatcode
                AND tp.placeid = 10000370 -- протокол `Осмотр онколога`
                AND tp.pstate = 1  -- Должен быть подписан
            -- Получаем данные о Диспанцерном учете
            lefT JOIN paramsinfo p_QRESULT
                ON p_QRESULT.protocolid = tp.protocolid
                AND p_QRESULT.codeparams = 10019211  -- Диспансерный учет(10019211)
                AND p_QRESULT.ver_no = 0
            lefT JOIN dicinfo DIC_ID_DN
                ON  DIC_ID_DN.refid = 10013810 -- Диспансерный учет(refid = 10013810)
                AND DIC_ID_DN.dicid = p_QRESULT.valueint
            -- Для ID_PRMP и  ID_PRMP_C
            JOIN OMSDET oms
                ON oms.TREATCODE = t.treatcode
            JOIN dicinfo dic_oms
                ON dic_oms.REFID = -10005
                AND dic_oms.DICID = oms.IDPR
            -------------------------------------------------------------
            -- Улуга из приема
            LEFT JOIN TREATSCH tsh
                ON tsh.TREATCODE = t.TREATCODE
            -------------------------------------------------------------
            -- Для IDVIDVME
            LEFT JOIN RECPROPERTIES_SEARCH (
                    tsh.SCHID, -- id услуги
                    31 -- (услуга) RECTYPES.RECTYPE
                ) rs on rs.RECPROPID = 10000004 -- Идентификатор IDVIDVME в ЕИС ОМС
            -------------------------------------------------------------
            -- Для PROFILE
            JOIN schlinks sl
                    ON sl.schid = tsh.schid
                    AND sl.matcode = -1
                    AND sl.itindex <> -1
                    AND sl.fdate <= t.treatdate
                    AND sl.ldate >= t.treatdate
            JOIN wschema ws_oms
                ON ws_oms.SCHID = sl.WRKID
            -------------------------------------------------------------
            -- Для ID_SP_PAY
            JOIN dicinfo dic_pay
                ON dic_pay.REFID = -10004
                and dic_pay.dicid = ws_oms.PAYTYPE_V010
            -------------------------------------------------------------
            LEFT JOIN doctdepartlinks ddl
                ON ddl.dcode = t.dcode AND ddl.depnum = t.depnum AND ddl.idmsp = t.idmsp -- код врача в приеме
            LEFT JOIN dicinfo dvmp
                ON dvmp.refid = -10002 AND dvmp.dicid = ddl.medcaretype -- ID_VMP  справочник V008 Классификатор видов МП [-10002])
            LEFT JOIN dicinfo dic
                ON dic.refid = -10000 AND dic.dicid = ddl.idmsp -- код специальности в приеме
            -- Допускаем возможно что у приема нет направления, это нужно для тз http://support.i-t-systems.ru/otrs/index.pl?Action=AgentTicketZoom;TicketID=36818#248534
            LEFT JOIN clreferrals clr
                ON clr.rtreatcode = t.treatcode
            LEFT JOIN jpersons jp
                ON clr.fromjid = jp.jid -- направлен откуда
            ----------------------------------------------
            LEFT JOIN recpropvalues rpv
                ON rpv.recpropid = 10000015
                AND rpv.recid = jp.jid -- ID_LPU_F - доп параметр в справочнике юр лиц
            LEFT JOIN recpropvalues rpv3
                ON rpv3.recpropid = 10000021
                AND rpv3.recid = jp.jid --признак ЛПУ "не включать в выгрузку
            LEFT JOIN dicinfo dic_prvs
                ON ddl.idmsp = dic_prvs.dicid
                AND dic_prvs.refid = -10000 -- специальность в отделении = id_prvs
            LEFT JOIN diagclients dc_main
                ON dc_main.objcode = t.treatcode
                AND dc_main.objtype = 1
                AND dc_main.dgtypecode = 1 -- основной диагноз приема
            LEFT JOIN diagnosis diag_main
                ON dc_main.dgcode = diag_main.dgcode
            LEFT JOIN dicinfo dic_c_zab
                ON dc_main.DIAGDESCTYPE = dic_c_zab.dicid
                AND dic_c_zab.refid = -8
            LEFT JOIN mds_eis_diagnosis eis_diag
                ON eis_diag.diagnosis_code = diag_main.mkbcode
        WHERE
            t.treatdate BETWEEN :bdate AND :fdate
            AND t.depnum = 10000137 -- Центр амбулаторной онкологической помощи
            AND COALESCE(rpv3.propvalueint, 0) = 0 --признак ЛПУ "не включать в выгрузку"
    ) as eis
    WHERE COALESCE(
            (
                SELECT FIRST 1 char_length(esl.error)
                FROM its_eis_service_load esl
                WHERE esl.serv_id = eis.serv_id
                ORDER BY esl.eslid DESC
            ), 0
        ) = 0 AND COALESCE(
            (
                SELECT FIRST 1 esl.send
                FROM its_eis_service_load esl
                WHERE esl.serv_id = eis.serv_id
                ORDER BY esl.eslid DESC
            ), ''F''
        ) <> ''T''
    INTO :dispa,:TREATDATE,:DIC_ID_DN_REKVINT2,:eis_diag_id_diagnosis,:id_CLREFERRALS,:SERV_ID,:ID_IN_CASE,:D_NUMBER,:DATE_ISSUE,:DATE_PLANG,:ID_LPU_F,:ID_LPU_T,:ID_D_TYPE,:ID_D_GROUP,:ID_PRVS,:ID_OB_TYPE,:ID_PRMP,:ID_B_PROF,:ID_DN,:ID_GOAL,:ID_DIAGNOS,:ID_LPU_RF,:ID_LPU_TO,:ID_NMKL,:ID_DOC,:IDDOCPRVS,:ERROR,:ID_C_ZAB,:TREATCODE
    do BEGIN
        -- Елсли нет направления
        IF(id_CLREFERRALS is null)THEN
        BEGIN
            DATE_ISSUE=TREATDATE;
            ID_LPU_F=9248;
        END
        suspend;
        IF(
            dispa=10046482 -- постановка на учет
            or
            dispa=10046483 -- Находится на учете) THEN
        ) THEN
        BEGIN
            ID_IN_CASE=null;
            DATE_ISSUE=TREATDATE;
            ID_LPU_F=9248;
            ID_D_TYPE=16;
            ID_D_GROUP=8;
            ID_DN=DIC_ID_DN_REKVINT2;
            ID_DIAGNOS=eis_diag_id_diagnosis;
            suspend;
        end
        -- Если в файле со случаями у этого SERVID . ID_C_ZAB = 2831, то добавляем к этому SERVID еще одну строку (см приложение)
        IF(ID_C_ZAB=2831)THEN
        BEGIN
            D_NUMBER=null;
            DATE_PLANG=null;
            ID_LPU_F=null;
            ID_LPU_T=null;
            ID_PRVS=null;
            ID_OB_TYPE=null;
            ID_PRMP=null;
            ID_B_PROF=null;
            ID_DN=null;
            ID_GOAL=null;
            ID_DIAGNOS=null;
            ID_LPU_RF=null;
            ID_LPU_TO=null;
            ID_NMKL=null;
            ERROR=null;
            ID_IN_CASE=null;
            DATE_ISSUE=(select TREATDATE  from treat where treatcode=:treatcode);
            ID_D_TYPE=38;
            ID_D_GROUP=20;
            suspend;
        END
    END
END',
'ITS_UNLOADING_DIRECTIONS_1       begin
    /*
    Назвать отчет "Выгрузка направлений"

    select
        SERV_ID
        ,ID_IN_CASE
        ,D_NUMBER
        ,DATE_ISSUE
        ,DATE_PLANG
        ,ID_LPU_F
        ,ID_LPU_T
        ,ID_D_TYPE
        ,ID_D_GROUP
        ,ID_PRVS
        ,ID_OB_TYPE
        ,ID_PRMP
        ,ID_B_PROF
        ,ID_DN
        ,ID_GOAL
        ,ID_DIAGNOS
        ,ID_LPU_RF
        ,ID_LPU_TO
        ,ID_C_ZAB
        ,ID_NMKL
        ,ID_DOC
        ,IDDOCPRVS
        ,ERROR
    from ITS_UNLOADING_DIRECTIONS_1([bdate],[fdate])
    */
    FOR
    SELECT DISTINCT
        eis.SERV_ID
        ,eis.ID_IN_CASE
        ,eis.D_NUMBER
        ,eis.DATE_ISSUE
        ,eis.DATE_PLANG
        ,eis.ID_LPU_F
        ,eis.ID_LPU_T
        ,eis.ID_D_TYPE
        ,eis.ID_D_GROUP
        ,eis.ID_PRVS
        ,eis.ID_OB_TYPE
        ,eis.ID_PRMP
        ,eis.ID_B_PROF
        ,eis.ID_DN
        ,eis.ID_GOAL
        ,eis.ID_DIAGNOS
        ,eis.ID_LPU_RF
        ,eis.ID_LPU_TO
        ,eis.ID_NMKL
        ,eis.ID_C_ZAB
        ,eis.ERROR
        ----
        ,eis.treatcode
    FROM
    (
        select
            cast(iif(diag.mkbcode containing ''C'', left(t.orderno || right(coalesce(p_kt.organid, p_mrt.organid),2),11),t.orderno) as numeric(11,0)) SERV_ID,
            cast(1 as numeric(3,0))  ID_IN_CASE,
            cast(coalesce(left(clr.extrefid,20),clr.refid) as char(20)) D_NUMBER,
            cast(coalesce(clr.extrefdate,clr.treatdate) as date)                         DATE_ISSUE,
            cast(null as date)                                   DATE_PLANG,
            cast(coalesce(rpv.propvalueint,9248) as numeric (11,0)) ID_LPU_F,
            cast(9248 as numeric (11,0)) ID_LPU_T,
            cast(dic_goal.rekvint3 as numeric(11,0)) ID_D_TYPE,
            cast(dic_goal.rekvint2 as numeric(11,0)) ID_D_GROUP,
            cast(null as numeric(11,0)) ID_PRVS,
            cast(null as numeric(11,0)) ID_OB_TYPE,
            cast(null as numeric(11,0)) ID_PRMP,
            cast(null as numeric(11,0)) ID_B_PROF,
            cast(null as numeric(11,0)) ID_DN,
            cast(
                -- iif(diag_main.mkbcode containing ''C'', dic_c_zab.rekvint3, 20)
                d_p_purpose0appeals.REKVINT1
                as numeric(11,0)) ID_GOAL,
            cast(eis_diag.id_diagnosis as numeric(11,0)) ID_DIAGNOS,
            cast(null as numeric(11,0)) ID_LPU_RF,
            cast(null as numeric(11,0)) ID_LPU_TO,
            cast(null as numeric(11,0)) ID_NMKL,
            cast (dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB,
            cast(null as char(200)) ERROR,
            -----
            t.treatcode
        from treat t
        left join departments dp on t.depnum = dp.depnum
        join clients cl on cl.pcode = t.pcode
        left join cl_get_profileinfo (t.pcode) c on 1=1
        left join getage(c.bdate, t.treatdate) ga on 1=1
        join clhistnum ch on t.histid = ch.histid
        join jpagreement jpa on t.jid = jpa.agrid and jpa.agrtype = 1  -- омс
        left join treatplace tp
            on t.treatcode = tp.treatcode
            and tp.placeid in (
                -- Прошлые протоколы до 18.07.2022
                10000736,10000413, -- протоколы "Компьютерная томография (#131727)", "МРТ(#131727)"
                --Новые протоколы от 18.07.2022
                10000742,10000740 -- ОЛД МРТ, ОЛД РКТ
            )
            and tp.pstate = 1  -- Подписан
        left join ITS_GET_PARAMS_FROM_PR_TP(''studykt111'', tp.protocolid) p_kt on 1=1  -- параметр "Исследование"(Профиль исследования_КТ) из группы "Исследование_КТ"
        left join ITS_GET_PARAMS_FROM_PR_TP(''diagnoskt111'', tp.protocolid) p_kt_d on p_kt.organid = p_kt_d.organid  -- параметр "Диагноз"(Диагноз_КТ)  из группы "Исследование_КТ"
        left join ITS_GET_PARAMS_FROM_PR_TP(''studymrt111'', tp.protocolid) p_mrt on 1=1 -- параметр "Исследование"(Профиль исследования_МРТ) из группы "Исследование_КТ"
        left join ITS_GET_PARAMS_FROM_PR_TP(''diagnosmrt111'', tp.protocolid) p_mrt_d on p_mrt.organid = p_mrt_d.organid -- параметр "Диагноз"(Диагноз_МРТ)  из группы "Исследование_КТ"
        left join ITS_GET_PARAMS_FROM_PR_TP(''purpose0appeals'',tp.protocolid) p_purpose0appeals on 1=1 -- Цель обращения
        left join dicinfo d_p_purpose0appeals on d_p_purpose0appeals.refid = 10014297 and d_p_purpose0appeals.dicid = p_purpose0appeals.valueint -- Получаем код на основе Цили обращения
        join wschema w on w.schid = p_kt.valueint or w.schid = p_mrt.valueint -- параметр "Исследование"
        join schlinks sl on sl.schid = w.schid and sl.matcode = -1 and sl.itindex <> -1 and sl.fdate <= t.treatdate and sl.ldate >= t.treatdate
        join wschema w2 on sl.wrkid = w2.schid
        join diagnosis diag  on (diag.dgcode = p_kt_d.valueint and p_kt.valueint > 0) or (diag.dgcode = p_mrt_d.valueint and p_mrt.valueint > 0)  -- параметр "Диагноз"
        left join doctdepartlinks  ddl   on ddl.dcode = t.dcode and ddl.depnum = t.depnum and ddl.idmsp = t.idmsp   -- код врача в приеме
        left join dicinfo dvmp on dvmp.refid = -10002 and dvmp.dicid = ddl.medcaretype   -- ID_VMP  справочник V008 Классификатор видов МП [-10002])
        left join dicinfo dic on dic.refid = -10000 and dic.dicid = ddl.idmsp  -- код специальности в приеме
        join clreferrals clr on clr.rtreatcode = t.treatcode -- любой тип направления
        join dicinfo dic_goal on clr.goal = dic_goal.dicid and dic_goal.refid = 48 -- цель направления
        left join jpersons fj on fj.jid = clr.fromjid
        left join dicinfo dicr on dicr.refid = 777003 and dicr.rekvint1 = clr.reftype  -- ORDER -- порядок направления
        left join dicinfo dpay on dpay.refid = -10004 and dpay.rekvint4 = 1  -- ID_SP_PAY  -- по умолчанию
        left join jpersons jp on clr.fromjid = jp.jid  -- направлен откуда
        left join recpropvalues rpv on rpv.recpropid = 10000015 and rpv.recid = jp.jid -- ID_LPU_F - доп параметр в справочнике юр лиц
        left join recpropvalues rpv2 on rpv2.recpropid = 10000004 and rpv2.recid = w.schid -- IDVIDVME - доп. параметр в прейскуранте
        left join diagnosis dg on clr.dgcode = dg.dgcode  -- диагноз направления
        left join recpropvalues rpv3 on rpv3.recpropid = 10000021 and rpv3.recid = jp.jid --признак ЛПУ "не включать в выгрузку
        left join dicinfo dic_prvs on ddl.idmsp = dic_prvs.dicid and dic_prvs.refid = -10000  -- специальность в отделении = id_prvs
        left join diagclients dc_main on dc_main.objcode = t.treatcode and dc_main.objtype = 1 and dc_main.dgtypecode = 1 -- основной диагноз приема
        left join diagnosis diag_main on dc_main.dgcode = diag_main.dgcode
        left join dicinfo dic_c_zab on dc_main.DIAGDESCTYPE = dic_c_zab.dicid and dic_c_zab.refid = -8 -- характер основного заболевания
        left join mds_eis_diagnosis eis_diag on eis_diag.diagnosis_code = diag_main.mkbcode
        where
            t.treatdate between :bdate and :fdate
            and t.depnum = 10000123 -- отделение "Отдел лучевой диагностики"
            and coalesce(rpv3.propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
            and dic_goal.rekvint3 in (36,37)  -- цель направления только КТ/МРТ
    ) as eis
    where
        coalesce((select first 1 char_length(esl.error)from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
        and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id
        order by esl.eslid desc),''F'') <> ''T''
    into :SERV_ID,:ID_IN_CASE,:D_NUMBER,:DATE_ISSUE,:DATE_PLANG,:ID_LPU_F,:ID_LPU_T,:ID_D_TYPE,:ID_D_GROUP,:ID_PRVS,:ID_OB_TYPE,:ID_PRMP,:ID_B_PROF,:ID_DN,:ID_GOAL,:ID_DIAGNOS,:ID_LPU_RF,:ID_LPU_TO,:ID_NMKL,:ID_C_ZAB,:ERROR,:treatcode
    do begin
        ID_DOC=null;
        IDDOCPRVS=null;
        --
        suspend;
        -- Если в файле со случаями у этого SERVID   ID_C_ZAB = 2831, то добавляем к этому SERVID еще одну строку (см приложение)
        IF(ID_C_ZAB=2831)THEN
        BEGIN
            D_NUMBER=null;
            DATE_PLANG=null;
            ID_LPU_F=null;
            ID_LPU_T=null;
            ID_PRVS=null;
            ID_OB_TYPE=null;
            ID_PRMP=null;
            ID_B_PROF=null;
            ID_DN=null;
            ID_GOAL=null;
            ID_DIAGNOS=null;
            ID_LPU_RF=null;
            ID_LPU_TO=null;
            ID_NMKL=null;
            ERROR=null;
            ID_IN_CASE=null;
            DATE_ISSUE=(select TREATDATE  from treat where treatcode=:treatcode);
            ID_D_TYPE=38;
            ID_D_GROUP=20;
            suspend;
        END
    end
end',
'ITS_UNLOADING_DIRECTIONS_3       begin
    /* -----

select
    SERV_ID
    ,ID_OBJECT
    ,OBJ_VALUE
    ,ERROR
from ITS_UNLOADING_DIRECTIONS_3([bdate],[fdate])

    */
    FOR SELECT
        eis.SERV_ID
        ,eis.ID_OBJECT
        ,eis.OBJ_VALUE
        ,eis.ERROR
        ,eis.FULLNAME
        ,eis.TREATCODE
        ,eis.ID_C_ZAB
    FROM(
        WITH eis AS (
            --Назвать отчет "Выгрузка доп. данных по случаю файл *_ADD.DBF"
            SELECT
                cast(
                    iif(
                        diag.mkbcode containing ''C''
                        , left(
                            t.orderno, --|| right(COALESCE(p_kt.organid, p_mrt.organid), 2),
                            11
                        )
                        , t.orderno
                    ) AS numeric(11, 0)
                ) AS SERV_ID
                , cast(
                    --  если у этого диагноза в таблице DIAGNOSIS стоит ISONKO = 1, то передаем 29
                    iif(
                            -- если в протоколе "Осмотр онколога[10000370]" заполнен хотя бы один параметр из группы "Приказ ТФОМС. Информация по случаю_амб", то передаем 30
                            (
                            select
                                sum(iif((COALESCE(p.valueint,p.VALUEREAL, null) is not null or p.valuetext != '''')
                                , 1
                                , 0))
                            from paramsinfo p
                            where
                                p.protocolid = tp.protocolid
                                and p.codeparams in (select * from ITS_GET_CODEPARAMS_FROM_GROUPID(
                                    10001223 --Приказ ТФОМС. Информация по случаю_амб
                                    )
                                )
                            )>0,30,29)
                as numeric(4, 0)) as ID_OBJECT
                , cast(''1'' AS CHAR(10)) AS OBJ_VALUE
                , cast(null  AS CHAR(200)) AS ERROR
                ---- info ----
                , cl.fullname
                , t.TREATCODE
                , cast(dic_c_zab.rekvint2 AS numeric(11, 0)) AS ID_C_ZAB
            FROM treat t
                JOIN ACCIDENTDET acd on acd.ACDETID = t.ACDIDDET
                JOIN ACCIDENT ac on ac.ACDID = acd.ACDID
                LEFT JOIN departments dp
                    ON t.depnum = dp.depnum
                JOIN clients cl
                    ON cl.pcode = t.pcode
                JOIN clhistnum ch
                    ON ch.histid =  t.histid
                JOIN jpagreement jpa
                    ON t.jid = jpa.agrid AND jpa.agrtype = 1 -- омс
                LEFT JOIN treatplace tp
                    ON t.treatcode = tp.treatcode
                    AND tp.placeid = 10000370 -- протокол `Осмотр онколога`
                    AND tp.pstate = 1  -- Должен быть подписан
                -- Получаем данные о Диспанцерном учете
                LEFT JOIN paramsinfo p_QRESULT
                    ON p_QRESULT.protocolid = tp.protocolid
                    AND p_QRESULT.codeparams = 10019211  -- Диспансерный учет(10019211)
                    AND p_QRESULT.ver_no = 0
                LEFT JOIN dicinfo DIC_GOAL
                    ON  DIC_GOAL.refid = 10013810 -- Диспансерный учет(refid = 10013810)
                    AND DIC_GOAL.dicid = p_QRESULT.valueint
                -------------------------------------------------------------
                -- Получаем основной потвержденный диагноз
                LEFT JOIN diagclients dc
                    ON tp.treatcode = dc.objcode
                    AND dc.objtype IN (1, 101, 102, 103, 104)
                    and dc.DGTYPECODE = 1 -- Основной
                LEFT JOIN diagnosis diag
                    ON dc.dgcode = diag.dgcode
                -------------------------------------------------------------
                -- Допускаем возможно что у приема нет направления, это нужно для тз http://support.i-t-systems.ru/otrs/index.pl?Action=AgentTicketZoom;TicketID=36818#248534
                LEFT JOIN clreferrals clr
                    ON clr.rtreatcode = t.treatcode -- любой тип направления
                LEFT JOIN jpersons jp2
                    ON clr.fromjid = jp2.jid -- направлен откуда
                LEFT JOIN recpropvalues rpv3
                    ON rpv3.recpropid = 10000021 AND rpv3.recid = jp2.jid --признак ЛПУ "не включать в выгрузку
                -------------------------------------------------------------
                LEFT JOIN diagclients dc_main
                    ON dc_main.objcode = t.treatcode AND dc_main.objtype = 1 AND dc_main.dgtypecode = 1 -- основной диагноз приема
                LEFT JOIN diagnosis diag_main
                    ON dc_main.dgcode = diag_main.dgcode
                LEFT JOIN dicinfo dic_c_zab
                    ON dc_main.DIAGDESCTYPE = dic_c_zab.dicid AND dic_c_zab.refid = -8
            WHERE
                t.treatdate between :bdate and :fdate
                AND t.depnum = 10000137 -- Центр амбулаторной онкологической помощи
                AND COALESCE(rpv3.propvalueint, 0) = 0 --признак ЛПУ "не включать в выгрузку"
        )
        SELECT DISTINCT *
        FROM eis
        WHERE COALESCE(
                (
                    SELECT FIRST 1 CHAR_LENGTH(esl.error)
                    FROM its_eis_service_load esl
                    WHERE esl.serv_id = eis.serv_id
                    ORDER BY
                        esl.eslid DESC
                )
                , 0
            ) = 0
            AND COALESCE(
                (
                    SELECT FIRST 1 esl.send
                    FROM its_eis_service_load esl
                    WHERE esl.serv_id = eis.serv_id
                    ORDER BY esl.eslid DESC
                ), ''F''
            ) <> ''T''
    ) eis
    into :SERV_ID,:ID_OBJECT,:OBJ_VALUE,:ERROR,:FULLNAME,:TREATCODE,:ID_C_ZAB
    do begin
        suspend;
        /*
        для тех SERV_ID, у кот. в основном файле со случаями ID_C_ZAB = 2831, добавляем еще одну строку с тем же SERV_ID,  ID_OBJECT =  6, OBJ_VALUE = 1
        */
        IF(ID_C_ZAB = 2831)THEN
        BEGIN
            ID_OBJECT =  6;
            OBJ_VALUE = 1;
            suspend;
        END
    end
end',
'ITS_I25_VALUETEXT_TEMPLATE       begin
  select p.valuetext
from (
      select first 1 t.protocolid, gp.codeparams
      from treatplace t0
      join groupsparams gp0 on gp0.codeparams = :codeparams
      join treatplace t on t0.pcode = t.pcode and t.protocolid <> t0.protocolid and coalesce(t.treatdelete,0) = 0 and ((t.treatdate = t0.treatdate and :oneday = 1) or :oneday = 0)
      join workplacedoclinks wdl on wdl.placeid = t.placeid
      join groupsdictlinks gdl on wdl.groupid = gdl.groupid --and gdl.codeparams = :codeparams
      join groupsparams gp on gdl.codeparams = gp.codeparams and iif(gp.templatecode <> '''', gp.templatecode = gp0.templatecode, gp.codeparams = :codeparams)
      where t0.protocolid = :protocolid
      order by t.treattime desc
      ) t
join paramsinfo p on p.protocolid = t.protocolid and p.ver_no = 0 and p.codeparams = t.codeparams and p.valuetext <> ''''
into :valuetext;
suspend;
end',
'ITS_REGEVENT_KOMFORTEL           declare filial type of column operlog.filial;
declare eventdate type of column operlog.eventdate;
declare uid type of column operlog.uid;
declare uname type of column operlog.uname;
declare rstr2 type of column operlog.rstr2;
declare prior_role type of column rdb$roles.rdb$role_name;
declare rmemo type of column operlog.rmemo;
declare event_count ticode;
declare frompid type of column operlog.frompid;
declare eventtext ttext1024;
declare aname type of column operlogref.aname;
declare msgsend type of column operlogref.msgsend;
declare eventtypeid type of column operlogref.eventtypeid;
declare currlgid type of column operlog.lgid;
declare sqlt TTEXT1024;
declare handler type of column OPERLOG_HANDLERREF.HANDLER;
begin
  /*
  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  Это модефецированная копию стандарной процедуры REGEVENT специально созданая для KOMFORTEL_GET_FILE

  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  */
  if (coalesce(eventtype, 0) <> 0) then
  begin
    -- системное событие отключено
    if ((select avisible from operlogref where eventtype = :eventtype) is distinct from 1) then
      exit;

    -- база данных в режиме резервной копии
    if ((select repl$state from repl$getstate) = 2) then
      exit;

    -- база данных в режиме зеркалирования
    if (upper(rdb$get_context(''SYSTEM'', ''REPLICA'')) = ''TRUE'') then
      exit;

    when gdscode ctx_var_not_found do
    begin
      -- контекстная переменная REPLICA не найдена, запись события в обычном режиме
    end

  end else
    execute procedure raise_exception(''Не указан тип события системного журнала'');

  event_count = 0;
  eventdate = current_date;

  prior_role = rdb$get_context(''USER_TRANSACTION'', ''ROLE'');

  select current_uid, current_uname, current_filial, coalesce(:cashid, current_cashid), current_pid
  from s_session_info
  into uid, uname, filial, cashid, frompid;

  if ((rstr1 is null) and (pcode is not null)) then
    select c.fullname from clients c where c.pcode = :pcode into rstr1;

  rstr2 = coalesce(rdb$get_context(''USER_SESSION'', ''COMP_NAME''), rdb$get_context(''SYSTEM'', ''CLIENT_ADDRESS''));

  rdb$set_context(''USER_TRANSACTION'', ''ROLE'', ''USER'');

  while ((event_count = 0) or (eventmemo > '''')) do
  begin
    rmemo = substring(eventmemo from 1 for 4096);

    currlgid = next value for operlog_gen;

    insert into operlog
      (lgid, eventdate, eventtype, uid, uname, pcode,
      rstr1, rstr2, rint1, rint2, rint3, rint4,
      rdate1, rdate2, rdate3, rsum1, rsum2,
      dcode, eventtime, rmemo, moduleid, filial,
      jid, agrid, lstid, cashid, rint5, frompid)
    values
      (:currlgid, :eventdate, :eventtype, :uid, :uname, :pcode,
      :rstr1, :rstr2, :rint1, :rint2, :rint3, :rint4,
      :rdate1, :rdate2, :rdate3, :rsum1, :rsum2,
      :dcode, current_timestamp, :rmemo, :moduleid, :filial,
      :jid, :agrid, :lstid, :cashid, :rint5, :frompid);

    --рассылка сообщений
    select msgsend, aname, eventtypeid from operlogref  where eventtype = :eventtype
    into :msgsend, :aname, :eventtypeid;

    if ( msgsend = 1 ) then
    begin
      eventtext = '''';

      if (aname > '''') then begin
        eventtext = aname;
      end

      if (jpcodeid > 0) then begin
        eventtext = eventtext || ''. Пациент: '' || coalesce((select fullname from clients where pcode = :jpcodeid), '''');
      end

      if (jid > 0) then begin
        eventtext = eventtext || ''. Юр. лицо: '' || coalesce((select jname from jpersons where jid = :jid), '''');
      end

      if (mr > '''') then begin
        eventtext = eventtext || ''. '' || mr;
      end

      execute procedure event_send(:eventtypeid, :eventtext, :dcode, :uid, :currlgid);
    end

    eventmemo = substring(eventmemo from 4097);
    event_count = event_count + 1;

    for select oh.handler
      from operlog_link ol
      inner join operlog_handlerref oh on
        oh.oplhandlerid = ol.olrhandlerid and coalesce(oh.handler, '''') <> ''''
      where ol.eventtype = :eventtype and ol.eventtype<>602
    into :handler
    do
    begin
      sqlt = ''execute procedure '' || handler || ''('' || :currlgid || '')'';

      execute statement sqlt;
    end

  end



  rdb$set_context(''USER_TRANSACTION'', ''ROLE'', :prior_role);

  when gdscode read_only_database do
  begin
    rdb$set_context(''USER_TRANSACTION'', ''ROLE'', :prior_role);
  end

  when any do
  begin
    rdb$set_context(''USER_TRANSACTION'', ''ROLE'', :prior_role);
    exception;
  end
end',
'ITS_UNLOADING_DIRECTIONS_2       BEGIN
    /*
    ГО -> ЕИС ОНКО -> Выгрузка доп.данных по случаю файл *_ADD.DBF

    Файл ADD. Если в основном файле со случаями ID_C_ZAB = 2831, то у этого SERVID формируем строку с ID_OBJECT = 6, OBJ_VALUE = 1

    select SERV_ID,ID_OBJECT,OBJ_VALUE,ERROR from ITS_UNLOADING_DIRECTIONS_2(''17.08.2022'',''17.08.2022'')
    */

    FOR SELECT
        eis.SERV_ID
        ,eis.ID_OBJECT
        ,eis.OBJ_VALUE
        ,eis.ERROR
        ,eis.ID_C_ZAB
        ,eis.VALUEDATE
    FROM
    (
        select
            cast(sd.sdid as Numeric (11,0)) as SERV_ID 	--Идентификатор случая лечения в основном файле импорта услуг
            ,cast(30 as Numeric (4,0)) as ID_OBJECT
            ,cast(1 as Character (10)) as OBJ_VALUE
            ,cast('''' as char(200)) ERROR
            -- info --
            ,cast(dic_c_zab.rekvint2 as numeric(11,0)) as ID_C_ZAB
            ,p2.valuedate as VALUEDATE
        from stat_direction sd
        join stat_jornal sj on sd.sdid = sj.dirid
        join clients cl on cl.pcode = sd.pcode
        join clhistnum ch on sd.histid = ch.histid
        join jpagreement jpa on sd.agrid = jpa.agrid and jpa.agrtype = 1  -- омс
        join accident ac on sd.acdid = ac.acdid
        join diagnosis diag on ac.finaldiag = diag.dgcode
        join diagclients dc on ac.acdid = dc.acdid and dc.objtype = 104
        -- Для ID_C_ZAB
        left join dicinfo dic_c_zab on dc.DIAGDESCTYPE = dic_c_zab.dicid and dic_c_zab.refid = -8 -- характер заболевания
        --
        left join clreferrals clr_in on clr_in.refid = sd.refid -- направление на госпитализацию
        left join jpersons jp on clr_in.fromjid = jp.jid  -- направлен откуда
        left join recpropvalues rpv3 on rpv3.recpropid = 10000021 and rpv3.recid = jp.jid --признак ЛПУ "не включать в выгрузку
        join treatplace tp on sd.sdid = tp.dirid and tp.pstate = 1 and tp.placeid = 10000713
        join paramsinfo p on tp.protocolid = p.protocolid and p.codeparams = 10018653 and p.ver_no = 0 -- ксг
        join wschema w on p.valueint = w.schid
        /*
            У случая должен быть (заполен протокол "ДС. Протокол оперативного вмешательства") или у него ID_C_ZAB=2831
            Если у случая заполнен протокол и ID_C_ZAB=2831 то формируем доп строку.
        */
        -- "ДС. Протокол оперативного вмешательства" (ID 10000697)
        LEFT join treatplace tp2 on sd.sdid = tp2.dirid and tp2.placeid = 10000697 and tp2.pstate = 1
        -- и заполнен Дата операции (10018051)
        LEFT join paramsinfo p2 on p2.protocolid=tp2.protocolid and p2.codeparams=10018051 and p2.valuedate is not null and p2.ver_no=0
        ------------------------------------------------------------------------------------------------
        where
            cast(sd.planend as date) between :bdate and :fdate
            and coalesce(sd.enddate_not_present,0) = 0
            and coalesce(rpv3.propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
            and sd.dstatus in (4,7) -- пациент выписан
            and sd.stat_dep = 10000002 -- отдеелние "Дневной стационар" (это онкология)
    ) as EIS
    WHERE
        COALESCE(
            (SELECT FIRST 1 CHAR_LENGTH(ESL.ERROR)
            FROM ITS_EIS_SERVICE_LOAD ESL
            WHERE ESL.SERV_ID = EIS.SERV_ID
            ORDER BY ESL.ESLID DESC), 0) = 0
        AND COALESCE(
            (SELECT FIRST 1 ESL.SEND
            FROM ITS_EIS_SERVICE_LOAD ESL
            WHERE ESL.SERV_ID = EIS.SERV_ID
            ORDER BY ESL.ESLID DESC),''F'') <> ''T''
    into :SERV_ID,:ID_OBJECT,:OBJ_VALUE,:ERROR,:ID_C_ZAB,:VALUEDATE
    do BEGIN
        IF(valuedate is not null)THEN
        BEGIN
            suspend;
        END
        IF(ID_C_ZAB=2831)THEN
        BEGIN
            ID_OBJECT = 6;
            OBJ_VALUE = 1;
            suspend;
        END
    end
end',
'ITS_UNLOADING_DIRECTIONS_4       BEGIN
    /*
    ГО -> ЕИС ОНКО -> Выгрузка направлений файл *_D.DBF


    select
        SERV_ID,ID_IN_CASE,D_NUMBER,DATE_ISSUE,DATE_PLANG,ID_LPU_F,ID_LPU_T,ID_D_TYPE,ID_D_GROUP,ID_PRVS,ID_OB_TYPE,ID_PRMP,ID_B_PROF,ID_DN,ID_GOAL,ID_DIAGNOS,ID_LPU_RF,ID_LPU_TO,ID_NMKL,ERROR,ID_C_ZAB
    from ITS_UNLOADING_DIRECTIONS_4([bdate],[fdate])
    */
    FOR SELECT DISTINCT
        eis.SERV_ID
        ,eis.ID_IN_CASE
        ,eis.D_NUMBER
        ,eis.DATE_ISSUE
        ,eis.DATE_PLANG
        ,eis.ID_LPU_F
        ,eis.ID_LPU_T
        ,eis.ID_D_TYPE
        ,eis.ID_D_GROUP
        ,eis.ID_PRVS
        ,eis.ID_OB_TYPE
        ,eis.ID_PRMP
        ,eis.ID_B_PROF
        ,eis.ID_DN
        ,eis.ID_GOAL
        ,eis.ID_DIAGNOS
        ,eis.ID_LPU_RF
        ,eis.ID_LPU_TO
        ,eis.ID_NMKL
        ,eis.ERROR
        ,eis.ID_C_ZAB
    FROM (
        select distinct
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

        from stat_direction sd
        join stat_jornal sj on sd.sdid = sj.dirid
        left join departments dp on sj.depnum_last = dp.depnum
        join clients cl on cl.pcode = sd.pcode
        left join cl_get_profileinfo (sd.pcode) c on 1=1
        left join getage(c.bdate, cast(sd.planstart as date)) ga on 1=1
        join clhistnum ch on sd.histid = ch.histid
        join jpagreement jpa on sd.agrid = jpa.agrid and jpa.agrtype = 1  -- омс
        join accident ac on sd.acdid = ac.acdid
        join diagnosis diag on ac.finaldiag = diag.dgcode
        join diagclients dc on ac.acdid = dc.acdid and dc.objtype = 104
        left join stat_doctor s_doct on sd.sdid = s_doct.dirid
        left join doctdepartlinks ddl on ddl.dcode = s_doct.dcode and ddl.depnum = sj.depnum_last -- код лечащего врача в госпитализации
        left join dicinfo dvmp on dvmp.refid = -10002 and dvmp.dicid = ddl.medcaretype   -- ID_VMP  справочник V008 Классификатор видов МП [-10002])
        left join clreferrals clr_in on clr_in.refid = sd.refid -- направление на госпитализацию
        left join dicinfo dpay on dpay.refid = -10004 and dpay.rekvint4 = 1  -- ID_SP_PAY  -- по умолчанию
        left join jpersons jp on clr_in.fromjid = jp.jid  -- направлен откуда
        left join recpropvalues rpv on rpv.recpropid = 10000015 and rpv.recid = jp.jid -- ID_LPU_F - доп параметр в справочнике юр лиц
        left join diagnosis dg on clr_in.dgcode = dg.dgcode  -- диагноз направления
        left join recpropvalues rpv3 on rpv3.recpropid = 10000021 and rpv3.recid = jp.jid --признак ЛПУ "не включать в выгрузку
        left join dicinfo dic_prvs on ddl.idmsp = dic_prvs.dicid and dic_prvs.refid = -10000  -- специальность в отделении = id_prvs в справочнике персонала
        left join dicinfo dic_prvs_dep on dp.idmsp = dic_prvs_dep.dicid and dic_prvs_dep.refid = -10000  -- специальность в отделении = id_prvs в справочнике отделений
        left join dicinfo dic_c_zab on dc.DIAGDESCTYPE = dic_c_zab.dicid and dic_c_zab.refid = -8 -- характер заболевания
        left join dicinfo dic_prmp on dp.idpr = dic_prmp.dicid and dic_prmp.refid = -10005  -- профиль мед. помощи = id_prmp
        left join stat_dep_history sdh on sd.sdid = sdh.dirid
        left join dicinfo dic_b_prof on sdh.bedprofileid = dic_b_prof.dicid and dic_b_prof.refid = -71000002  --  профиль койки
        join treatplace tp on sd.sdid = tp.dirid and tp.pstate = 1 and tp.placeid = 10000713
        join paramsinfo p on tp.protocolid = p.protocolid and p.codeparams = 10018653 and p.ver_no = 0
        join wschema w on p.valueint = w.schid
        where
            cast(sd.planend as date) between :bdate and :fdate and coalesce(sd.enddate_not_present,0) = 0
            and coalesce(rpv3.propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
            and sd.dstatus in (4,7) -- пациент выписан
            and sd.stat_dep = 10000002 -- отдеелние "Дневной стационар" (это онкология)
    ) as eis
    WHERE
        COALESCE((
            SELECT FIRST 1 CHAR_LENGTH(ESL.ERROR)
            FROM ITS_EIS_SERVICE_LOAD ESL WHERE ESL.SERV_ID = EIS.SERV_ID ORDER BY ESL.ESLID DESC), 0) = 0
        AND COALESCE((
            SELECT FIRST 1 ESL.SEND FROM ITS_EIS_SERVICE_LOAD ESL WHERE ESL.SERV_ID = EIS.SERV_ID
            ORDER BY ESL.ESLID DESC),''F'') <> ''T''
    into :SERV_ID,:ID_IN_CASE,:D_NUMBER,:DATE_ISSUE,:DATE_PLANG,:ID_LPU_F,:ID_LPU_T,:ID_D_TYPE,:ID_D_GROUP,:ID_PRVS,:ID_OB_TYPE,:ID_PRMP,:ID_B_PROF,:ID_DN,:ID_GOAL,:ID_DIAGNOS,:ID_LPU_RF,:ID_LPU_TO,:ID_NMKL,:ERROR,:ID_C_ZAB
    do BEGIN
        suspend;
        IF(ID_C_ZAB=2831)THEN
        BEGIN
            ID_IN_CASE=null;
            D_NUMBER=null;
            DATE_ISSUE=(
                select sd.PLANSTART
                from STAT_DIRECTION sd
                where sd.SDID=:SERV_ID
            );
            DATE_PLANG=null;
            ID_LPU_F=null;
            ID_LPU_T=null;
            ID_D_TYPE=38;
            ID_D_GROUP=20;
            ID_PRVS=null;
            ID_OB_TYPE=null;
            ID_PRMP=null;
            ID_B_PROF=null;
            ID_DN=null;
            ID_GOAL=null;
            ID_DIAGNOS=null;
            ID_LPU_RF=null;
            ID_LPU_TO=null;
            ID_NMKL=null;
            ERROR=null;
            suspend;
        END
    end
end',
'ITS_LOAD_AND_LINK_USL            declare variable ADATE type of column SCHSTRUCTURE.ADATE;
declare variable FDATE type of column SCHSTRUCTURE.FDATE;
declare variable SPECCODE type of column WSCHEMA.SPECCODE;
declare variable MAINSCH type of column WSCHEMA.SCHID;
declare variable SECSCH type of column WSCHEMA.SCHID;
declare variable DATESCHSTRUCTID type of column SCHSTRUCTURE.SCHSTRUCTID;
declare variable LINKSCHSTRUCTID type of column SCHSTRUCTURE.SCHSTRUCTID;
declare variable ID type of column MYSCHGROUPS.ID;
declare variable DCODE type of column MYSCHGROUPS.DCODE;
begin
    ADATE = ''01.01.2018'';
    FDATE = ''01.01.2100'';
    SPECCODE = 10021806; -- ВАЖНО!!! Сюда подставить код специализации "ДЛИ"
    DCODE = 10000575;

    SECSCH = (select GEN_ID(WSCH_GEN, 1) from RDB$DATABASE);
    DATESCHSTRUCTID = (select Gen_ID(SCHSTRUCTURE_GEN, 1) from RDB$DATABASE);
    LINKSCHSTRUCTID = (select Gen_ID(SCHSTRUCTURE_GEN, 1) from RDB$DATABASE);
    MAINSCH =  (select first 1 W.SCHID from WSCHEMA W
                left join SPECIALITY S on (W.SPECCODE = S.SCODE)
                where (W.STRUCTID = 1)   and (W.WTYPE = 1) and (Upper(W.KODOPER) containing :mainkodoper)
                order by Upper(W.KODOPER));
    ID = (select GEN_ID(SPRAV_GEN,1) from RDB$DATABASE);

    insert into WSCHEMA
        (DIAGNOST, ISCAPTION, KODOPER, MODIFYDATE, OTHERWRK, PPCAR, PPPER, PPPULP, PREPSTAMP, SCHID, SCHNAME, SPECCODE, SPRIORITET, STRUCTID, TYPEMAKER, AGEUNITTYPE, ISFORREPORT, ISLINKED, WTYPE,PRICEINTREATEDIT, SCODE)
        values (0,''0'', :seckodoper, CURRENT_TIMESTAMP, 0, 0, 0, 0, 0, :SECSCH, :SCHNAME, :SPECCODE, 1, 1, 2, 0, 1, 0, 1, 2, 3);

    insert into SCHSTRUCTURE
        (SCHSTRUCTID, MAINSCH,SECSCH,ADATE,FDATE,KOLVO,JID,LINKTYPE)
        values (:DATESCHSTRUCTID, :MAINSCH, -1, :ADATE, :FDATE,0,0,3);

    insert into MYSCHGROUPS
        (ID,SCHID,DCODE,STAT_USL,STAT_COUNT)
        VALUES (:ID,:MAINSCH,:DCODE,1,1);

    insert into SCHSTRUCTURE
        (SCHSTRUCTID, MAINSCH, SECSCH, ADATE, FDATE, KOLVO, JID, LINKTYPE,   USETYPE)
        values (:LINKSCHSTRUCTID, :MAINSCH, :SECSCH, :ADATE, :FDATE, 1, 0, 3, null);
end',
'ITS_LOAD_TESTS_LATEUS            declare variable cdparam type of column groupsparams.codeparams;
declare variable edizm type of column edizm.edcode;
begin
    edizm = 10021813;
    cdparam = (select gen_id(param_gen, 1) from rdb$database);

    insert into groupsparams (codeparams,nameparams, F25_NAMEPARAMS,typeparams,edcode,mtypeid,TEMPLATECODE) values (:cdparam,:nameparams, :nameparams_2,2,:edizm,0, :templ);
    update groupsdictlinks set sortorder = sortorder + 1  where groupid = :groupid and sortorder >= 1;
    insert into groupsdictlinks (groupid,codeparams,sortorder) values (:groupid,:cdparam,1 ) ;
end',
'ITS_GET_SCH_NUM_IN_PROF          declare variable TEMP_CTDID type of column CLREFDET.CTDID;
begin
  id = 0;
  for select crd.ctdid
    from prof_jornal        pj
    join clreferrals   cr  on cr.profid = pj.prjid
    join clrefdet      crd on crd.refid = cr.refid
    where pj.prjid = :prjid
    order by crd.ctdid
  into temp_ctdid
  do begin
    id = id+1;
    if (ctdid = temp_ctdid) then suspend;
  end
end',
'ITS_GET_SCH_NUM_IN_ACD           declare variable TEMP_TSCHID type of column TREATSCH.TSCHID;
begin
  id = 0;
  for select ts.tschid
    from diagclients  dgc
    join treatsch     ts  on ts.treatcode = dgc.objcode and dgc.organid = ts.organid
    where dgc.acdid = :acdid
    order by ts.tschid
  into temp_tschid
  do begin
    id = id+1;
    if (tschid = temp_tschid) then suspend;
  end
end',
'ITS_GET_SCH_NUM_IN_TREAT         declare variable TEMP_TSCHID type of column TREATSCH.TSCHID;
begin
  id = 0;
  for select ts.tschid
    from treat t
    join treatsch     ts  on ts.treatcode = t.treatcode
    where t.treatcode = :treatcode
    order by ts.tschid
  into temp_tschid
  do begin
    id = id+1;
    if (tschid = temp_tschid) then suspend;
  end
end',
'ITS_I25_VALUETEXT_OLD            begin
  select p.valuetext
from (
      select first 1 t.protocolid
      from treatplace t0
      join treatplace t on t0.pcode = t.pcode and t.protocolid <> t0.protocolid /*and t.treattime < t0.treattime*/ and coalesce(t.treatdelete,0) = 0 and ((t.treatdate = t0.treatdate and :oneday = 1) or :oneday = 0)
      join workplacedoclinks wdl on wdl.placeid = t.placeid
      join groupsdictlinks gdl on wdl.groupid = gdl.groupid and gdl.codeparams = :codeparams
      where t0.protocolid = :protocolid
      order by t.treattime desc
      ) t
join paramsinfo p on p.protocolid = t.protocolid and p.ver_no = 0 and p.codeparams = :codeparams and p.valuetext <> ''''
into :valuetext;
suspend;
end',
'ITS_I25_VALUETEXT                begin
  select p.valuetext
from (
      select first 1 t.protocolid, gp.codeparams
      from treatplace t0
      join groupsparams gp0 on gp0.codeparams = :codeparams
      join treatplace t on t0.pcode = t.pcode and t.protocolid <> t0.protocolid and coalesce(t.treatdelete,0) = 0 and ((t.treatdate = t0.treatdate and :oneday = 1) or :oneday = 0)
      join workplacedoclinks wdl on wdl.placeid = t.placeid
      join groupsdictlinks gdl on wdl.groupid = gdl.groupid --and gdl.codeparams = :codeparams
      join groupsparams gp on gdl.codeparams = gp.codeparams and iif(gp.templatecode <> '''', gp.templatecode = gp0.templatecode, gp.codeparams = :codeparams)
      where t0.protocolid = :protocolid
      order by t.treattime desc
      ) t
join paramsinfo p on p.protocolid = t.protocolid and p.ver_no = 0 and p.codeparams = t.codeparams and p.valuetext <> ''''
into :valuetext;
suspend;
end',
'ITS_REP_IS_WORKING_DAY           declare weekday_num bigint;
declare day_date bigint;
declare month_date bigint;
begin
	is_working_day = 1;

	select extract(weekday from cast(:DATE_IN as date)) from rdb$database
	into :weekday_num;

	--выходные в РФ
	/*select extract(day from cast(:DATE_IN as date))
			, extract(month from cast(:DATE_IN as date))
		from rdb$database
		into day_date, month_date;

	if((day_date in (1,2,3,4,5,6,7,8) and month_date = 1)
		or (day_date in (23) and month_date = 2)
		or (day_date in (8) and month_date = 3)
    or (day_date in (1,9) and month_date = 5)
		or (day_date in (4) and month_date = 11)
    ) then is_working_day = 0; */
   if (:date_in in (select bdate from dicinfo where refid = 991012222212644))
   then is_working_day = 0;

	if(:weekday_num in (0,6)) then is_working_day = 0;
	suspend;

end',
'ITS_DICID_TO_DIAG                begin
  for select distinct coalesce(d.dicid, d2.dicid, d3.dicid) dicid, coalesce(d.dicname, d2.dicname, d3.dicname) dicname,
  coalesce(d.dicname, d2.dicname, d3.dicname) fullname
  from treatplace tp
  join diagclients dc on tp.treatcode = dc.objcode and dc.objtype in (1,101,102,103,104)
  join diagnosis dg on dg.dgcode = dc.dgcode
  left join dicinfo d on d.refid = :refid and (dg.mkbcode = d.txtcode) and (d.bdate <= tp.treatdate or d.bdate is null) and (d.disdate >= tp.treatdate or d.disdate is null)
  left join dicinfo d2 on d2.refid = :refid and (dg.mkbcode containing d2.txtcode) and (d2.bdate <= tp.treatdate or d2.bdate is null) and (d2.disdate >= tp.treatdate or d2.disdate is null)
  left join dicinfo d3 on d3.refid = :refid and d3.txtcode is null and (d3.bdate <= tp.treatdate or d3.bdate is null) and (d3.disdate >= tp.treatdate or d3.disdate is null)
  where tp.protocolid = :protocolid and (d.dicid > 0 or d2.dicid  > 0 or d3.dicid > 0)
  into :dicid, :dicname, :fullname
  do
  begin
    suspend;
  end
end',
'ITS_EIS_GISTO                    begin
  for select distinct d4.dicid dicid, d4.dicname dicname,
  d4.dicname fullname
from its_dicid_to_diag(:refid_parent, :protocolid) its
left join treatplace tp on tp.protocolid = :protocolid
left join dicinfo d on d.dicid = its.dicid and d.refid = :refid_parent
left join dicinfo d4 on d4.refid = :refid_child and d4.rekvint3 = d.rekvint2 and (d4.bdate <= tp.treatdate or d4.bdate is null) and (d4.disdate >= tp.treatdate or d4.disdate is null)
where (its.dicname = :dicname_in or :dicname_in = '''')
  into :dicid, :dicname, :fullname
  do
  begin
    suspend;
  end
end',
'ITS_EIS_DIR_CAN_OPEN             begin
    error_code = 0;
    error_text = '''';

    select first 1 1 as checkcode, ''Случай уже принят в ЕИС, редактирование запрещено!'' as checktext
    from stat_direction sd
    join its_eis_service_load its on its.serv_id = sd.sdid and its.send = ''T''
    where sd.sdid = :dirid into error_code, error_text;
    suspend;
end',
'ITS_EIS_KSG                      begin
  for select distinct w.schid dicid, w.kodoper dicname,
  w.kodoper || '' '' || w.schname fullname
  from treatplace tp
  join diagclients dc on tp.treatcode = dc.objcode and dc.objtype in (1,101,102,103,104)
  join diagnosis dg on dg.dgcode = dc.dgcode
  join vmu_ksg_def v on v.id_diagnosis = dg.eis_id_diagnos AND v.ID_UMP = 2 and v.DATE_END = ''01.01.2200''
  join vmu_ksg vk on v.id_ksg = vk.id_ksg
  --join wschema w on v.id_ksg = w.eis_id_ksg and coalesce(w.bage,18) > 17
  join wschema w on w.kodoper = vk.ksg_code and tp.treatdate < coalesce(w.disdate,''01.01.2200'') and coalesce(w.iscaption,0) = 0 /*and w.structid = (select max(structid) from pricestructure where bdate <= tp.treatdate)*/ and coalesce(w.bage,18) > 17
where v.id_drugs_scheme > 0 and v.id_diagnosis > 0 and tp.protocolid = :protocolid
union
select distinct w.schid dicid, w.kodoper dicname,
  w.kodoper || '' '' || w.schname fullname
  from treatplace tp
  join diagclients dc on tp.treatcode = dc.objcode and dc.objtype in (1,101,102,103,104)
  join vmu_ksg_def v on v.id_diagnosis in (-3) and v.ID_NMKL IS NULL and v.DATE_END = ''01.01.2200'' AND v.ID_UMP = 2
  join vmu_ksg vk on v.id_ksg = vk.id_ksg
  --join wschema w on v.id_ksg = w.eis_id_ksg and coalesce(w.bage,18) > 17
  join wschema w on w.kodoper = vk.ksg_code and tp.treatdate < coalesce(w.disdate,''01.01.2200'') and coalesce(w.iscaption,0) = 0 /*and w.structid = (select max(structid) from pricestructure where bdate <= tp.treatdate)*/ and coalesce(w.bage,18) > 17
where v.id_drugs_scheme > 0 and tp.protocolid = :protocolid
union
select distinct w.schid dicid, w.kodoper dicname,
  w.kodoper || '' '' || w.schname fullname
  from treatplace tp
  join diagclients dc on tp.treatcode = dc.objcode and dc.objtype in (1,101,102,103,104)
  join vmu_ksg_def v on v.id_diagnosis in (-1,-3) and v.ID_NMKL IS NULL and v.DATE_END = ''01.01.2200'' AND v.ID_UMP = 2
  join vmu_ksg vk on v.id_ksg = vk.id_ksg
  --join wschema w on v.id_ksg = w.eis_id_ksg and coalesce(w.bage,18) > 17
  join wschema w on w.kodoper = vk.ksg_code and tp.treatdate < coalesce(w.disdate,''01.01.2200'') and coalesce(w.iscaption,0) = 0 /*and w.structid = (select max(structid) from pricestructure where bdate <= tp.treatdate)*/ and coalesce(w.bage,18) > 17
where tp.protocolid = :protocolid
  into :dicid, :dicname, :fullname
  do
  begin
    suspend;
  end
end',
'ITS_EIS_SCHEME                   begin
  for select distinct dic.dicid dicid, dic.extcode dicname,
  extcode || '' '' || dic.fullname fullname
  from treatplace tp
  join diagclients dc on tp.treatcode = dc.objcode and dc.objtype in (1,101,102,103,104)
  join diagnosis dg on dg.dgcode = dc.dgcode
  join paramsinfo p on tp.protocolid = p.protocolid and p.ver_no = 0
  join groupsparams gp on p.codeparams = gp.codeparams and gp.TEMPLATECODE = ''PROFILE''
  join wschema w on w.schid = p.valueint
  join vmu_ksg vk on w.kodoper = vk.ksg_code
  join vmu_ksg_def v on v.id_diagnosis = dg.eis_id_diagnos and v.id_ksg = vk.id_ksg /*v.id_ksg = w.eis_id_ksg*/ and v.ID_NMKL IS NULL and v.DATE_END = ''01.01.2200'' AND v.ID_UMP = 2
  join dicinfo dic on v.id_drugs_scheme = dic.rekvint2 and dic.refid = -99000001 and dic.rekvint1 = 1
where v.id_drugs_scheme > 0 and v.id_diagnosis > 0 and tp.protocolid = :protocolid
union
select distinct dic.dicid dicid, dic.extcode dicname,
  extcode || '' '' || dic.fullname fullname
  from treatplace tp
  join paramsinfo p on tp.protocolid = p.protocolid and p.ver_no = 0
  join groupsparams gp on p.codeparams = gp.codeparams and gp.TEMPLATECODE = ''PROFILE''
  join wschema w on w.schid = p.valueint
  join vmu_ksg vk on w.kodoper = vk.ksg_code
  join vmu_ksg_def v on v.id_ksg = vk.id_ksg /*v.id_ksg = w.eis_id_ksg*/ and v.ID_NMKL IS NULL and v.DATE_END = ''01.01.2200'' AND v.ID_UMP = 2 and v.id_diagnosis = -3
  join dicinfo dic on v.id_drugs_scheme = dic.rekvint2 and dic.refid = -99000001 and dic.rekvint1 = 1
  where v.id_drugs_scheme > 0 and tp.protocolid = :protocolid
  into :dicid, :dicname, :fullname
  do
  begin
    suspend;
  end
end',
'ITS_EIS_AMBL_S                   BEGIN
    /*
        Выгрузка ЕИС Амбулатория. Случаи. 24.08.2022. Кустов Д.В.

        Связанные задачи:
            - http://support.i-t-systems.ru/otrs/index.pl?Action=AgentTicketZoom;TicketID=38552;ArticleID=255788#255646

        select * from ITS_EIS_AMBL_S([bdate],[fdate],''[treat_depnum]'',''[dcode]'')
    */

    -------------- INIT ----------------------------------------------------------
    -- Подготовка входящих параметров
    select * from ITS_EIS_INIT_PARAMS(:stat_departments_sdepid, :list_doctors,
    -- Пока по умолчанию будет Выгрузка "Отдел лучевой диагностики"
    ''(10000123)'') into :stat_departments_sdepid, :list_doctors;
    -------------------------------------------------------------------------------

    -- Выгрузка "Отдел лучевой диагностики"
    IF(10000123 in (select * from GETINTEGERLIST(:stat_departments_sdepid)))THEN
    BEGIN
        FOR
        ----------------------------------------------------------------------------------------------------
        --? Выгрузка только КТ/МРТ
        with eis_kt_mrt as (
            select
                0 as type_res,
                cast(left (cl_lastname,40) as char(40)) SURNAME,
                cast(left (cl_firstname,40) as char(40)) NAME1,
                cast(left (cl_midname,40) as char(40)) NAME2,
                c_bdate BIRTHDAY,
                c_sex SEX,
                cast(''п'' as char(1)) "ORDER", --порядок направления - плановый
                cast(left (ch_nspser,20) as char(20)) POLIS_S,
                cast(left (ch_nspnum,20) as char(20)) POLIS_N,
                cast('''' as char(5)) PAYER,
                cast('''' as char(5)) STREET,
                cast('''' as char(2)) STREETYPE,
                cast('''' as char(3)) AREA,
                cast('''' as char(7)) HOUSE,
                cast('''' as char(2)) KORP,
                cast('''' as char(5)) FLAT,
                cast(left (w2_kodoper,30) as char(30)) PROFILE,
                iif(ga_ageinyears>=18,''в'',''д'') PROFILENET,
                cast(t_treatdate as date) DATEIN,
                cast(t_treatdate as date) DATEOUT,
                sum(cast(sl_kolvo as numeric(3,0))) AMOUNT,
                cast(diag_mkbcode as char(10)) "DIAGNOSIS",
                cast('''' as char(7)) DIAG_PREF,
                ''F'' SEND,
                cast('''' as char(250)) ERROR,
                cast(iif(c_doctypeid=5,3,1) as char(1)) TYPEDOC,
                cast(left(c_paspser,2) as char(10)) SER1,
                cast(right(c_paspser,2) as char(10)) SER2,
                cast(left(c_paspnum,10) as char(10)) NPASP,
                cast(iif(diag_mkbcode containing ''C'', left(t_orderno || right(coalesce(p_kt_organid, p_mrt_organid),2),11),t_orderno) as numeric(11,0)) SERV_ID,
                cast(iif(diag_mkbcode containing ''C'',1,row_number() over(partition by t_orderno)) as numeric(3,0)) ID_IN_CASE,
                cast(dic_prvs_rekvint2 as numeric(11,0)) ID_PRVS,
                cast(4 as numeric(6,0)) IDPRVSTYPE,
                cast(4 as numeric(6,0)) PRVS_PR_G,
                cast(24 as numeric(11,0)) ID_EXITUS,
                cast(coalesce(c_histnum, cl_pcode) as char(20)) ILLHISTORY,
                cast(1 as numeric(6,0)) CASE_CAST,
                cast(null as numeric(3,0)) AMOUNT_D,
                cast(coalesce(w_treattype, 39) as numeric(6,0)) ID_PRMP,
                cast(coalesce(w_treattype, 39) as numeric(6,0)) ID_PRMP_C,
                cast(iif(diag_mkbcode containing ''C'', diag_mkbcode, max(diag_mkbcode) over(partition by t_orderno)) as char(10)) DIAG_C,
                cast('''' as char(20)) DIAG_S_C,
                cast(iif(diag_mkbcode containing ''C'', diag_mkbcode, max(diag_mkbcode) over(partition by t_orderno)) as char(10)) DIAG_P_C,
                cast(13 as numeric(6,0)) QRESULT,
                cast(dic_prvs_rekvint2 as numeric(11,0)) ID_PRVS_C,
                cast(46 as numeric(6,0)) ID_SP_PAY,
                cast(null as float) ID_ED_PAY,
                cast(5 as numeric(6,0)) ID_VMP,
                cast(''9248.'' || ddl_extcode as char(20)) ID_DOC,
                cast(dp_speccode as char(20)) ID_DEPT,
                cast(''9248.'' || ddl_extcode as char(20)) ID_DOC_C,
                cast(dp_speccode as char(20)) ID_DEPT_C,
                ''F'' IS_CRIM,
                cast(0 as numeric(11,0)) IDSERVDATA,
                cast(0 as numeric(6,0)) IDSERVMADE,
                cast(0 as numeric(11,0)) IDSERVLPU,
                cast(4 as numeric(6,0)) ID_GOAL, -- Посещение с иными целями (4)
                cast(4 as numeric(6,0)) ID_GOAL_C,
                cast(2 as numeric(6,0)) ID_PAT_CAT,
                cast(5 as numeric(6,0)) ID_GOSP,
                cast(coalesce(rpv2_propvalueint,1) as numeric(6,0)) IDVIDVME,
                cast(3 as numeric(6,0)) IDFORPOM,
                cast(null as numeric(6,0)) IDMETHMP,
                cast(9248 as numeric(11,0)) ID_LPU,
                cast(0 as INTEGER) N_BORN, -- не исправлять на numeric !
                ''F'' IS_STAGE,
                cast(null as numeric(11,0)) ID_FINT,
                cast('''' as char(20)) ID_CASE,
                cast('''' as char(20)) ID_SERV,
                cast('''' as char(20)) SNILS,
                cast(1 as numeric(6,0)) ID_TRANSF,
                cast(5 as numeric(6,0)) ID_INCOMPL,
                cast(null as numeric(3,0)) ID_MAIN,
                cast(9248 as numeric(11,0)) ID_LPU_P,
                cast(9248 as numeric(11,0)) ID_LPU_P_C,
                cast(null as numeric(11,0)) ID_B_PROF,
                cast (dic_c_zab_rekvint2 as numeric(11,0)) ID_C_ZAB,
                cast (0 as numeric(11,0)) ID_INTER,
                t_treatcode,
                D_DCODE
            from ITS_EIS_A_L_KT_MRT as eis
            where
                eis.T_TREATDATE between :bdate and :fdate
                and eis.t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
                and coalesce(eis.rpv3_propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
                and eis.dic_goal_rekvint3 in (36,37)  -- цель направления только КТ/МРТ
            group by cl_lastname, cl_pcode, cl_firstname, cl_midname, c_bdate, c_sex, ch_nspser, ch_nspnum, w2_kodoper, w2_schid, c_ageinyears, t_treatdate, c_doctypeid, c_paspser, c_paspnum, t_orderno, c_histnum, w2_kodoper, ga_ageinyears, sl_kolvo, diag_mkbcode, c_doctypeid, t_treatcode, dic_prvs_rekvint2, w_treattype, ddl_extcode, dp_speccode, rpv2_propvalueint, dic_c_zab_rekvint2,p_kt_organid, p_mrt_organid, diag_main_mkbcode, D_DCODE
        ),
        --? Выгрузка всех кроме КТ/МРТ
        eis_other as (
            select
                1 as TYPE_RES,SURNAME,NAME1,NAME2,BIRTHDAY,SEX,"ORDER",POLIS_S,POLIS_N,PAYER,STREET,STREETYPE,AREA,HOUSE,KORP,FLAT,PROFILE,PROFILENET,DATEIN,DATEOUT,sum(cast(kolvo as numeric(3,0))) AMOUNT,DIAGNOSIS,DIAG_PREF,SEND,ERROR,TYPEDOC,SER1,SER2,NPASP,SERV_ID,ID_IN_CASE,ID_PRVS,IDPRVSTYPE,PRVS_PR_G,ID_EXITUS,ILLHISTORY,CASE_CAST,AMOUNT_D,cast(ITS_GET_FIRST_VAL(list(dic_oms_p_REKVINT2)) AS numeric(6, 0)) as ID_PRMP,cast(ITS_GET_FIRST_VAL(list(dic_oms_p_REKVINT2)) AS numeric(6, 0)) as ID_PRMP_C,DIAG_C,DIAG_S_C,DIAG_P_C,QRESULT,ID_PRVS_C,cast(ITS_GET_FIRST_VAL(list(ID_SP_PAY)) as numeric(6, 0)) as ID_SP_PAY,ID_ED_PAY,ID_VMP,ID_DOC,ID_DEPT,ID_DOC_C,ID_DEPT_C,IS_CRIM,IDSERVDATA,IDSERVMADE,IDSERVLPU,ID_GOAL,ID_GOAL_C,ID_PAT_CAT,ID_GOSP,IDVIDVME,IDFORPOM,IDMETHMP,ID_LPU,N_BORN,IS_STAGE,ID_FINT,ID_CASE,ID_SERV,SNILS,ID_TRANSF,ID_INCOMPL,ID_MAIN,ID_LPU_P,ID_LPU_P_C,ID_B_PROF,ID_C_ZAB,ID_INTER,t_treatcode,D_DCODE
            from (
                select
                    cast(left (cl_lastname,40) as char(40)) SURNAME,
                    cast(left (cl_firstname,40) as char(40)) NAME1,
                    cast(left (cl_midname,40) as char(40)) NAME2,
                    c_bdate BIRTHDAY,
                    c_sex SEX,
                    cast(''п'' as char(1)) "ORDER", --порядок направления - плановый
                    cast(left (ch_nspser,20) as char(20)) POLIS_S,
                    cast(left (ch_nspnum,20) as char(20)) POLIS_N,
                    cast('''' as char(5)) PAYER,
                    cast('''' as char(5)) STREET,
                    cast('''' as char(2)) STREETYPE,
                    cast('''' as char(3)) AREA,
                    cast('''' as char(7)) HOUSE,
                    cast('''' as char(2)) KORP,
                    cast('''' as char(5)) FLAT,
                    cast(left (w2_kodoper,30) as char(30)) PROFILE,
                    iif(ga_ageinyears>=18,''в'',''д'') PROFILENET,
                    cast(t_treatdate as date) DATEIN,
                    cast(t_treatdate as date) DATEOUT,
                    -- !!! Колличество усулг с учетом колличетсва ОМС
                    sl.kolvo*tsh_SCOUNT as kolvo, -- AMOUNT
                    cast(diag_main_mkbcode AS CHAR(10)) "DIAGNOSIS",
                    cast('''' as char(7)) DIAG_PREF,
                    ''F'' SEND,
                    cast('''' as char(250)) ERROR,
                    cast(iif(c_doctypeid=5,3,1) as char(1)) TYPEDOC,
                    cast(left(c_paspser,2) as char(10)) SER1,
                    cast(right(c_paspser,2) as char(10)) SER2,
                    cast(left(c_paspnum,10) as char(10)) NPASP,
                    cast(t_orderno as numeric(11,0)) SERV_ID,
                    cast(1 as numeric(3,0)) ID_IN_CASE,
                    cast(dic_prvs_rekvint2 as numeric(11,0)) ID_PRVS,
                    cast(1 as numeric(6,0)) IDPRVSTYPE,
                    cast(1 as numeric(6,0)) PRVS_PR_G,
                    cast(24 as numeric(11,0)) ID_EXITUS,
                    cast(coalesce(c_histnum, cl_pcode) as char(20)) ILLHISTORY,
                    cast(34 as numeric(6,0)) CASE_CAST,
                    cast(null as numeric(3,0)) AMOUNT_D,
                    dic_oms.REKVINT2 as dic_oms_REKVINT2, -- ID_PRMP,ID_PRMP_C
                    dic_oms_p.REKVINT2 as dic_oms_p_REKVINT2, --
                    cast(diag_main_mkbcode AS CHAR(10)) DIAG_C,
                    cast('''' as char(20)) DIAG_S_C,
                    cast(diag_main_mkbcode AS CHAR(10)) DIAG_P_C,
                    cast(13 as numeric(6,0)) QRESULT,
                    cast(dic_prvs_rekvint2 as numeric(11,0)) ID_PRVS_C,
                    cast(dic_oms.REKVINT3 as numeric(6, 0)) as ID_SP_PAY,
                    cast(null as float) ID_ED_PAY,
                    cast(5 as numeric(6,0)) ID_VMP,
                    cast(''9248.'' || ddl_extcode as char(20)) ID_DOC,
                    cast(dp_speccode as char(20)) ID_DEPT,
                    cast(''9248.'' || ddl_extcode as char(20)) ID_DOC_C,
                    cast(dp_speccode as char(20)) ID_DEPT_C,
                    ''F'' IS_CRIM,
                    cast(0 as numeric(11,0)) IDSERVDATA,
                    cast(0 as numeric(6,0)) IDSERVMADE,
                    cast(0 as numeric(11,0)) IDSERVLPU,
                    cast(4 as numeric(6,0)) ID_GOAL, -- Посещение с иными целями (4)
                    cast(4 as numeric(6,0)) ID_GOAL_C,
                    cast(2 as numeric(6,0)) ID_PAT_CAT,
                    cast(5 as numeric(6,0)) ID_GOSP,
                    cast(
                        /*
                        если у услуги из приема в доп.параметрах заполнено поле Идентификатор IDVIDVME в ЕИС ОМС, то передаем ID_NMKL таблицы MDS_EIS_RO_NMKL иначе передаем 1
                        */
                        iif(rs.valueid is not null,
                        (
                            select coalesce(rs.propvalueint,1)
                            from RECPROPERTIES_SEARCH (
                                    tsh_SCHID, -- id услуги
                                    31 -- (услуга) RECTYPES.RECTYPE
                                ) rs
                            where rs.RECPROPID = 10000013  -- Идентификатор номенклатуры ЕИС (ID_NMKL)
                        ),1
                    ) as numeric (6,0)) as IDVIDVME,
                    cast(3 as numeric(6,0)) IDFORPOM,
                    cast(null as numeric(6,0)) IDMETHMP,
                    cast(9248 as numeric(11,0)) ID_LPU,
                    cast(0 as INTEGER) N_BORN, -- не исправлять на numeric !
                    ''F'' IS_STAGE,
                    cast(null as numeric(11,0)) ID_FINT,
                    cast('''' as char(20)) ID_CASE,
                    cast('''' as char(20)) ID_SERV,
                    cast(cl_snils as char(20)) SNILS,
                    cast(1 as numeric(6,0)) ID_TRANSF,
                    cast(5 as numeric(6,0)) ID_INCOMPL,
                    cast(null as numeric(3,0)) ID_MAIN,
                    cast(9248 as numeric(11,0)) ID_LPU_P,
                    cast(9248 as numeric(11,0)) ID_LPU_P_C,
                    cast(null as numeric(11,0)) ID_B_PROF,
                    cast (dic_c_zab_rekvint2 as numeric(11,0)) ID_C_ZAB,
                    cast (0 as numeric(11,0)) ID_INTER,
                    t_treatcode,
                    D_DCODE,
                    TSH_SCHID
                from ITS_EIS_A_L_O as eis
                left JOIN OMSDET oms
                    ON oms.TREATCODE = t_TREATCODE and oms.TSCHID = tsh_TSCHID
                -- Для ID_PRMP и  ID_PRMP_C и AMOUNT
                left JOIN dicinfo dic_oms_p
                    ON dic_oms_p.REFID = -10005
                    AND dic_oms_p.DICID = oms.IDPR
                -- Для ID_SP_PAY
                left JOIN dicinfo dic_oms
                    ON dic_oms.REFID = -10004
                    AND dic_oms.DICID = oms.PAYTYPE_V010
                ----------------------------------------------------
                left join wschema w on w.SCHID = tsh_SCHID
                join SCHLINKS sl on Sl.SCHID =  W.SCHID and Sl.WRKID > 0 and Sl.ITINDEX >= 0
                LEFT JOIN RECPROPERTIES_SEARCH (
                    -- tsh_SCHID, -- id услуги
                    Sl.WRKID,
                    31 -- (услуга) RECTYPES.RECTYPE
                ) rs on rs.RECPROPID = 10000004 -- Идентификатор IDVIDVME в ЕИС ОМС
                where
                    eis.t_treatdate between :bdate and :fdate
                    and eis.t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
                    and (eis.dic_goal_dicid is not null or (eis.clr_goal = 0 or eis.clr_goal is null))   -- цель направления (НЕ КТ/МРТ) или отсутвие цели
                    and eis.w_KODOPER  != ''0011'' -- Услуга в приеме должна быть не КТ/МРТ
            ) as tmp
            group by SURNAME,NAME1,NAME2,BIRTHDAY,SEX,"ORDER",POLIS_S,POLIS_N,PAYER,STREET,STREETYPE,AREA,HOUSE,KORP,FLAT,PROFILE,PROFILENET,DATEIN,DATEOUT,DIAGNOSIS,DIAG_PREF,SEND,ERROR,TYPEDOC,SER1,SER2,NPASP,ID_IN_CASE,SERV_ID,ID_PRVS,IDPRVSTYPE,PRVS_PR_G,ID_EXITUS,ILLHISTORY,CASE_CAST,AMOUNT_D,DIAG_C,DIAG_S_C,DIAG_P_C,QRESULT,ID_PRVS_C,ID_ED_PAY,ID_VMP,ID_DOC,ID_DEPT,ID_DOC_C,ID_DEPT_C,IS_CRIM,IDSERVDATA,IDSERVMADE,IDSERVLPU,ID_GOAL,ID_GOAL_C,ID_PAT_CAT,ID_GOSP,IDVIDVME,IDFORPOM,IDMETHMP,ID_LPU,N_BORN,IS_STAGE,ID_FINT,ID_CASE,ID_SERV,SNILS,ID_TRANSF,ID_INCOMPL,ID_MAIN,ID_LPU_P,ID_LPU_P_C,ID_B_PROF,ID_C_ZAB,ID_INTER,T_TREATCODE,D_DCODE
        )
        select
            TYPE_RES,SURNAME,NAME1,NAME2,BIRTHDAY,SEX,"ORDER",POLIS_S,POLIS_N,PAYER,STREET,STREETYPE,AREA,HOUSE,KORP,FLAT,PROFILE,PROFILENET,DATEIN,DATEOUT,AMOUNT,DIAGNOSIS,DIAG_PREF,SEND,ERROR,TYPEDOC,SER1,SER2,NPASP,SERV_ID,ID_IN_CASE,ID_PRVS,IDPRVSTYPE,PRVS_PR_G,ID_EXITUS,ILLHISTORY,CASE_CAST,AMOUNT_D,ID_PRMP,ID_PRMP_C,DIAG_C,DIAG_S_C,DIAG_P_C,QRESULT,ID_PRVS_C,ID_SP_PAY,ID_ED_PAY,ID_VMP,ID_DOC,ID_DEPT,ID_DOC_C,ID_DEPT_C,IS_CRIM,IDSERVDATA,IDSERVMADE,IDSERVLPU,ID_GOAL,ID_GOAL_C,ID_PAT_CAT,ID_GOSP,IDVIDVME,IDFORPOM,IDMETHMP,ID_LPU,N_BORN,IS_STAGE,ID_FINT,ID_CASE,ID_SERV,SNILS,ID_TRANSF,ID_INCOMPL,ID_MAIN,ID_LPU_P,ID_LPU_P_C,ID_B_PROF,ID_C_ZAB,ID_INTER,t_treatcode,D_DCODE
        from (
            select * from eis_kt_mrt as eis
                union
            select * from eis_other as eis
        ) as eis
        where
            -- Логика вывода приема для определенного доктора, или если не указан списко докторов, то вывводим всех
            iif(
                -- Если не указан доктор, то выводим всех
                :list_doctors != ''-1'',
                -- Если указаны доктора, то выводим только их
                iif(
                    eis.D_DCODE in (select * from GETINTEGERLIST(:list_doctors)),1,0
                )
            ,1)=1
            -- Если случай отправлен, то не показываем его
            and coalesce((select first 1 char_length(esl.error) from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
            and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc),''F'') <> ''T''
        ----------------------------------------------------------------------------------------------------
        into TYPE_RES,SURNAME,NAME1,NAME2,BIRTHDAY,SEX,"ORDER",POLIS_S,
        POLIS_N,PAYER,STREET,STREETYPE,AREA,HOUSE,KORP,FLAT,PROFILE,PROFILENET,DATEIN,
        DATEOUT,AMOUNT,DIAGNOSIS,DIAG_PREF,SEND,ERROR,TYPEDOC,SER1,SER2,NPASP,SERV_ID,ID_IN_CASE,ID_PRVS,IDPRVSTYPE,PRVS_PR_G,ID_EXITUS,ILLHISTORY,CASE_CAST,AMOUNT_D,ID_PRMP,ID_PRMP_C,DIAG_C,DIAG_S_C,DIAG_P_C,QRESULT,ID_PRVS_C,ID_SP_PAY,ID_ED_PAY,ID_VMP,ID_DOC,ID_DEPT,ID_DOC_C,ID_DEPT_C,IS_CRIM,IDSERVDATA,
        IDSERVMADE,IDSERVLPU,ID_GOAL,ID_GOAL_C,ID_PAT_CAT,ID_GOSP,IDVIDVME,IDFORPOM,IDMETHMP,ID_LPU,N_BORN,
        IS_STAGE,ID_FINT,ID_CASE,ID_SERV,SNILS,ID_TRANSF,ID_INCOMPL,ID_MAIN,ID_LPU_P,ID_LPU_P_C,ID_B_PROF,ID_C_ZAB,ID_INTER,t_treatcode,D_DCODE
        do begin
            suspend;
        end
    end
END',
'ITS_EIS_CLINIC_CLIENTS           DECLARE treatcode type of column treat.TREATCODE;
DECLARE REKVINT3 type of column dicinfo.rekvint3;
DECLARE dic_goal_refid type of column dicinfo.refid;
BEGIN
    /*
    Выгрузка пациентов.sql

    select
        list(res_others) as res_others,list(res_kt_mrt) as res_kt_mrt
    from ITS_EIS_CLINIC_clients(''22.08.2022'',''22.08.2022'')
    */
    FOR
    select
        dic_goal.rekvint3,t.TREATCODE,dic_goal.refid
    from treat t
    join clreferrals clr on clr.rtreatcode = t.treatcode -- любой тип направления
    left join dicinfo dic_goal on clr.goal = dic_goal.dicid -- цель направления
    left join jpersons jp2 on clr.fromjid = jp2.jid -- направлен откуда
    left join recpropvalues rpv3 on rpv3.recpropid = 10000021 and rpv3.recid = jp2.jid --признак ЛПУ "не включать в выгрузку
    where
        t.treatdate between :bdate and :fdate
        and t.depnum = 10000123 -- отделение "Отдел лучевой диагностики"
        and coalesce(rpv3.propvalueint,0) = 0 --признак ЛПУ "не включать в выгрузку"
        ---
        and coalesce((select first 1 char_length(esl.error) from its_eis_service_load esl where esl.serv_id = t.ortnarad order by esl.eslid desc), 0) = 0
        and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = t.ortnarad order by esl.eslid desc),''F'') <> ''T''
        ---
    into :rekvint3,:TREATCODE,:dic_goal_refid
    do BEGIN
        -- Приемы КТ/МРТ
        res_kt_mrt=null;
        -- Приемы не КТ/МРТ
        res_others=null;
        -- Выгрузка КТ/МРТ -- цель направления только КТ/МРТ
        IF(:rekvint3 in (36,37) and  :dic_goal_refid = 48)THEN
        BEGIN
            res_kt_mrt = :treatcode;
        END
        -- Остальные
        ELSE
        BEGIN
            res_others = :treatcode;
        END
        suspend;
    end
END',
'ITS_EIS_NAPR_KT_MRT              begin
    /*
    "Выгрузка направлений" для  "Отдел лучевой диагностики" случаев КТ/МРТ

    select * from ITS_EIS_NAPR_KT_MRT([bdate],[fdate])
    */
    FOR
    SELECT DISTINCT
        *
    from
        (
        select
            cast(iif(diag_mkbcode containing ''C'', left(t_orderno || right(coalesce(p_kt_organid, p_mrt_organid),2),11),t_orderno) as numeric(11,0)) SERV_ID
            ,cast(1 as numeric(3,0))  ID_IN_CASE
            ,cast(coalesce(left(clr_extrefid,20),clr_refid) as char(20)) D_NUMBER
            ,cast(coalesce(clr_extrefdate,clr_treatdate) as date) DATE_ISSUE
            ,cast(null as date) DATE_PLANG
            ,cast(coalesce(rpv_propvalueint,9248) as numeric (11,0)) ID_LPU_F
            ,cast(9248 as numeric (11,0)) ID_LPU_T
            ,cast(dic_goal_rekvint3 as numeric(11,0)) ID_D_TYPE
            ,cast(dic_goal_rekvint2 as numeric(11,0)) ID_D_GROUP
            ,cast(null as numeric(11,0)) ID_PRVS
            ,cast(null as numeric(11,0)) ID_OB_TYPE
            ,cast(null as numeric(11,0)) ID_PRMP
            ,cast(null as numeric(11,0)) ID_B_PROF
            ,cast(null as numeric(11,0)) ID_DN
            ,cast(d_p_purpose0appeals_REKVINT1 as numeric(11,0)) ID_GOAL
            ,cast(eis_diag_id_diagnosis as numeric(11,0)) ID_DIAGNOS
            ,cast(null as numeric(11,0)) ID_LPU_RF
            ,cast(null as numeric(11,0)) ID_LPU_TO
            ,cast(null as numeric(11,0)) ID_NMKL
            ,cast (dic_c_zab_rekvint2 as numeric(11,0)) ID_C_ZAB
            ,cast(null as char(200)) ERROR
            -----
            ,t_treatcode
            ,d_dcode
        from ITS_EIS_A_L_KT_MRT
        where
            t_treatdate between :bdate and :fdate
            and t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
            and coalesce(rpv3_propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
            and dic_goal_rekvint3 in (36,37)  -- цель направления только КТ/МРТ
            -----------------------------------------
        ) as eis
        where
            coalesce((select first 1 char_length(esl.error)from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
            and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc),''F'') <> ''T''
    into :SERV_ID,:ID_IN_CASE,:D_NUMBER,:DATE_ISSUE,:DATE_PLANG,:ID_LPU_F,:ID_LPU_T,:ID_D_TYPE,:ID_D_GROUP,:ID_PRVS,:ID_OB_TYPE,:ID_PRMP,:ID_B_PROF,:ID_DN,:ID_GOAL,:ID_DIAGNOS,:ID_LPU_RF,:ID_LPU_TO,:ID_NMKL,:ID_C_ZAB,:ERROR,:t_treatcode,:D_DCODE
    do begin
        ID_DOC=null;
        IDDOCPRVS=null;
        suspend;
        -- Если в файле со случаями у этого SERVID   ID_C_ZAB = 2831, то добавляем к этому SERVID еще одну строку (см приложение)
        IF(ID_C_ZAB=2831)THEN
        BEGIN
            D_NUMBER=null;
            DATE_PLANG=null;
            ID_LPU_F=null;
            ID_LPU_T=null;
            ID_PRVS=null;
            ID_OB_TYPE=null;
            ID_PRMP=null;
            ID_B_PROF=null;
            ID_DN=null;
            ID_GOAL=null;
            ID_DIAGNOS=null;
            ID_LPU_RF=null;
            ID_LPU_TO=null;
            ID_NMKL=null;
            ERROR=null;
            ID_IN_CASE=null;
            DATE_ISSUE=(select TREATDATE  from treat where treatcode=:t_treatcode);
            ID_D_TYPE=38;
            ID_D_GROUP=20;
            suspend;
        END
    end
end',
'ITS_EIS_AMBL_N                   begin
    /*
        Выгрузка ЕИС Амбулатория. Направления. 24.08.2022. Кустов Д.В.

        Связанные задачи:
            - http://support.i-t-systems.ru/otrs/index.pl?Action=AgentTicketZoom;TicketID=38552;ArticleID=255788#255646

        select * from ITS_EIS_AMBL_N([bdate],[fdate],''[treat_depnum]'',''[dcode]'')
    */

    -------------- INIT ----------------------------------------------------------
    -- Подготовка входящих параметров
    select * from ITS_EIS_INIT_PARAMS(:stat_departments_sdepid, :list_doctors,
    -- Пока по умолчанию будет Выгрузка "Отдел лучевой диагностики"
    ''(10000123)'') into :stat_departments_sdepid, :list_doctors;
    -------------------------------------------------------------------------------


    --? Выгрузка "Отдел лучевой диагностики"
    IF(10000123 in (select * from GETINTEGERLIST(:stat_departments_sdepid)))THEN
    BEGIN
        FOR
        ----------------------------------------------------------------------------------------------------
        select
            *
        from (
            --? Выгрузка только КТ/МРТ
            select 0 as type_res, eis_kt_mrt.* from ITS_EIS_NAPR_KT_MRT(:bdate,:fdate) as eis_kt_mrt
                union
            --? Выгрузка всех кроме КТ/МРТ
            select 1 as type_res, eis_other.* from ITS_EIS_NAPR_O(:bdate,:fdate) as eis_other
        ) as eis
        where
            -- Логика вывода приема для определенного доктора, или если не указан списко докторов, то вывводим всех
            iif(
                -- Если не указан доктор, то выводим всех
                :list_doctors != ''-1'',
                -- Если указаны доктора, то выводим только их
                iif(
                    eis.D_DCODE in (select * from GETINTEGERLIST(:list_doctors)),1,0
                )
            ,1)=1
            -- Если случай отправлен, то не показываем его
            and coalesce((select first 1 char_length(esl.error) from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
            and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc),''F'') <> ''T''
        ----------------------------------------------------------------------------------------------------
        into :TYPE_RES,:SERV_ID,:ID_IN_CASE,:D_NUMBER,:DATE_ISSUE,:DATE_PLANG,:ID_LPU_F,:ID_LPU_T,:ID_D_TYPE,:ID_D_GROUP,:ID_PRVS,:ID_OB_TYPE,:ID_PRMP,:ID_B_PROF,:ID_DN,:ID_GOAL,:ID_DIAGNOS,:ID_LPU_RF,:ID_LPU_TO,:ID_C_ZAB,:ID_NMKL,:ID_DOC,:IDDOCPRVS,:ERROR,:t_treatcode,:d_dcode
        do begin
            suspend;
        end
    end
end',
'ITS_EIS_NAPR_O                   begin
    /*
    "Выгрузка направлений" для  "Отдел лучевой диагностики" случаев НЕ КТ/МРТ

    select * from ITS_EIS_NAPR_O([bdate],[fdate])
    */
    FOR
    SELECT DISTINCT
        *
    from
        (
        select
            cast(t_orderno as numeric(11,0)) SERV_ID
            ,cast(1 as numeric(3,0))  ID_IN_CASE
            ,cast(null as char(20)) D_NUMBER
            ,cast(coalesce(clr_extrefdate,clr_treatdate) as date) DATE_ISSUE
            ,cast(null as date) DATE_PLANG
            ,cast(coalesce(rpv_propvalueint,9248) as numeric (11,0)) ID_LPU_F
            ,cast(null as numeric (11,0)) ID_LPU_T
            ,cast(19 as numeric(11,0)) ID_D_TYPE
            ,cast(14 as numeric(11,0)) ID_D_GROUP
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
            ,cast (dic_c_zab_rekvint2 as numeric(11,0)) ID_C_ZAB
            ,cast(null as char(200)) ERROR
            -----
            ,t_treatcode
            ,d_dcode
        from ITS_EIS_A_L_O
        where
            t_treatdate between :bdate and :fdate
            and t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
            and (dic_goal_dicid is not null or (clr_goal = 0 or clr_goal is null))   -- цель направления (НЕ КТ/МРТ) или отсутвие цели
            and w_KODOPER  != ''0011'' -- Услуга в приеме должна быть не КТ/МРТ
        ) as eis
        where
            coalesce((select first 1 char_length(esl.error)from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
            and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc),''F'') <> ''T''
    into :SERV_ID,:ID_IN_CASE,:D_NUMBER,:DATE_ISSUE,:DATE_PLANG,:ID_LPU_F,:ID_LPU_T,:ID_D_TYPE,:ID_D_GROUP,:ID_PRVS,:ID_OB_TYPE,:ID_PRMP,:ID_B_PROF,:ID_DN,:ID_GOAL,:ID_DIAGNOS,:ID_LPU_RF,:ID_LPU_TO,:ID_NMKL,:ID_C_ZAB,:ERROR,:t_treatcode,:D_DCODE
    do begin
        ID_DOC=null;
        IDDOCPRVS=null;
        IF(DATE_ISSUE is null)THEN
        BEGIN
            DATE_ISSUE=(select TREATDATE from treat where treatcode=:t_treatcode);
        END
        suspend;
        -- Если в файле со случаями у этого SERVID   ID_C_ZAB = 2831, то добавляем к этому SERVID еще одну строку (см приложение)
        IF(ID_C_ZAB=2831)THEN
        BEGIN
            D_NUMBER=null;
            DATE_PLANG=null;
            ID_LPU_F=null;
            ID_LPU_T=null;
            ID_PRVS=null;
            ID_OB_TYPE=null;
            ID_PRMP=null;
            ID_B_PROF=null;
            ID_DN=null;
            ID_GOAL=null;
            ID_DIAGNOS=null;
            ID_LPU_RF=null;
            ID_LPU_TO=null;
            ID_NMKL=null;
            ERROR=null;
            ID_IN_CASE=null;
            DATE_ISSUE=(select TREATDATE from treat where treatcode=:t_treatcode);
            ID_D_TYPE=38;
            ID_D_GROUP=20;
            suspend;
        END
    end
end',
'ITS_EIS_INIT_PARAMS              BEGIN
    -----------------------------------------------------------------
    IF(:in_list_depnum = ''(-1)'')THEN
    BEGIN
        :in_list_depnum=:def_depnum;
    END
    -- Обрезаем скобки которые ставятся в Генераторе отчетов
    list_depnum=trim(''('' from  trim('')'' from :in_list_depnum));
    -----------------------------------------------------------------
    IF(:in_list_depnum = ''(-1)'')THEN
    BEGIN
        :in_list_depnum=:def_doctors;
    END
    -- Обрезаем скобки которые ставятся в Генераторе отчетов
    list_doctors=trim(''('' from  trim('')'' from :in_list_doctors));
    -----------------------------------------------------------------
    suspend;
END',
'ITS_EIS_AMBL_D_S                 begin

    /*
        Выгрузка ЕИС Амбулатория. доп. данных по случаям. 24.08.2022. Кустов Д.В.

        Связанные задачи:
            - http://support.i-t-systems.ru/otrs/index.pl?Action=AgentTicketZoom;TicketID=38552;ArticleID=255788#255646

        select * from ITS_EIS_AMBL_D_S([bdate],[fdate],''[treat_depnum]'',''[dcode]'')
    */

    -------------- INIT ----------------------------------------------------------
    -- Подготовка входящих параметров
    select * from ITS_EIS_INIT_PARAMS(:stat_departments_sdepid, :list_doctors,
    -- Пока по умолчанию будет Выгрузка "Отдел лучевой диагностики"
    ''(10000123)'') into :stat_departments_sdepid, :list_doctors;
    -------------------------------------------------------------------------------

    --? Выгрузка "Отдел лучевой диагностики"
    IF(10000123 in (select * from GETINTEGERLIST(:stat_departments_sdepid)))THEN
    BEGIN
        FOR
        ----------------------------------------------------------------------------------------------------
        --? Выгрузка КТ/МРТ
        with eis_kt_mrt as (
            select 0 as type_res, eis.*  from (
                select
                    cast(iif(diag_mkbcode containing ''C'', left(t_orderno || right(coalesce(p_kt_organid, p_mrt_organid),2),11),t_orderno) as numeric(11,0)) SERV_ID
                    ,cast(iif(dic_c_zab_rekvint2=2831,6,null) as numeric(4, 0)) as ID_OBJECT
                    ,cast(''1'' AS CHAR(10)) AS OBJ_VALUE
                    ,cast(null AS CHAR(200)) AS ERROR
                    ,t_treatcode
                    ,d_dcode
                    ,dic_c_zab_rekvint2
                from ITS_EIS_A_L_KT_MRT
                where
                    t_treatdate between :bdate and :fdate
                    and t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
                    and coalesce(rpv3_propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
                    and dic_goal_rekvint3 in (36,37)  -- цель направления только КТ/МРТ
            ) as eis
            -- Нужны только записи с 6
            where ID_OBJECT = 6
            group by SERV_ID,ID_OBJECT,OBJ_VALUE,ERROR,t_treatcode,d_dcode,dic_c_zab_rekvint2
        )
        --? Выгрузка остальных приемов, только не КТ/МРТ
        , eis_other as (
            select 1 as type_res, eis.* from (
                select
                    cast(t_orderno as numeric(11,0)) SERV_ID
                    ,cast(iif(dic_c_zab_rekvint2=2831,6,null) as numeric(4, 0)) as ID_OBJECT
                    ,cast(''1'' AS CHAR(10)) AS OBJ_VALUE
                    ,cast(null AS CHAR(200)) AS ERROR
                    ,t_treatcode
                    ,d_dcode
                    ,dic_c_zab_rekvint2
                from ITS_EIS_A_L_O
                where
                    t_treatdate between :bdate and :fdate
                    and t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
                    and (dic_goal_dicid is not null or (clr_goal = 0 or clr_goal is null))   -- цель направления (НЕ КТ/МРТ) или отсутвие цели
                    and w_KODOPER  != ''0011'' -- Услуга в приеме должна быть не КТ/МРТ
            ) as eis
            -- Нужны только записи с 6
            where ID_OBJECT = 6
            group by SERV_ID,ID_OBJECT,OBJ_VALUE,ERROR,t_treatcode,d_dcode,dic_c_zab_rekvint2
        )
        ----------------------------------------------------------------------------------------------------
        select
            *
        from (
            select * from eis_kt_mrt as eis
                union
            select * from eis_other as eis
        ) as eis
        where
            -- Логика вывода приема для определенного доктора, или если не указан списко докторов, то вывводим всех
            iif(
                -- Если не указан доктор, то выводим всех
                :list_doctors != ''-1'',
                -- Если указаны доктора, то выводим только их
                iif(
                    eis.D_DCODE in (select * from GETINTEGERLIST(:list_doctors)),1,0
                )
            ,1)=1
            -- Если случай отправлен, то не показываем его
            and coalesce((select first 1 char_length(esl.error) from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
            and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc),''F'') <> ''T''
        ----------------------------------------------------------------------------------------------------
        INTO type_res,SERV_ID,ID_OBJECT,OBJ_VALUE,ERROR,t_treatcode,d_dcode,dic_c_zab_rekvint2
        do begin
            suspend;
        end
    end
end',
'ITS_EIS_AMBL_P                   BEGIN
    /*
        Выгрузка ЕИС Амбулатория. Пациенты. 24.08.2022. Кустов Д.В.

        Связанные задачи:
            - http://support.i-t-systems.ru/otrs/index.pl?Action=AgentTicketZoom;TicketID=38552;ArticleID=255788#255646

        select * from ITS_EIS_AMBL_P([bdate],[fdate],''[treat_depnum]'',''[dcode]'')
    */

    -------------- INIT ----------------------------------------------------------
    -- Подготовка входящих параметров
    select * from ITS_EIS_INIT_PARAMS(:stat_departments_sdepid, :list_doctors,
    -- Пока по умолчанию будет Выгрузка "Отдел лучевой диагностики"
    ''(10000123)'') into :stat_departments_sdepid, :list_doctors;
    -------------------------------------------------------------------------------

    --? Выгрузка "Отдел лучевой диагностики"
    IF(10000123 in (select * from GETINTEGERLIST(:stat_departments_sdepid)))THEN
    BEGIN
        FOR
        ----------------------------------------------------------------------------------------------------
        --? Выгрузка только КТ/МРТ
        with eis_kt_mrt as (
            select
                0 as type_res,
                cast(t_orderno as numeric(11,0)) SERV_ID,
                cast(left(coalesce(cl_lastname, ''''),40) as char(40)) SURNAME,
                cast(left(coalesce(cl_firstname, ''''),40) as char(40)) NAME,
                cast(left(coalesce(cl_midname, ''''),40) as char(40)) S_NAME,
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
                iif(left(right(cl_paspser, 3), 1) = ''-'' or left(right(cl_paspser, 3), 1) = '' '',
                left(trim(left(cl_paspser, iif((CHAR_LENGTH(cl_paspser) - 3) < 0, 0, CHAR_LENGTH(cl_paspser) - 3))), 6),
                left(trim(left(cl_paspser, iif((CHAR_LENGTH(cl_paspser) - 2) < 0, 0, CHAR_LENGTH(cl_paspser) - 2))), 6))
                as char(6)) SER_L,
                cast(right(coalesce(cl_paspser, ''''), 2) as char(2)) SER_R,
                cast(left(coalesce(cl_paspnum, ''''), 12) as char (12)) DOC_NUMBER,
                cl_paspdate ISSUE_DATE,
                cl_paspplace as DOCORG_ASMEMO,
                cast(left(coalesce(cl_snils, ''''), 14) as char (14)) SNILS,
                coalesce(di_REKVTEXT1,''000'') C_OKSM,
                null IS_SMP,
                cast(clh_nsptype as smallint) POLIS_TYPE,
                clh_nsp,
                cast(left(coalesce(clh_nspser, ''''), 20) as CHAR(20)) POLIS_S,
                cast(left(coalesce(clh_nspnum, ''''), 20) as CHAR(20)) POLIS_N,
                cast(left(jp_lpucode2,3) as numeric(3,0)) ID_SMO,
                cast(clh_bdate as date) POLIS_BD,
                cast(iif(coalesce(clh_datecancel, ''01.01.3000'') > clh_fdate, clh_fdate, clh_datecancel) as date) POLIS_ED,
                null ID_SMO_REG,
                cast((case when cl_KLCODE_REG like ''78%'' then ''г''
                when not di_SIMPLENAME containing ''РОССИ'' THEN ''п''
                when (not cl_KLCODE_REG is null or (not ter_reg_TCODE is null and cl_KLCODE_REG is null)) then ''р''
                else '''' end) as Character (1)) ADDR_TYPE,
                iif(cl_kodter_reg is not null, cast(cl_kodter_reg as smallint), cast(cklr_tercode as smallint)) IDOKATOREG,
                null IDOBLTOWN,
                null ID_PREFIX,
                cast(left(gh_id_house,9) as Numeric (9,0)) ID_HOUSE,
                iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like ''78%'', cast(left(coalesce(cl_addr_reg, ''''), 10) as char(10)), null) HOUSE,
                iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like ''78%'', cast(left(coalesce(cl_corp_reg, ''''), 5) as char(5)), null) KORPUS,
                iif(coalesce(cklr_klcode, 0) > 0 , cast(left(coalesce(cl_flat_reg, ''''), 5) as char(5)), null) FLAT,
                iif(coalesce(cklr_klcode, 0) > 0 and not di_SIMPLENAME containing ''РОССИ'', cast(left(coalesce(c_addr_reg, ''''), 200) as char(200)),null) U_ADDRESS,
                iif(coalesce(klr_code, 0) > 0 and not cl_KLCODE_REG like ''78%'', cast(left(coalesce(klr_code, ''''), 13) as char(13)), null) KLADR_CODE,
                iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like ''78%'', cast(left(coalesce(cklr_stname, ''''), 150) as char(150)), null) STREET,
                iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like ''78%'', gh_idstrtype, null) IDSTRTYPE,
                cast((case when cl_KLCODE like ''78%'' then ''г''
                when not di_SIMPLENAME containing ''РОССИ'' THEN ''п''
                when (not cl_KLCODE is null or (not ter_reg_TCODE is null and cl_KLCODE is null)) then ''р''
                else '''' end) as Character (1)) ADDRTYPE_L,
                iif(cl_kodter is not null, cast(cl_kodter as smallint), cast(cklf_tercode as smallint)) OKATOREG_L,
                null OBLTOWN_L,
                null PREFIX_L,
                cast(left(gh_fact_id_house,9) as Numeric (9,0)) ID_HOUSE_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like ''78%'', cast(left(coalesce(cl_addr, ''''), 10) as char(10)), null) HOUSE_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like ''78%'', cast(left(coalesce(cl_corp, ''''), 5) as char(5)), null) KORPUS_L,
                iif(coalesce(cklf_klcode, 0) > 0 , cast(left(coalesce(cl_flat, ''''), 5) as char(5)), null) FLAT_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not di_SIMPLENAME containing ''РОССИ'', cast(left(coalesce(c_addr_fact, ''''), 200) as char(200)),null) U_ADDR_L,
                iif(coalesce(klf_code, 0) > 0 and not cl_KLCODE like ''78%'', cast(left(coalesce(klf_code, ''''), 13) as char(13)), null) KLADR_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like ''78%'', cast(left(coalesce(cklf_stname, ''''), 150) as char(150)), null) STREET_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like ''78%'', gh_fact_idstrtype,null) STRTYPE_L,
                cast(left(coalesce(c_workplace, ''''), 254) as CHAR(254)) PLACE_WORK,
                null ADDR_WORK,
                null ADDR_PLACE,
                null REMARK,
                cast(left(coalesce(c_BIRTHPLACE, ''''), 100) as char(100)) B_PLACE,
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
            from ITS_EIS_A_L_KT_MRT as eis
            where
                eis.T_treatdate between :bdate and :fdate
                and eis.t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
                and coalesce(eis.rpv3_propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
                and eis.dic_goal_rekvint3 in (36,37)  -- цель направления только КТ/МРТ
        )
        --? Выгрузка остальных приемов, только не КТ/МРТ
        , eis_other as (
            select
                1 as type_res,
                cast(t_orderno as numeric(11,0)) SERV_ID,
                cast(left(coalesce(cl_lastname, ''''),40) as char(40)) SURNAME,
                cast(left(coalesce(cl_firstname, ''''),40) as char(40)) NAME,
                cast(left(coalesce(cl_midname, ''''),40) as char(40)) S_NAME,
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
                iif(left(right(cl_paspser, 3), 1) = ''-'' or left(right(cl_paspser, 3), 1) = '' '',
                left(trim(left(cl_paspser, iif((CHAR_LENGTH(cl_paspser) - 3) < 0, 0, CHAR_LENGTH(cl_paspser) - 3))), 6),
                left(trim(left(cl_paspser, iif((CHAR_LENGTH(cl_paspser) - 2) < 0, 0, CHAR_LENGTH(cl_paspser) - 2))), 6))
                as char(6)) SER_L,
                cast(right(coalesce(cl_paspser, ''''), 2) as char(2)) SER_R,
                cast(left(coalesce(cl_paspnum, ''''), 12) as char (12)) DOC_NUMBER,
                cl_paspdate ISSUE_DATE,
                cl_paspplace as DOCORG_ASMEMO,
                cast(left(coalesce(cl_snils, ''''), 14) as char (14)) SNILS,
                coalesce(di_REKVTEXT1,''000'') C_OKSM,
                null IS_SMP,
                cast(clh_nsptype as smallint) POLIS_TYPE,
                clh_nsp,
                cast(left(coalesce(clh_nspser, ''''), 20) as CHAR(20)) POLIS_S,
                cast(left(coalesce(clh_nspnum, ''''), 20) as CHAR(20)) POLIS_N,
                cast(left(jp_lpucode2,3) as numeric(3,0)) ID_SMO,
                cast(clh_bdate as date) POLIS_BD,
                cast(iif(coalesce(clh_datecancel, ''01.01.3000'') > clh_fdate, clh_fdate, clh_datecancel) as date) POLIS_ED,
                null ID_SMO_REG,
                cast((case when cl_KLCODE_REG like ''78%'' then ''г''
                when not di_SIMPLENAME containing ''РОССИ'' THEN ''п''
                when (not cl_KLCODE_REG is null or (not ter_reg_TCODE is null and cl_KLCODE_REG is null)) then ''р''
                else '''' end) as Character (1)) ADDR_TYPE,
                iif(cl_kodter_reg is not null, cast(cl_kodter_reg as smallint), cast(cklr_tercode as smallint)) IDOKATOREG,
                null IDOBLTOWN,
                null ID_PREFIX,
                cast(left(gh_id_house,9) as Numeric (9,0)) ID_HOUSE,
                iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like ''78%'', cast(left(coalesce(cl_addr_reg, ''''), 10) as char(10)), null) HOUSE,
                iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like ''78%'', cast(left(coalesce(cl_corp_reg, ''''), 5) as char(5)), null) KORPUS,
                iif(coalesce(cklr_klcode, 0) > 0 , cast(left(coalesce(cl_flat_reg, ''''), 5) as char(5)), null) FLAT,
                iif(coalesce(cklr_klcode, 0) > 0 and not di_SIMPLENAME containing ''РОССИ'', cast(left(coalesce(c_addr_reg, ''''), 200) as char(200)),null) U_ADDRESS,
                iif(coalesce(klr_code, 0) > 0 and not cl_KLCODE_REG like ''78%'', cast(left(coalesce(klr_code, ''''), 13) as char(13)), null) KLADR_CODE,
                iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like ''78%'', cast(left(coalesce(cklr_stname, ''''), 150) as char(150)), null) STREET,
                iif(coalesce(cklr_klcode, 0) > 0 and not cl_KLCODE_REG like ''78%'', gh_idstrtype, null) IDSTRTYPE,
                cast((case when cl_KLCODE like ''78%'' then ''г''
                when not di_SIMPLENAME containing ''РОССИ'' THEN ''п''
                when (not cl_KLCODE is null or (not ter_reg_TCODE is null and cl_KLCODE is null)) then ''р''
                else '''' end) as Character (1)) ADDRTYPE_L,
                iif(cl_kodter is not null, cast(cl_kodter as smallint), cast(cklf_tercode as smallint)) OKATOREG_L,
                null OBLTOWN_L,
                null PREFIX_L,
                cast(left(gh_fact_id_house,9) as Numeric (9,0)) ID_HOUSE_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like ''78%'', cast(left(coalesce(cl_addr, ''''), 10) as char(10)), null) HOUSE_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like ''78%'', cast(left(coalesce(cl_corp, ''''), 5) as char(5)), null) KORPUS_L,
                iif(coalesce(cklf_klcode, 0) > 0 , cast(left(coalesce(cl_flat, ''''), 5) as char(5)), null) FLAT_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not di_SIMPLENAME containing ''РОССИ'', cast(left(coalesce(c_addr_fact, ''''), 200) as char(200)),null) U_ADDR_L,
                iif(coalesce(klf_code, 0) > 0 and not cl_KLCODE like ''78%'', cast(left(coalesce(klf_code, ''''), 13) as char(13)), null) KLADR_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like ''78%'', cast(left(coalesce(cklf_stname, ''''), 150) as char(150)), null) STREET_L,
                iif(coalesce(cklf_klcode, 0) > 0 and not cl_KLCODE like ''78%'', gh_fact_idstrtype,null) STRTYPE_L,
                cast(left(coalesce(c_workplace, ''''), 254) as CHAR(254)) PLACE_WORK,
                null ADDR_WORK,
                null ADDR_PLACE,
                null REMARK,
                cast(left(coalesce(c_BIRTHPLACE, ''''), 100) as char(100)) B_PLACE,
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
                and eis.w_KODOPER  != ''0011'' -- Услуга в приеме должна быть не КТ/МРТ
        )
        ----------------------------------------------------------------------------------------------------
        select
            *
        from (
            select * from eis_kt_mrt as eis
                union
            select * from eis_other as eis
        ) as eis
        where
            -- Логика вывода приема для определенного доктора, или если не указан списко докторов, то вывводим всех
            iif(
                -- Если не указан доктор, то выводим всех
                :list_doctors != ''-1'',
                -- Если указаны доктора, то выводим только их
                iif(
                    eis.D_DCODE in (select * from GETINTEGERLIST(:list_doctors)),1,0
                )
            ,1)=1
            -- Если случай отправлен, то не показываем его
            and coalesce((select first 1 char_length(esl.error) from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
            and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc),''F'') <> ''T''
        ----------------------------------------------------------------------------------------------------
        into TYPE_RES,SERV_ID,SURNAME,NAME,S_NAME,BIRTHDAY,SEX,ID_PAT_CAT,LGOTS,DOC_TYPE,SER_L,SER_R,DOC_NUMBER,ISSUE_DATE,DOCORG_ASMEMO,SNILS,C_OKSM,IS_SMP,POLIS_TYPE,CLH_NSP,POLIS_S,POLIS_N,ID_SMO,POLIS_BD,POLIS_ED,ID_SMO_REG,ADDR_TYPE,IDOKATOREG,IDOBLTOWN,ID_PREFIX,ID_HOUSE,HOUSE,KORPUS,FLAT,U_ADDRESS,KLADR_CODE,STREET,IDSTRTYPE,ADDRTYPE_L,OKATOREG_L,OBLTOWN_L,PREFIX_L,ID_HOUSE_L,HOUSE_L,KORPUS_L,FLAT_L,U_ADDR_L,KLADR_L,STREET_L,STRTYPE_L,PLACE_WORK,ADDR_WORK,ADDR_PLACE,REMARK,B_PLACE,VNOV_D,ID_G_TYPE,G_SURNAME,G_NAME,G_S_NAME,G_BIRTHDAY,G_SEX,G_DOC_TYPE,G_SERIA_L,G_SERIA_R,G_DOC_NUM,G_ISSUE_D,G_DOCORG_ASMEMO,G_B_PLACE,N_BORN,SEND,ERROR,ID_MIS,ID_PATIENT,T_TREATCODE,D_DCODE
        do BEGIN
            suspend;
        end
    end
END',
'ITS_EIS_AMBL_CONS                begin
    /*
        Выгрузка ЕИС Амбулатория. Выгрузка консилиума. 24.08.2022. Кустов Д.В.

        Связанные задачи:
            - http://support.i-t-systems.ru/otrs/index.pl?Action=AgentTicketZoom;TicketID=38552;ArticleID=255788#255646

        select * from ITS_EIS_AMBL_CONS([bdate],[fdate],''[treat_depnum]'',''[dcode]'')
    */

    -------------- INIT ----------------------------------------------------------
    -- Подготовка входящих параметров
    select * from ITS_EIS_INIT_PARAMS(:stat_departments_sdepid, :list_doctors,
    -- Пока по умолчанию будет Выгрузка "Отдел лучевой диагностики"
    ''(10000123)'') into :stat_departments_sdepid, :list_doctors;
    -------------------------------------------------------------------------------

    --? Выгрузка "Отдел лучевой диагностики"
    IF(10000123 in (select * from GETINTEGERLIST(:stat_departments_sdepid)))THEN
    BEGIN
        FOR
        ----------------------------------------------------------------------------------------------------
        --? Выгрузка только КТ/МРТ
        with eis_kt_mrt as (
            select
                cast(iif(diag_mkbcode containing ''C'', left(t_orderno || right(coalesce(p_kt_organid, p_mrt_organid),2),11),t_orderno) as numeric(11,0)) SERV_ID,
                cast(2838 as numeric(11,0)) ID_PR_CONS,
                cast(t_treatdate as date) DATE_CONS,
                cast(null as char(200)) ERROR,
                T_TREATCODE
                ,D_DCODE
            from ITS_EIS_A_L_KT_MRT
            where
                t_treatdate between :bdate and :fdate
                and t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
                and coalesce(rpv3_propvalueint,0) = 0 --признак ЛПУ "не включать в выгрузку"
                and dic_goal_rekvint3 in (36,37) -- цель направления только КТ/МРТ
                and diag_mkbcode containing ''C'' -- вкладка нужна только для онко
            group by cl_lastname, cl_pcode, cl_firstname, cl_midname, c_bdate, c_sex, ch_nspser, ch_nspnum, w2_kodoper, w2_schid, c_ageinyears, t_treatdate, c_doctypeid, c_paspser,  c_paspnum, t_orderno, c_histnum, w2_kodoper, ga_ageinyears, sl_kolvo, diag_mkbcode,c_doctypeid, t_treatcode, dic_prvs_rekvint2, w_treattype, ddl_extcode, dp_speccode, rpv2_propvalueint, dic_c_zab_rekvint2,dic_c_zab_rekvint3, dic_goal_rekvint3, dic_goal_rekvint2,clr_extrefid,clr_refid,clr_extrefdate,clr_treatdate, rpv_propvalueint, diag_main_mkbcode, p_kt_organid, p_mrt_organid, eis_diag_id_diagnosis,D_DCODE
        )
        select
            *
        from (
            select 0 as TYPE_RES, eis.* from eis_kt_mrt as eis
        ) as eis
        where
            -- Логика вывода приема для определенного доктора, или если не указан списко докторов, то вывводим всех
            iif(
                -- Если не указан доктор, то выводим всех
                :list_doctors != ''-1'',
                -- Если указаны доктора, то выводим только их
                iif(
                    eis.D_DCODE in (select * from GETINTEGERLIST(:list_doctors)),1,0
                )
            ,1)=1
            -- Если случай отправлен, то не показываем его
            and coalesce((select first 1 char_length(esl.error) from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
            and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc),''F'') <> ''T''
        ----------------------------------------------------------------------------------------------------
        into TYPE_RES,SERV_ID,ID_PR_CONS,DATE_CONS,ERROR,T_TREATCODE,D_DCODE
        do begin
            suspend;
        end
    end
end',
'ITS_EIS_AMBL_ONKO_ADD            begin

    /*
        Выгрузка ЕИС Амбулатория. Выгрузка доп. данных для ОНКО _ONKO_ADD. 24.08.2022. Кустов Д.В.

        Связанные задачи:
            - http://support.i-t-systems.ru/otrs/index.pl?Action=AgentTicketZoom;TicketID=38552;ArticleID=255788#255646

        select * from ITS_EIS_AMBL_ONKO_ADD([bdate],[fdate],''[treat_depnum]'',''[dcode]'')
    */

    -------------- INIT ----------------------------------------------------------
    -- Подготовка входящих параметров
    select * from ITS_EIS_INIT_PARAMS(:stat_departments_sdepid, :list_doctors,
    -- Пока по умолчанию будет Выгрузка "Отдел лучевой диагностики"
    ''(10000123)'') into :stat_departments_sdepid, :list_doctors;
    -------------------------------------------------------------------------------

    --? Выгрузка "Отдел лучевой диагностики"
    IF(10000123 in (select * from GETINTEGERLIST(:stat_departments_sdepid)))THEN
    BEGIN
        FOR
        ----------------------------------------------------------------------------------------------------
        --? Выгрузка только КТ/МРТ
        with eis_kt_mrt as (
            select
                cast(iif(diag_mkbcode containing ''C'', left(t_orderno || right(coalesce(p_kt_organid, p_mrt_organid),2),11),t_orderno) as numeric(11,0)) SERV_ID,
                cast(eis_diag_id_diagnosis as numeric(11,0)) ID_DIAGNOS,
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
                cast(null as char(200)) ERROR,
                T_TREATCODE,
                d_dcode
            from ITS_EIS_A_L_KT_MRT
            where
                t_treatdate between :bdate and :fdate
                and t_depnum = 10000123 -- отделение "Отдел лучевой диагностики"
                and coalesce(rpv3_propvalueint,0) = 0 --признак ЛПУ "не включать в выгрузку"
                and dic_goal_rekvint3 in (36,37) -- цель направления только КТ/МРТ
                and diag_mkbcode containing ''C'' -- вкладка нужна только для онко
            group by cl_lastname, cl_pcode, cl_firstname, cl_midname, c_bdate, c_sex, ch_nspser, ch_nspnum, w2_kodoper, w2_schid, c_ageinyears, t_treatdate, c_doctypeid, c_paspser,  c_paspnum, t_orderno, c_histnum, w2_kodoper, ga_ageinyears, sl_kolvo, diag_mkbcode,c_doctypeid, t_treatcode, dic_prvs_rekvint2, w_treattype, ddl_extcode, dp_speccode, rpv2_propvalueint, dic_c_zab_rekvint2,dic_c_zab_rekvint3, dic_goal_rekvint3, dic_goal_rekvint2, clr_extrefid,clr_refid,clr_extrefdate,clr_treatdate, rpv_propvalueint, diag_main_mkbcode, p_kt_organid, p_mrt_organid, eis_diag_id_diagnosis,T_TREATCODE,d_dcode
        )
        select
            *
        from (
            select 0 as TYPE_RES,eis.* from eis_kt_mrt as eis
        ) as eis
        where
            -- Логика вывода приема для определенного доктора, или если не указан списко докторов, то вывводим всех
            iif(
                -- Если не указан доктор, то выводим всех
                :list_doctors != ''-1'',
                -- Если указаны доктора, то выводим только их
                iif(
                    eis.D_DCODE in (select * from GETINTEGERLIST(:list_doctors)),1,0
                )
            ,1)=1
            -- Если случай отправлен, то не показываем его
            and coalesce((select first 1 char_length(esl.error) from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc), 0) = 0
            and coalesce((select first 1 esl.send from its_eis_service_load esl where esl.serv_id = eis.serv_id order by esl.eslid desc),''F'') <> ''T''
        ----------------------------------------------------------------------------------------------------
        into TYPE_RES,SERV_ID,ID_DIAGNOS,DS1_T,ID_ST,ID_T,ID_N,ID_M,MTSTZ,SOD,K_FR,WEI,HEI,BSA,ERROR,t_treatcode,d_dcode
        do begin
            suspend;
        end
    end
end',

'ITS_UNLOADING_DIRECTIONS_5       BEGIN
    /*
    ГО - ЕИС СТАЦИОНАР - Доп.данные по случаю файл *_ADD.DBF


    select
        SERV_ID,ID_OBJECT,OBJ_VALUE,ERROR,SDEPNAME,DNAME,ID_C_ZAB
    from ITS_UNLOADING_DIRECTIONS_5([BDATE],[FDATE],trim(''('' from trim('')'' from ''[sdoc_dcode]'' )), trim(''('' from trim('')'' from ''[stat_departments_sdepid]'')))

    */
    FOR select distinct
        eis.SERV_ID
        ,eis.ID_OBJECT
        ,eis.OBJ_VALUE
        ,eis.ERROR
        ,eis.SDEPNAME
        ,eis.DNAME
        ,eis.ID_C_ZAB
    from (select
            cast(sd.sdid as numeric(11,0)) SERV_ID
            ,cast(28 as numeric(4,0)) id_object
            ,cast(d.rekvint1 as char(10)) obj_value
            ,cast('''' as char(200)) error
            -- info --
            ,sdp.sdepname sdepname
            ,doc.dname dname
            ,cast(dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
        from stat_direction sd
        join stat_jornal sj on sd.sdid = sj.dirid
        left join stat_departments sdp on sdp.SDEPID=sd.STAT_DEP
        left join stat_doctor s_doct on sd.sdid = s_doct.dirid
        left join doctdepartlinks  ddl on ddl.dcode = s_doct.dcode and ddl.depnum = sj.depnum_last -- код лечащего врача в госпитализации
        left join doctor doc on ddl.dcode=doc.dcode
        left join treatplace tpl on tpl.dirid=sd.sdid and tpl.placeid=10000556
        left join paramsinfo psi on psi.protocolid=tpl.protocolid and psi.codeparams=10019888 and psi.VER_NO = 0
        left join dicinfo d on d.dicid=psi.valueint  --протоколе "Выписной эпикриз ОМР"(10000556), значение параметра "Шкала реабилитационной маршрутизации" (DICINFO.REFID = 10014203, передаем значение из REKVINT1)
        join clreferrals clr on sd.sdid = clr.dirid and clr.fstatic = 1  -- вкладка "Услуги" в стационаре
        left join jpersons jp on clr.fromjid = jp.jid  -- направлен откуда
        left join recpropvalues rpv3 on rpv3.recpropid = 10000021 and rpv3.recid = jp.jid --признак ЛПУ "не включать в выгрузку
        -- Для ID_C_ZAB
        left join accident ac on sd.acdid = ac.acdid
        left join diagclients dc on ac.acdid = dc.acdid and dc.objtype = 104
        left join dicinfo dic_c_zab on dc.DIAGDESCTYPE = dic_c_zab.dicid and dic_c_zab.refid = -8 -- характер заболевания
        ---
    where
        cast(sd.planend as date) between :bdate and :fdate and coalesce(sd.enddate_not_present,0) = 0
        and coalesce(rpv3.propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
        and sd.dstatus in (4,7) -- пациент выписан
        --?? and sd.stat_dep = 10000001 -- только отделение ОМР
        and sd.STAT_DEP in (select * from GETINTEGERLIST(:stat_departments_sdepid))
        and iif(
            (select * from GETINTEGERLIST(:sdoc_dcode))=(-1),
            s_doct.dcode in (
                select distinct sdoc.dcode id
                from stat_doctor sdoc
                inner join
                (select sdoc1.dirid dirid,  max(sdoc1.sdocid) maxid
                from stat_doctor sdoc1
                group by sdoc1.dirid) sdoc1 on sdoc.sdocid = sdoc1.maxid
            ),
            s_doct.dcode in (select * from GETINTEGERLIST(:sdoc_dcode))
        )
    ) as eis
    where
        COALESCE(
            (SELECT FIRST 1 CHAR_LENGTH(ESL.ERROR)
            FROM ITS_EIS_SERVICE_LOAD ESL
            WHERE ESL.SERV_ID = EIS.SERV_ID
            ORDER BY ESL.ESLID DESC), 0) = 0
        AND COALESCE(
            (SELECT FIRST 1 ESL.SEND
            FROM ITS_EIS_SERVICE_LOAD ESL
            WHERE ESL.SERV_ID = EIS.SERV_ID
            ORDER BY ESL.ESLID DESC),''F'') <> ''T''
    into SERV_ID,ID_OBJECT,OBJ_VALUE,ERROR,SDEPNAME,DNAME,ID_C_ZAB
    do begin
        suspend;
        IF(ID_C_ZAB=2831)THEN
        BEGIN
            ID_OBJECT = 6;
            OBJ_VALUE = 1;
            suspend;
        END
    end
end',
'ITS_UNLOADING_DIRECTIONS_6       BEGIN
    /*

    ГО - ЕИС СТАЦИОНАР - Выгрузка направлений *_D.DBF

    select
        SERV_ID,ID_IN_CASE,D_NUMBER,DATE_ISSUE,DATE_PLANG,ID_LPU_F,ID_LPU_T,ID_D_TYPE,ID_D_GROUP,ID_PRVS,ID_OB_TYPE,ID_PRMP,ID_B_PROF,ID_DN,ID_GOAL,ID_DIAGNOS,ID_LPU_RF,ID_LPU_TO,ID_NMKL,ERROR,SDEPNAME,DNAME,ID_C_ZAB
    from ITS_UNLOADING_DIRECTIONS_6([BDATE],[FDATE],trim(''('' from trim('')'' from ''[sdoc_dcode]'' )), trim(''('' from trim('')'' from ''[stat_departments_sdepid]'')))
    */
    FOR select distinct
        eis.SERV_ID
        ,eis.ID_IN_CASE
        ,eis.D_NUMBER
        ,eis.DATE_ISSUE
        ,eis.DATE_PLANG
        ,eis.ID_LPU_F
        ,eis.ID_LPU_T
        ,eis.ID_D_TYPE
        ,eis.ID_D_GROUP
        ,eis.ID_PRVS
        ,eis.ID_OB_TYPE
        ,eis.ID_PRMP
        ,eis.ID_B_PROF
        ,eis.ID_DN
        ,eis.ID_GOAL
        ,eis.ID_DIAGNOS
        ,eis.ID_LPU_RF
        ,eis.ID_LPU_TO
        ,eis.ID_NMKL
        ,eis.ERROR
        ,eis.SDEPNAME
        ,eis.DNAME
        ,eis.ID_C_ZAB
    from
    (
        select distinct
            cast(sd.sdid as numeric(11,0)) SERV_ID,
            cast(1 as numeric(3,0)) ID_IN_CASE,
            cast(coalesce(left(clr_in.extrefid,20),clr_in.refid) as char(20)) D_NUMBER,
            cast(coalesce(clr_in.extrefdate,clr_in.treatdate) as date) DATE_ISSUE,
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
            cast(null as char(200)) ERROR,
            -- info --
            sdp.sdepname sdepname
            ,doc.dname dname
            ,cast(dic_c_zab.rekvint2 as numeric(11,0)) ID_C_ZAB
        from  stat_direction sd
        left join stat_departments sdp on sdp.SDEPID=sd.STAT_DEP
        join stat_jornal sj on sd.sdid = sj.dirid
        left join departments dp on sj.depnum_last = dp.depnum
        join clients cl on cl.pcode = sd.pcode
        left join cl_get_profileinfo (sd.pcode) c on 1=1
        left join getage(c.bdate, cast(sd.planstart as date)) ga on 1=1
        join clhistnum ch on sd.histid = ch.histid
        join jpagreement jpa on sd.agrid = jpa.agrid and jpa.agrtype = 1  -- омс
        join clreferrals clr on sd.sdid = clr.dirid and clr.fstatic = 1  -- вкладка "Услуги" в стационаре
        join clrefdet cld on clr.refid = cld.refid
        join wschema w on w.schid = cld.schid
        join accident ac on sd.acdid = ac.acdid
        join diagnosis diag on ac.finaldiag = diag.dgcode
        left join diagclients dc    on ac.acdid = dc.acdid and dc.objtype = 104
        left join stat_doctor s_doct  on sd.sdid = s_doct.dirid
        left join doctdepartlinks  ddl   on ddl.dcode = s_doct.dcode and ddl.depnum = sj.depnum_last -- код лечащего врача в госпитализации
        left join doctor doc on ddl.dcode=doc.dcode
        left join dicinfo dvmp on dvmp.refid = -10002 and dvmp.dicid = ddl.medcaretype   -- ID_VMP  справочник V008 Классификатор видов МП [-10002])
        join clreferrals clr_in on clr_in.refid = sd.refid -- направление на госпитализацию
        --join dicinfo dic_goal on clr.goal = dic_goal.dicid and dic_goal.refid = 48 -- цель направления
        left join dicinfo dicr on dicr.refid = 777003 and dicr.rekvint1 = clr.reftype  -- ORDER -- порядок направления
        left join dicinfo dpay on dpay.refid = -10004 and dpay.rekvint4 = 1  -- ID_SP_PAY  -- по умолчанию
        left join jpersons jp on clr.fromjid = jp.jid  -- направлен откуда
        left join recpropvalues rpv on rpv.recpropid = 10000015 and rpv.recid = jp.jid -- ID_LPU_F - доп параметр в справочнике юр лиц
        left join recpropvalues rpv2 on rpv2.recpropid = 10000004 and rpv2.recid = w.schid -- IDVIDVME - доп. параметр в прейскуранте
        left join diagnosis dg on clr.dgcode = dg.dgcode  -- диагноз направления
        left join recpropvalues rpv3 on rpv3.recpropid = 10000021 and rpv3.recid = jp.jid --признак ЛПУ "не включать в выгрузку
        left join dicinfo dic_prvs on ddl.idmsp = dic_prvs.dicid and dic_prvs.refid = -10000  -- специальность в отделении = id_prvs
        left join dicinfo dic_c_zab on dc.DIAGDESCTYPE = dic_c_zab.dicid and dic_c_zab.refid = -8 -- характер заболевания
        left join dicinfo dic_prmp on dp.idpr = dic_prmp.dicid and dic_prmp.refid = -10005  -- профиль мед. помощи
        left join stat_dep_history sdh on sd.sdid = sdh.dirid
        left join dicinfo dic_b_prof on sdh.bedprofileid = dic_b_prof.dicid and dic_b_prof.refid = -71000002  --  профиль койки
        where
            cast(sd.planend as date) between :bdate and :fdate and coalesce(sd.enddate_not_present,0) = 0
            and coalesce(rpv3.propvalueint,0) = 0  --признак ЛПУ "не включать в выгрузку"
            and sd.dstatus in (4,7) -- пациент выписан
            --?? and sd.stat_dep <> 10000002 -- не отдеелние "Дневной стационар" (это онкология)
            and sd.STAT_DEP in (select * from GETINTEGERLIST(:stat_departments_sdepid))
            and iif(
                (select * from GETINTEGERLIST(:sdoc_dcode))=(-1),
                s_doct.dcode in (
                    select distinct sdoc.dcode id
                    from stat_doctor sdoc
                    inner join
                    (select sdoc1.dirid dirid,  max(sdoc1.sdocid) maxid
                    from stat_doctor sdoc1
                    group by sdoc1.dirid) sdoc1 on sdoc.sdocid = sdoc1.maxid
                ),
                s_doct.dcode in (select * from GETINTEGERLIST(:sdoc_dcode))
            )
            --?? group by cl.lastname, cl.pcode, cl.firstname, cl.midname, c.bdate, c.sex, ch.nspser, ch.nspnum, w.kodoper, w.schid, c.ageinyears, ga.ageinyears,sd.planstart, sd.planend, diag.mkbcode, c.doctypeid, c.paspser, c.paspnum, sd.sdid, dic_prvs.rekvint2, sj.stathistnum, dic_prmp.rekvint2, dvmp.rekvint3, ddl.extcode, dp.speccode, rpv2.propvalueint, sdp.sdepname,doc.dname,dic_b_prof.rekvint1, dic_c_zab.rekvint2, clr_in.extrefid, clr_in.refid, clr_in.extrefdate,clr_in.treatdate, rpv.propvalueint
        )  as eis
        where
            coalesce((
                select first 1 char_length(esl.error)
                from its_eis_service_load esl
                where esl.serv_id = eis.serv_id
                order by esl.eslid desc), 0) = 0
            and coalesce((
                select first 1 esl.send
                from its_eis_service_load esl
                where esl.serv_id = eis.serv_id
                order by esl.eslid desc),''F'') <> ''T''
    into SERV_ID,ID_IN_CASE,D_NUMBER,DATE_ISSUE,DATE_PLANG,ID_LPU_F,ID_LPU_T,ID_D_TYPE,ID_D_GROUP,ID_PRVS,ID_OB_TYPE,ID_PRMP,ID_B_PROF,ID_DN,ID_GOAL,ID_DIAGNOS,ID_LPU_RF,ID_LPU_TO,ID_NMKL,ERROR,SDEPNAME,DNAME,ID_C_ZAB
    do BEGIN
        suspend;
        IF(ID_C_ZAB=2831)THEN
        BEGIN
            ID_IN_CASE=null;
            D_NUMBER=null;
            DATE_ISSUE=(
                select sd.PLANSTART
                from STAT_DIRECTION sd
                where sd.SDID=:SERV_ID
            );
            DATE_PLANG=null;
            ID_LPU_F=null;
            ID_LPU_T=null;
            ID_D_TYPE=38;
            ID_D_GROUP=20;
            ID_PRVS=null;
            ID_OB_TYPE=null;
            ID_PRMP=null;
            ID_B_PROF=null;
            ID_DN=null;
            ID_GOAL=null;
            ID_DIAGNOS=null;
            ID_LPU_RF=null;
            ID_LPU_TO=null;
            ID_NMKL=null;
            ERROR=null;
            suspend;
        END
    end
end'