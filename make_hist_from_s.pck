CREATE OR REPLACE PACKAGE PUB_DEV.make_hist_from_s
AUTHID CURRENT_USER
IS

-- 20.12.2016 Паршуков С.В. первая версия пакета
-- 13/01/2017 Паршуков С.В. модифицированы настроечные таблицы
-- 04/02/2017 Паршуков С.В. добавил интеллекта по управлению "Н" таблицей и контролем результата 
-- исправлющих прошу придерживаться "принятые сокращения и соглашения", описанные в спецификации пакета


/*   Пакет создает из снимочных таблиц исторические
  
  ---------- настроечные параметры:---------------------------------------------------
    1) d_mkhst_columns_sets  
                            - индивидуальные параметры архивирования для колонок таблицы
                              заполняется пользователем через метод s102_CreateColumnSetRules
                              по умолчанию, сохраняется каждое новое значение поля.
                              обязательно проверить полученный набор
    2) d_mkhst_columns_method 
                            - справочник методов сохранения в истории. 
                              используется данным пакетом. расширяется через РРМ на доработку логики пакета
    3) d_mkhst_columns_def  
                            - подсказки от пользователя к выбору метода для колонки
                              используется пользлвателем как подсказка для метода s102_CreateColumnSetRules при выборе
                              метода сохранения
    4) m_mkhst_tables
                            - основная таблица указывающая что сохранять, куда сохранять, на основе какого набора правил
                              перед заполнение обязательно создать целевую историческую таблицу.
                              можно сделать путем вызова s100_CreateTargetTable.
    
  --------- типичный запуск на добавление нового снимка в конец истории------------------
    AddSnap2End (ID филиала, исходная таблица, номер набора, добавляемый день)
    
  --------- принятые сокращения и соглашения ---------------------------------------------
    in_   префикс переменных, передаваемых как параметры в функции
    p_    тоже самое
    o_    префикс переменных, передаваемых параметром в функцию через который она возвращает результат работы
    l_    префикс локальных переменных внутри ф-ции
          или префикс названий ф-ций, специфичных для пакета
    gpv_  Global Package Variable (глобальная переменная пакета - видима снаружи - в спецификации)
    gpc_  Global Package Variable (глобальная константа пакета - видима снаружи - в спецификации)
    lpv_  Local  Package Variable (глобальная переменная пакета - не видима снаружи)
  */

-- результаты работы ф-ций
  res_error         constant number := -1;
  res_success       constant number := 1;

--режимы работы вспомогательных ф-ций S_xxx 
  gpc_OnlyCheck     constant number      :=0;
  gpc_CheckAndAlter constant number      :=1;

-- режимы работы ф-ции инкапсулируюшей EXECUTE IMMEDIATE (битовая маска)
  gpc_Exec          constant number      :=1; -- 0001
  gpc_Log           constant number      :=2; -- 0010
  gpc_Print         constant number      :=4; -- 0100
  gpv_runMode                number      :=gpc_Exec; -- для целей отладки перед запуском процедур 
                                                     -- можно установить gpv_runMode:=gpv_Exec+gpv_Log

-- настроечные параметры можно использовать для подстройки чувствительности алгоритма к обрабатываемой таблице
  gpv_snapLimitCount             number      :=1000; -- количество записей в снимке, меньше которого считаем снимок тестовым и не обрабатываем
  gpv_ParallelCount              number      := 5;   -- кол-во параллелей на которое будет исполняться запросы

-----------------------------------------------------------------------------------------------------
-- вспомогательные ф-ции по обслуживанию настроечных таблиц и независимых проверок
function s100_CreateTargetTable(p_cset_id number, p_source_name varchar2, p_target_name varchar2 default null, o_sql out nocopy clob, p_mode number default 1) return number;
function s101_CheckAbility2use    (p_cset_id     number,   p_source_name varchar2, o_res out nocopy clob, p_target_name varchar2 default null, p_mode number default 0, p_filial_id number default 0) return number;
function s102_CreateColumnSetRules(p_source_name varchar2, o_clset_id out number) return number;

-----------------------------------------------------------------------------------------------------
-- основные рабочие ф-ции вызываемые планировщиком для ведения истории
--Procedure AddSnap2start(in_filial_id IN NUMBER default 0, in_snap_date IN VARCHAR2, in_scr_table varchar2);
--Procedure AddSnapInsert(in_filial_id IN NUMBER default 0, in_snap_date IN VARCHAR2, in_scr_table varchar2);
Procedure AddSnap2End(in_filial_id IN NUMBER, in_scr_table varchar2, in_snap_date IN VARCHAR2, in_clset_id number default null);


-----------------------------------------------------------------------------------------------------
-- внутренние вспомогательные ф-ции - можно убрать внутрь пакета после отладки и перевода в пром
function l_getlong( p_tname in varchar2, p_cname in varchar2, p_where in varchar2 ) return varchar2;
function l_getLastPartition(p_source_tab varchar2, p_filial_id number, o_res out varchar) return number;
function add_partition(p_part_table in varchar2, p_filial_id number) return number;
function checkData(p_mode number, 
	p_h_table varchar2, p_h_subpartname varchar2, p_clset_id number, p_h_date date) return number;
  
END;
/
CREATE OR REPLACE PACKAGE BODY PUB_DEV.make_hist_from_s IS

-- 20.12.2016 Паршуков С.В. первая версия пакета
-- 13/01/2017 Паршуков С.В. модифицированы настроечные таблицы
-- 04/02/2017 Паршуков С.В. добавил интеллекта по управлению "Н" таблицей и контролем результата 
-- исправлющих прошу придерживаться "принятые сокращения и соглашения", описанные в спецификации пакета

  crlf              constant varchar2(2) := chr(13)||chr(10);
  
  -- Режим в котором вызвана ф-ция "checkData" - сбор данных или проверка
  lpc_collectData   constant number := 1;
  lpc_checkData     constant number := 2;

  -- режимы сбора конрольных данных ф-цией "checkData"
  lpc_ByEnd         constant number := 1;
  lpc_ByStart       constant number := 2;
  lpc_ByDate        constant number := 3;
  
  -- результат работы ф-ции проверки данных
  lpv_CollectResult number := 0; -- 0=не проводился сбор lpv_chk% переменных
                                 -- 1=проводился, но не смогли определить lpv_chk_sumfld, поэтому без суммы
                                 -- 2=проводился, все успешно

  -- глобальные переменные в которые ф-ция проверки помещает контрольные значения
  lpv_chk_mode   number := lpc_ByEnd;-- режим, в котором проведут сбор контрольной информации
  lpv_chk_date   date;        -- дата за которую соберут контрольную информацию перед обновлением новой порцией расчета
  lpv_chk_maxd   date;        -- максимальная дата в снимке (h_start_date)
  lpv_chk_cnt    number;      -- кол-во записей в снимке
  lpv_chk_sum    number;      -- сумма по полю - которое выберет алгоритм 
  lpv_chk_sumfld varchar2(20);-- название поля, по которому собрана сумма выше
  

--=========================================================================================================
-- вспомогательная универсальная процедура - кандидат на вынос в UTILS пакет
-- число в строку с указанной точностью, с дефолтными разделителями и без лидирующих побелов
function num2ch(p_number number, p_scale number default null) return varchar2
is
begin
	return trim(case when p_scale is null then to_char(p_number,'999,999,999,999')
                   else to_char(p_number,'999,999,999,990.'||lpad('0',p_scale,'0')) end);
end;

-------------------------------------------------------------------------------------------------------
function checkData(p_mode number, 
	p_h_table varchar2, p_h_subpartname varchar2, p_clset_id number, p_h_date date) return number
is
  l_sumfld varchar2(50);
  l_svmtd number;
  l_clmtp number;
  l_nable  varchar2(10);
  l_numdist number;
  l_numnulls number; 
  
  l_chk_maxd date;
  l_chk_cnt  number;
  l_chk_sum  number;
  
begin 
  -- -- фунцкия вызвана в режиме "собираем данные"
  if p_mode = lpc_collectData then 
    lpv_CollectResult := 0; 
    lpv_chk_date := null; lpv_chk_maxd := null; lpv_chk_cnt:=null; lpv_chk_sum:=null; lpv_chk_sumfld := null;
    -- если дата сборки пришлась на конец месяца пытаем найти поле с методом(2)
    if trunc(p_h_date)=last_day(trunc(p_h_date)) then
      begin
        select column_name, svmtd_svmtd_id, clmtp_clmtp_id 
          into l_sumfld, l_svmtd, l_clmtp
        from d_mkhst_col_sets 
        where clset_id = p_clset_id 
          and svmtd_svmtd_id=2 and rownum<=1;
        
        select nullable, num_distinct, num_nulls
       into l_nable, l_numdist, l_numnulls
        from all_tab_columns a
       where 1=1
         and a.column_name = upper(l_sumfld)
         and a.table_name = upper(p_h_table)
         and a.OWNER = user
         and a.data_type = 'NUMBER';
        
      exception
        when NO_DATA_FOUND then null; -- поищем стандартным путем ниже
      end;
    end if; -- двигаемся по пути поиска поля с наибольшей вариативностью и заполненостью в Н_талице

    -- стандартный путь поиска поля для суммирования
    if l_sumfld is null then
     begin
       select * 
         into l_svmtd, l_clmtp, l_sumfld, l_nable, l_numdist, l_numnulls
       from (
       select svmtd_svmtd_id, d.clmtp_clmtp_id,  a.column_name, nullable, num_distinct, num_nulls
        from d_mkhst_col_sets d, all_tab_columns a
        where d.clset_id = p_clset_id and d.svmtd_svmtd_id in (1) and p_h_date between d.start_date and d.end_date
          and a.column_name= upper(d.column_name)
          and a.table_name = upper(p_h_table)
          and a.OWNER = user
          and a.data_type = 'NUMBER'
        order by num_nulls, density asc)
        where rownum<=1;
      exception
        when NO_DATA_FOUND then null; -- пичаль. нет полей кандидатов на контроль через суммирование
      end;
    end if;
   
    lpv_chk_date := p_h_date;
    if l_sumfld is not null then 
      lpv_chk_sumfld := l_sumfld;
      job_log_psv.write_detail_log('Выбрано для контроля '||lpv_chk_sumfld||' поле у которого l_svmtd='||l_svmtd||', l_clmtp='||l_clmtp||', nullable='||l_nable||', num distinct ='||l_numdist||', num nulls='||l_numnulls); 
               
      if lpv_chk_mode=lpc_ByEnd then 
        execute immediate 'select max(h_start_date), count(1), sum('||lpv_chk_sumfld||')
                          from '||p_h_table||' subpartition ('||p_h_subpartname||')
                          where :d between h_start_date and h_end_date'
        into lpv_chk_maxd, lpv_chk_cnt, lpv_chk_sum
        using lpv_chk_date;
      end if;
      lpv_CollectResult := 2;
    else
      job_log_psv.write_detail_log('не смогли найти поле для выполнения контроля суммирования'); 
        execute immediate 'select max(h_start_date), count(1)
                          from '||p_h_table||' subpartition ('||p_h_subpartname||')
                          where :d between h_start_date and h_end_date'
        into lpv_chk_maxd, lpv_chk_cnt
        using lpv_chk_date;
      lpv_CollectResult := 1;
   end if;
   job_log_psv.write_detail_log('сохранили для проверки: на дату='||to_char(lpv_chk_date)
                             ||', макс h_start_date='||to_char(lpv_chk_maxd)
                             ||', кол-во записей='||num2ch(lpv_chk_cnt)
                             ||', сумма (по '||l_sumfld||')='||to_char(lpv_chk_sum)); 
   return res_success;
    
  -- фунцкия вызвана в режиме проверки результат
	elsif p_mode=lpc_checkData then 
 	  if lpv_CollectResult=0 then
      job_log_psv.write_detail_log('Вызвана проверка результата без предварительного сбора контрольных цифр'); 
      return res_error;
 		end if;
    
    if lpv_chk_sumfld is not null then 
      if lpv_chk_mode=lpc_ByEnd then 
        execute immediate 'select max(h_start_date), count(1), sum('||lpv_chk_sumfld||')
                          from '||p_h_table||' subpartition ('||p_h_subpartname||')
                          where :d between h_start_date and h_end_date'
        into lpv_chk_maxd, lpv_chk_cnt, lpv_chk_sum
        using lpv_chk_date;
      end if;
      if l_chk_maxd<>lpv_chk_maxd or l_chk_cnt<>lpv_chk_cnt or l_chk_sum<>lpv_chk_sum then
        job_log_psv.write_detail_log('не совпало: '
                         ||case when l_chk_maxd<>lpv_chk_maxd then ' стала дата ('||to_char(l_chk_maxd)||')<> была дата('||to_date(lpv_chk_maxd)||')'
                                when l_chk_cnt<>lpv_chk_cnt  then ' стало записей ('||l_chk_cnt||')<> было записей('||lpv_chk_cnt||')'
                                when  l_chk_sum<>lpv_chk_sum then ' стала сумма ('||l_chk_sum||')<> была сумма('||lpv_chk_sum||')'
                           else null end
                         ); 
        return res_error;
      end if;
    else
        execute immediate 'select max(h_start_date), count(1)
                          from '||p_h_table||' subpartition ('||p_h_subpartname||')
                          where :d between h_start_date and h_end_date'
        into lpv_chk_maxd, lpv_chk_cnt
        using lpv_chk_date;
      if l_chk_maxd<>lpv_chk_maxd or l_chk_cnt<>lpv_chk_cnt then
        job_log_psv.write_detail_log('не совпало: '
                         ||case when l_chk_maxd<>lpv_chk_maxd then ' стала дата ('||to_char(l_chk_maxd)||')<> была дата('||to_date(lpv_chk_maxd)||')'
                                when l_chk_cnt<>lpv_chk_cnt  then ' стало записей ('||l_chk_cnt||')<> было записей('||lpv_chk_cnt||')'
                           else null end
                         ); 
        return res_error;
      end if;
    end if;
    
    return res_success;
    
  -- фунцкия вызвана в неизвестном режиме - видимо руками что-то не то указали
	else
    job_log_psv.write_detail_log('неподдерживаемый режим работы'); 
    return res_error;
	end if;
end;

----------------------------------------------------------------------------------------------------
-- вспомогательная универсальная процедура - кандидат на вынос в UTILS пакет
-- разбирает переданную строку "схема.таблица@"дблинк" на три отдельных строки 
procedure decodeTabName(p_tab in varchar2, po_owner out varchar2, po_table out varchar2, po_link out varchar2)
is
  pos integer;
  l_tab varchar2(250) := upper(p_tab);
begin
  pos := instr(l_tab,'@');
  if pos>0 then 
    po_link := substr(l_tab, pos);
    l_tab   := substr(l_tab, 1, pos-1);
  end if;
  po_owner := upper(nvl(substr(l_tab,1,instr(l_tab,'.')-1),user)); 
  po_table := upper(substr(l_tab,case when instr(l_tab,'.')=0 then 1 else instr(l_tab,'.')+1 end, 
                                 case when instr(l_tab,'@')=0 then length(l_tab) else instr(l_tab,'@')-instr(l_tab,'.')-1 end ));
  if po_owner is null then
    if po_link is null then 
      po_owner:=user;
    else 
      begin
        select username into po_owner from user_db_links where db_link = substr(po_link,2);
      exception when others then
          begin
            select username into po_owner from user_db_links where db_link like substr(po_link,2)||'%';
          exception when others then
            dbms_output.put_line('видимо кривой дб-линк '||po_link);
          end;
      end;
    end if;
  end if;
end;

----------------------------------------------------------------------------------------------------
-- вспомогательная универсальная процедура - кандидат на вынос в UTILS пакет
-- убеждается в существовании объекта в базе. 
-- нужно смотреть именно в ALL_OBJECTS, т.к. видимость объекта должна определяться правами доступа спрашивающего
function IfExists(p_owner varchar2, p_obj_name varchar2, p_dblink varchar2 default null) return number
is
  l_cnt number;
begin
  if p_dblink is null then 
    select count(1) into l_cnt from all_objects where owner=upper(p_owner) and object_name=upper(p_obj_name);
  else
    begin
      execute immediate 'select count(1) from all_objects'||p_dblink||' where owner=upper(:p_owner) and object_name=upper(:p_obj_name)' 
                      into l_cnt using p_owner, p_obj_name;
    exception 
      when others then 
        job_log_psv.write_detail_log(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace());
        l_cnt :=0;
    end;  
  end if;
  return l_cnt;
end;

--------------------------------------------------------------------------------
-- вспомогательная универсальная процедура - кандидат на вынос в UTILS пакет
-- убеждается в существовании объекта в базе. 
-- аналогично выше только без заморочек с раздельным указанием схемы\объекта\линка
function IfExists(p_obj_name varchar2) return number
is
  l_owner varchar2(100) ;--:= nvl(upper(substr(p_obj_name,1,instr(p_obj_name,'.')-1)),user);
  l_obj   varchar2(100) ;--:= upper(substr(p_obj_name,instr(p_obj_name,'.')+1));
  l_link  varchar2(100) ;
begin
  decodeTabName(p_obj_name, l_owner,l_obj,l_link);
  return ifExists(l_owner,l_obj, l_link);
end;



----------------------------------------------------------------------------------------------------
-- вспомогательная универсальная процедура - кандидат на вынос в UTILS пакет
-- чтобы не взрывать мозг подсчетом колва апострофов при формировании строки динамического запроса
function dat2str(p_dat in date) return varchar2
is
begin
  if p_dat is null then 
    return 'null';
  else
    return  'to_date('''||to_char(p_dat,'dd.mm.yyyy hh24:mi:ss')||''',''dd.mm.yyyy hh24:mi:ss'')' ;
  end if;
end;



----------------------------------------------------------------------------------------------------
--вспомогательная универсальная процедура - кандидат на вынос в UTILS пакет
-- устаревший тип LONG не поддерживает преобразование типов. Поэтому приходится конвертировать в строку
-- далее по обстоятества - можно уже как-то обработать\сравнить
function l_getlong( p_tname in varchar2, p_cname in varchar2, p_where in varchar2 ) return varchar2
    as
        l_cursor    integer default dbms_sql.open_cursor;
--        l_n         number;
        l_long_val  varchar2(4000);
        l_long_len  number;
        l_buflen    number := 4000;
       l_curpos    number := 0;
begin
  dbms_sql.parse( l_cursor,
                  'select ' || p_cname || ' from ' || p_tname || ' where '|| p_where,
                  dbms_sql.native );
--       dbms_sql.bind_variable( l_cursor, ':x', p_rowid );
   
  dbms_sql.define_column_long(l_cursor, 1);
  l_long_len := dbms_sql.execute(l_cursor);
   
  if (dbms_sql.fetch_rows(l_cursor)>0)
  then
     dbms_sql.column_value_long(l_cursor, 1, l_buflen, l_curpos , l_long_val, l_long_len );
  end if;
  dbms_sql.close_cursor(l_cursor);
  return l_long_val;
end;

-------------------------------------------------------------------------------------
--вспомогательная универсальная процедура - кандидат на вынос в UTILS пакет
-- для партиционированных (без субпартиций) только по RANGE по Датам вычисляет "расстояние" между последними двумя партициям
-- и делает SPLIT для крайней партиции. Называет новую партицию по шаблону "Имя таблицы" + "_" + "yyyymm"
function add_partition(p_part_table in varchar2, p_filial_id number) return number
is
  l_tab varchar2(100):= upper(trim(p_part_table)); -- 'SUBS_BILLS';
  l_err varchar2(2000);
  
  l_t   varchar2(100);
  l_st  varchar2(100);
  l_pc  number;
  l_lpname varchar2(100);
  
  l_pt3 varchar2(1000);
  l_pt2 varchar2(1000);
  l_delta number;
  l_newpoint varchar2(1000);
  l_suffix    varchar2(100);

begin
  begin
  select PARTITIONING_TYPE, SUBPARTITIONING_TYPE, PARTITION_COUNT  
    into l_t, l_st, l_pc
    from USER_PART_TABLES where table_name=l_tab and rownum<=1;
  exception
    when others then
       job_log_psv.write_detail_log(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace());
       return -1; -- не партиционированая таблица
  end;
 
  if l_t<>'LIST' then 
    job_log_psv.write_detail_log('не поддерживаемый тип партиционирования - '||l_t||' предполагаем партиционирование по филиалам методом LIST');
    return -2;
  end if;
  
  if l_st<>'RANGE' then 
    job_log_psv.write_detail_log('субпартиционирование по датам не методом RANGE - '||l_st ||'. выходим');
    return -3;
  end if;

  if l_pc<3 then 
    job_log_psv.write_detail_log('слишком малое кол-во партиций('||l_pc||'), чтобы вычислить дельту в диапазоне');
    return -4;
  end if;

  -- вычисляем название последней субпартиции в этом филиале (пригодится потом в Alter)
  if l_getLastPartition(l_tab, p_filial_id , l_lpname)<> res_success then 
		job_log_psv.write_detail_log('не смог вычислить крайнюю субпартицию в филиале '||p_filial_id);
    return -5;
	end if;
  
  -- вычисляем номер предпоследней субпартиции (по ним потом попросим High_Value) 
  select max(SUBPARTITION_POSITION)
  into l_delta
  from user_tab_subpartitions 
  where 1=1
     and table_name = upper(p_part_table)
     and partition_name= (select partition_name from user_tab_subpartitions 
                          where table_name = upper(p_part_table) and subpartition_name=l_lpname)
     and subpartition_name <> l_lpname;
  
  -- просим High_Value (третья с конца)
  l_pt3 := l_getlong('user_tab_subpartitions', 'HIGH_VALUE', 'table_name ='''||l_tab||''' and SUBPARTITION_POSITION = '||to_char(l_delta-1));
  -- просим High_Value (вторая с конца)
  l_pt2 := l_getlong('user_tab_subpartitions', 'HIGH_VALUE', 'table_name ='''||l_tab||''' and SUBPARTITION_POSITION = '||to_char(l_delta-0));
  
  --------------------------------------------------
  if upper(trim(l_pt3)) like 'TO_DATE%' then -- если субпартиционирование по датам
    l_err := 'select MONTHS_BETWEEN('||l_pt2||','||l_pt3||') from dual';
  else                                       -- если субпартиционирование по числам
    l_err := 'select '||l_pt2||'-'||l_pt3||' from dual';
  end if;
  begin
    execute immediate l_err into l_delta;
    job_log_psv.write_detail_log('расстояние между точками субпартиций HIGH_VALUE, l_delta='||l_delta);
  exception
    when others then 
      job_log_psv.write_detail_log(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace());
      job_log_psv.write_detail_log('не получилось вычислить дельту между ('||l_pt2||') and ('||l_pt3||') по выражению ('||l_err||')');
      return -6;
  end;
  
  --------------------------------------------------
  if upper(trim(l_pt2)) like 'TO_DATE%' then 
     l_err := 'select ''to_date(''''''||to_char(add_months('||l_pt2||','||l_delta||'),''dd.mm.yyyy'')||'''''',''''dd.mm.yyyy'''')'' from dual';
  else --number
     l_err := 'select '||l_pt2||'+'||l_delta||' from dual';
  end if;
  begin
    execute immediate l_err into l_newpoint;
  exception
    when others then 
      job_log_psv.write_detail_log(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace());
      job_log_psv.write_detail_log('не получилось новое значение l_newpoint по выражению ('||l_err||')');
      return -7;
   end;

  --------------------------------------------------
  -- сочиняем название субпартиции по правилу P(YYYYMM)_(p_filial_id)
  if upper(trim(l_pt2)) like 'TO_DATE%' then 
    execute immediate 'select to_char('||l_pt2||',''yyyymm'') from dual' into l_suffix;
  else
    execute immediate 'select to_char('||l_pt2||') from dual' into l_suffix;
  end if;

  --------------------------------------------------
/*  dbms_output.put_line(l_tab||': type='||l_t||', subpart='||l_st||', count='||l_pc);
  dbms_output.put_line('l_pt3='||l_pt3);
  dbms_output.put_line('l_pt2='||l_pt2);
  dbms_output.put_line('l_delta='||l_delta);
  dbms_output.put_line('l_newpoint='||l_newpoint);*/
  
  l_err := 'alter table '||l_tab||' split subpartition '||l_lpname||
    ' at ('||l_newpoint||')'||
    ' into(subpartition P'||l_suffix||'_'||p_filial_id||', subpartition '||l_lpname||') UPDATE INDEXES';
    
  job_log_psv.write_detail_log('разделяем крайнюю субпартицию '||l_lpname||' по дате '||l_newpoint||' в две новых:P'||l_suffix||'_'||p_filial_id||','||l_lpname||' запросом='||l_err);
  
  execute immediate l_err;
  return 0;
  
exception 
  when others then
    job_log_psv.write_detail_log(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace());
    return sqlcode;
end;




-------------------------------------------------------------------------------------------------------
-- вернет имя последней субпартиции в p_source_tab по филиалу p_filial_id
function l_getLastPartition(p_source_tab varchar2, p_filial_id number, o_res out varchar) return number
is
  l_owner       varchar2(50); -- владелец p_source_tab
  l_source_tab  varchar2(50); -- таблица в которой ищем последнюю партицию ф указанном филиала p_filial_id
  l_link        varchar2(50); -- возможный линк из p_source_tab / временная переменная
  l_partField   varchar2(50); -- поле по которому фактически партиционирована таблица
  l_part_name   varchar2(50); -- имя партиции, которая содержит нужный филиала
  l_cnt         number;
begin
  -- раскладываем имя вход.таблицы на компоненты
  decodeTabName(p_source_tab, l_owner, l_source_tab, l_link);
	select count(1), max(PARTITIONING_TYPE) into l_cnt, l_link
    from all_part_tables 
   where owner=l_owner 
     and table_name = l_source_tab;
     
  if l_cnt=0 then 
		o_res := 'Ошибка: таблица не является партиционированной';
		return res_error;
  end if;
  if l_link<>'LIST' then 
		o_res := 'Ошибка: таблица партиционированной не по методу LIST';
		return res_error;
  end if;
  
  select column_name into l_partField from ALL_PART_KEY_COLUMNS  where owner=l_owner and name = l_source_tab;
  if l_partField <> 'FILIAL_ID' then 
		o_res := 'Ошибка: партиционированна не по полю FILIAL_ID';
		return res_error;
  end if;

  -- определяем партицию филиала
  select 
    partition_name into l_part_name
  from 
    all_tab_partitions tp,
    ALL_PART_KEY_COLUMNS  pc
  where 1=1
    and tp.table_owner=l_owner and tp.table_name = l_source_tab
    and tp.table_owner = pc.owner
    and tp.table_name = pc.name
    and pc.column_name = 'FILIAL_ID'
    and to_char(p_filial_id) = l_getlong( 'ALL_TAB_PARTITIONS', 'HIGH_VALUE', 
                            'TABLE_OWNER='''||tp.table_owner||''' 
                         and TABLE_NAME='''||tp.table_name||''' 
                         and partition_name='''||tp.partition_name||'''' );

  -- определяем имя последней субпартиции
  select max(SUBPARTITION_NAME) keep (dense_rank last order by subpartition_position) into o_res
  from all_tab_subpartitions 
  where 1=1
    and table_owner= l_owner
    and table_name = l_source_tab
    and partition_name = l_part_name;
  
  return res_success;
end;

-------------------------------------------------------------------------------------------------------
-- вернет что-то вроде varchar2(20) или number(10,2)
function l_getSize(p_type varchar, p_len number, p_precis number, p_scale number) return varchar2
is
begin
	return case when p_type like '%CHAR%' then '('||p_len||')' 
              when p_type='NUMBER' and p_precis is not null and p_scale is not null then '('||p_precis||','||p_scale||')' 
							when p_type='NUMBER' and p_precis is not null                         then '('||p_precis||')' 
         else null end;
end;

-------------------------------------------------------------------------------------
/*  
  gpv_Exec              constant number      :=1; -- 0001
  gpv_Log               constant number      :=2; -- 0010
  gpv_Print             constant number      :=4; -- 0100
*/  
procedure l_RunSql(p_sql clob, execMode number default gpv_runMode)
is
begin
	if bitAnd(execMode, gpc_Print)=gpc_Print  then
		dbms_output.put_line(p_sql);
	end if;
  if bitAnd(execMode, gpc_Log)=gpc_Log  then
		job_log_psv.write_detail_log('--------------------------------------------------');
		job_log_psv.write_detail_log(p_sql);
	end if;
  if bitAnd(execMode, gpc_Exec)=gpc_Exec  then
		execute immediate p_sql;
	end if;
exception
  when others then
    job_log_psv.write_detail_log(p_sql); --текст возможного SQL
		raise;		
end;

---------------------------------------------------------------------------------------------------------------
-- сохранение в таблицу логирования
procedure l_logResult(p_mkhst number, p_filial number, p_snapDate date, 
    p_clset number, p_target_tab varchar2, p_addMethod varchar2, p_exitCode number,
	  p_total number, p_unch number, p_new number, p_closed number, p_chng_cls number, p_chng_open number, 
    p_comm varchar2)
is
  l_cnt   number;
  l_eCode number;
begin
  job_log_psv.write_detail_log('логируем результаты');
  
  select count(1), max(exitCode)  
    into l_cnt, l_eCode
  from f_mkhst_results 
  where mkhst_mkhst_id = p_mkhst and filial_id = p_filial and snapdate=p_snapDate;
  
  if (l_cnt>0 and l_eCode>0 and p_exitCode<0) then 
    job_log_psv.write_detail_log('Уже есть положительный результат, поэтому пришедшую ошибку записывать в f_mkhst_results не буду');
  else
    merge into f_mkhst_results f
    using ( select p_mkhst m, p_filial f , p_snapDate d from dual) s
    on (f.mkhst_mkhst_id = s.m and 
        f.filial_id      = s.f and
        nvl(f.snapdate,to_date('01.01.2000','dd.mm.yyyy')) = nvl(s.d,to_date('01.01.2000','dd.mm.yyyy')) 
       )
    when matched then update set
       f.addMethod  = p_addMethod
      ,f.exitCode   = p_exitCode  
      ,f.total_reload=p_total 
      ,f.unchanged  = p_unch
      ,f.newly      = p_new
      ,f.closed     = p_closed
      ,f.chng_closed= p_chng_cls
      ,f.chng_opened= p_chng_open    
      ,f.comm       = substr(p_comm, 1, 2000)
    when not matched then 
         insert (mkhst_mkhst_id, filial_id, snapDate  , addMethod  , exitCode        
                ,total_reload  ,unchanged  ,newly    ,closed   ,chng_closed   ,chng_opened      
                ,comm )
         values (p_mkhst       , p_filial , p_snapDate, p_addMethod, p_exitCode
                ,p_total       ,p_unch     ,p_new    ,p_closed ,p_chng_cls    ,p_chng_open
                ,p_comm);
    commit;
  end if;
  
  if p_exitCode > 0 then
    delete from f_mkhst_high_changed where clset_clset_id=p_clset and filial_id = p_filial and snapDate = p_snapDate;
    execute immediate 'insert into f_mkhst_high_changed 
              select :clset, :filial, :snap, t2.column_value, count(1)
              from '||p_target_tab||'_tmp t,
                   table(cast(str2tbl(t.mostly3change_columns) as TStringList)) t2
               where t.filial_id=:filial
                 and t.sub1part2type3column4=3 
                 and t2.column_value is not null
              group by t2.column_value'
    using p_clset, p_filial, p_snapdate, p_filial;
    commit;
  end if;
end;


-------------------------------------------------------------------------------------------------------
/* возвращает текст SQL запроса на создание партиционированной таблицы истории или сообщение об ошибке в параметре o_sql
   при передаче p_mode = gpv_CheckAndAlter выполняет запросы на создание таблицы
-- на вход:
p_cset_id     = идентификатор набора колонок от исходной таблицы с методами хранения этих колонок
p_source_name = имя входной таблицы
p_target_name = предполагаемое имя выходной таблицы (по умолчанию первая буква исходной таблицы будет заменена на "H")
o_sql         = текст запроса на создание временных таблиц
p_mode        = режим работы из двух: gpv_OnlyCheck, gpv_CheckAndAlter 
*/
function s100_CreateTargetTable(p_cset_id number, p_source_name varchar2, p_target_name varchar2 default null, o_sql out nocopy clob, p_mode number default 1) return number
is
  l_cnt     number;          -- временная 
  l_sql     clob;
  l_sql2tmp clob ;           -- результат
  l_sql_add varchar2(30000):='';
  l_link    varchar2(30);
  l_owner   varchar2(30);
  l_source_tab varchar2(30);
  l_target_tab varchar2(30);
  l_save_mthd  number; -- метод сохранения или по умолчанию, если не найден в d_mkhst_col_sets
  l_partColumn varchar(30); -- колонка по которой партиционирована исходная таблица
begin
	job_log_psv.start_log(null, $$plsql_unit, 'Начали работать');
  job_log_psv.write_detail_log('p_cset_id='||p_cset_id||', p_source_name='||p_source_name||', p_target_name='||p_target_name, 0);
  select count(1) into l_cnt from d_mkhst_col_sets where clset_id=p_cset_id;
  -- проверяем наличие в настроечной таблице такого набора колонок
  if l_cnt=0 then
		o_sql := 'Ошибка: Задан отсутствующий набор clset_id='||p_cset_id||' из таблицы d_mkhst_col_sets';
		job_log_psv.write_detail_log(o_sql, 0);
    job_log_psv.end_log ('Закончили работу');
		return res_error;
	end if;
  -- раскладываем имя вход.таблицы на компоненты
  decodeTabName(p_source_name, l_owner, l_source_tab, l_link);
  -- исли исходной нет - выходим с ошибкой
  if IfExists(p_source_name)=0 then 
		o_sql := 'Ошибка: Исходная таблица или представление '||p_source_name||' не существует';
		job_log_psv.write_detail_log(o_sql, 0);
    job_log_psv.end_log ('Закончили работу');
		return res_error;
	end if; 
  
  select max(a.column_name) into l_partColumn from all_part_key_columns a where a.owner=l_owner and a.name = l_source_tab;
  if l_partColumn is null then 
		o_sql := 'Ошибка: Исходная таблица не партиционирована. Пакет работает только с таблицами партиционированными по филиалам и субпартиционированными по датам';
		job_log_psv.write_detail_log(o_sql, 0);
    job_log_psv.end_log ('Закончили работу');
		return res_error;
	end if;
  
  -- если целевая есть, выходим с ошибкой и рекомендацией использовать другой метод пакета
  l_target_tab := nvl(upper(p_target_name), 'H'||substr(l_source_tab,2));
  if ifExists(l_target_tab)=1 then 
		o_sql := 'Ошибка: Целевая таблица '||l_target_tab||' уже существует - используйте процеду "s101_checkAbility2use" данного пакета для проверки возможности принять все требуемые столбцы исходной';
		job_log_psv.write_detail_log(o_sql, 0);
    job_log_psv.end_log ('Закончили работу');
    return res_error;
	end if; 
  --инициализация
  o_sql := 'execute immediate ''create table '||l_target_tab||'('||crlf||
    ' h_start_date date'||crlf||
    ',h_end_date   date'||crlf;
  -- формируем SQL на основе методов сохранения в параметрической таблице по набору p_cset_id и набора колонок исходной таблицы
  for i in (select 
              atc.COLUMN_NAME, dcs.svmtd_svmtd_id, atc.DATA_TYPE, atc.DATA_LENGTH, 
              atc.DATA_PRECISION, atc.data_scale, atc.NULLABLE, acc.COMMENTS
            from 
              d_mkhst_col_sets dcs,
              all_tab_columns      atc,
              all_col_comments     acc
            where 1=1
              and atc.COLUMN_NAME = dcs.column_name(+)
              and sysdate between dcs.start_date(+) and dcs.end_date(+)
              and atc.owner=acc.owner(+) and atc.table_name = acc.table_name(+) and atc.column_name = acc.column_name(+)
              and (
                       dcs.clset_id = p_cset_id --ищем по заданному набору
                   and atc.OWNER = upper(l_owner) -- исходной таблице (владелец)
                   and atc.TABLE_NAME = upper(l_source_tab) -- исходной таблице (имя таблицы)
                  )
            order by atc.column_id )
	loop
		if i.column_name in ('H_START_DATE', 'H_END_DATE') then 
		  o_sql := 'Ошибка: Исходная таблица или представление содержит столбец '||i.column_name||' являющийся ключевым. Не смогу работать с такой снимочной таблицей, т.к. в целевой исторической это служебное поле';
	  	job_log_psv.write_detail_log(o_sql, 0);
      job_log_psv.end_log ('Закончили работу');
		  return res_error;
	  end if;
    
    l_sql := '';
	  if i.svmtd_svmtd_id is null then 
			-- пробуем найти в дефолтной таблице
			select svmtd_svmtd_id into l_save_mthd 
      from D_MKHST_COL_DEFAULTS
      where i.COLUMN_NAME like upper(column_mask) and rownum<=1;
      if l_save_mthd is null then
				l_save_mthd := 1;
        job_log_psv.write_detail_log('-- для столбца '||i.column_name||' нет метода сохранения в d_mkhst_col_sets и ничего не подошло из дефолтных в d_mkhst_columns_def. Использую "1"=сохранять всегда если изменилось. Когда требуется контролировать все значения',0);
      else
				job_log_psv.write_detail_log('-- для столбца '||i.column_name||' нет метода сохранения в d_mkhst_col_sets. Подошел из дефолтных в d_mkhst_columns_def метод '||to_char(l_save_mthd), 0);
	  	end if;
    else
			l_save_mthd := i.svmtd_svmtd_id; 
		end if;

    if l_save_mthd >0 then 
	     l_sql := l_sql || ',' ||i.column_name||' '||i.data_type 
                      || case when i.data_type like '%CHAR%' then '('||i.data_length||')' 
                              when i.data_type='NUMBER' and i.data_precision is not null and i.data_scale is not null then '('||i.data_precision||','||i.data_scale||')' 
													    when i.data_type='NUMBER' and i.data_precision is not null  then '('||i.data_precision||')' 
                              else null end 
                      || case when i.nullable = 'N' then ' not null' else '' end 
                || l_sql || crlf;
       if i.COMMENTS is not null then 
         l_sql_add := l_sql_add || 'execute immediate ''comment on column '||l_target_tab||'.'||i.column_name|| ' is ''''' ||i.COMMENTS||''''''';'||crlf; 	
       end if;
    else
  	  -- skip column
      job_log_psv.write_detail_log('Для колонки ' || i.column_name || ' предписано не сохранять историю значений', 0);
		end if;
    o_sql := o_sql||l_sql;
	end loop;
  
  --копируем sql основной
  l_sql2tmp := replace(o_sql, l_target_tab, l_target_tab||'_TMP'); -- делаем копию целевой таблицы с суффиксом _TMP 
  --финализация основной таблицы
  if o_sql like 'execute immediate ''create%' then -- если при создании кода для основной все прошло без ошибок, то sql начинается со слова create
		o_sql := o_sql || ')' || crlf ||
      'partition by list ('||l_partColumn||')'|| crlf||
      'subpartition by range (h_END_DATE)'|| crlf ||
      '('|| crlf;
    for i in (select * from pub_ds.d_filials where filial_id<>0 order by filial_id)
			loop
				o_sql := o_sql ||case when i.filial_id<>1 then ' ,' else ' ' end ||
          'partition P'||i.filial_id||' values ('||to_char(i.filial_id)||') /*tablespace DW_FIL*/  pctfree 0'|| crlf||
          '  ('|| crlf||
          '    subpartition P201412_'||i.filial_id||' values less than(TO_DATE('''' 2015-01-01 00:00:00'''', ''''SYYYY-MM-DD HH24:MI:SS'''', ''''NLS_CALENDAR=GREGORIAN'''')) '|| crlf||
          '   ,subpartition P201512_'||i.filial_id||' values less than(TO_DATE('''' 2016-01-01 00:00:00'''', ''''SYYYY-MM-DD HH24:MI:SS'''', ''''NLS_CALENDAR=GREGORIAN'''')) '|| crlf||
          '   ,subpartition P201603_'||i.filial_id||' values less than(TO_DATE('''' 2016-04-01 00:00:00'''', ''''SYYYY-MM-DD HH24:MI:SS'''', ''''NLS_CALENDAR=GREGORIAN'''')) '|| crlf||
          '   ,subpartition P201606_'||i.filial_id||' values less than(TO_DATE('''' 2016-07-01 00:00:00'''', ''''SYYYY-MM-DD HH24:MI:SS'''', ''''NLS_CALENDAR=GREGORIAN'''')) '|| crlf||
          '   ,subpartition P201609_'||i.filial_id||' values less than(TO_DATE('''' 2016-10-01 00:00:00'''', ''''SYYYY-MM-DD HH24:MI:SS'''', ''''NLS_CALENDAR=GREGORIAN'''')) '|| crlf||
          '   ,subpartition P201612_'||i.filial_id||' values less than(TO_DATE('''' 2017-01-01 00:00:00'''', ''''SYYYY-MM-DD HH24:MI:SS'''', ''''NLS_CALENDAR=GREGORIAN'''')) '|| crlf||
          '   ,subpartition PLAST_'||i.filial_id||' values less than(maxvalue) '|| crlf||
          '  )'|| crlf;
			end loop;
      o_sql := o_sql || crlf||')'';';      
	end if;

  --финализация _TMP таблицы
  if o_sql like 'execute immediate ''create%' then -- если при создании кода для основной все прошло без ошибок, то sql начинается со слова create
		l_sql2tmp := l_sql2tmp || ',sub1part2type3column4 number, mostly3change_columns varchar2(3999)'||crlf||
      ')' || crlf||
      'partition by list ('||l_partColumn||')'|| crlf||
      'subpartition by list (sub1part2type3column4)'|| crlf||
      '('|| crlf;
    for i in (select * from pub_ds.d_filials where filial_id<>0 order by filial_id)
			loop
				l_sql2tmp := l_sql2tmp ||case when i.filial_id<>1 then ' ,' else ' ' end ||
          'partition P'||i.filial_id||' values ('||to_char(i.filial_id)||') /*tablespace DW_FIL*/  pctfree 0'|| crlf||
          '  ('|| crlf||
          '    subpartition SP_copy_'||i.filial_id||' values (-1) '|| crlf|| -- для копии H_ таблицы перед анализом  
          '   ,subpartition SP_RSLT_'||i.filial_id||' values (-2) '|| crlf|| -- для результата слияния снапшота и H_ таблицы
          '   ,subpartition SP_uncg_'||i.filial_id||' values ( 0) '|| crlf|| -- неизменные записи
          '   ,subpartition SP_new__'||i.filial_id||' values ( 1) '|| crlf|| -- новые
          '   ,subpartition SP_delt_'||i.filial_id||' values ( 2) '|| crlf|| -- удаленные
          '   ,subpartition SP_chng_'||i.filial_id||' values ( 3) '|| crlf|| -- изменившиеся
          '  )'|| crlf;
			end loop;
      l_sql2tmp := l_sql2tmp || crlf||')'';';      
	end if;

  o_sql := o_sql || crlf
           || l_sql_add || crlf
           || l_sql2tmp;
  dbms_output.put_line('begin'||crlf||o_sql||'end;');
      
  if p_mode = gpc_CheckAndAlter then 
		 execute immediate 'begin'||crlf||o_sql||'end;';
  end if;
  job_log_psv.end_log ('Закончили работу');
  return res_success;
end;


-----------------------------------------------------------------------------------------------------------------------------------
-- проверка возможности поместить данные исходной таблицы в целевую по правилам данного набора
/*
  возвращает либо "успех"=1 либо "ошибка" = -1
  возвращаемый параметр "o_cset_id" - содержит ID описывающий набор для переданной таблицы
  вход:
     p_source_name    обязательный  имя исходной таблицы
     o_cset_id        обязательный  идентификатор набора правил по работе со столбцами исходной таблицы
*/
function s102_CreateColumnSetRules(
  p_source_name    varchar2,
  o_clset_id       out number
) return number
is
  l_message       varchar2(2000);
  l_link          varchar2(30);
  l_source_owner  varchar2(30);
  l_source_tab    varchar2(30);
  l_max_cset      number;
  l_cnt           number;
begin
  -- раскладываем имя входной таблицы на компоненты - главное получить и L_SOURCE_OWNER и L_SOURCE_TAB
  decodeTabName(p_source_name, l_source_owner, l_source_tab, l_link);
  -- исли исходной нет - выходим с ошибкой
  if IfExists(p_source_name)=0 then 
		l_message := 'Ошибка: Исходная таблица или представление '||p_source_name||' не существует';
		job_log_psv.write_detail_log(l_message, 0);
		return res_error;
	end if; 

  select nvl(max(clset_id)+1,1) into l_max_cset from d_mkhst_col_sets;
  
  insert into d_mkhst_col_sets(clset_id, column_name, svmtd_svmtd_id, clmtp_clmtp_id, column_comm, start_date)
  select 
    l_max_cset, ut.column_name, 
    nvl((select svmtd_svmtd_id from d_mkhst_col_defaults where ut.COLUMN_NAME like upper(column_mask) and rownum<=1),1) svmtd_svmtd_id, 
    (select 1 from all_ind_columns ai where ai.index_owner=atc.owner and ai.table_name = atc.TABLE_NAME 
                                        and ai.INDEX_NAME = atc.CONSTRAINT_NAME and ai.COLUMN_NAME = ut.column_name) clmtp_clmtp_id,
     '/*' ||ut.table_name || ','||to_char(ut.column_id,'0000')||'*/ ' || substr(acc.COMMENTS,1,100) COMMENTS,
    to_date('01.01.2000','dd.mm.yyyy') start_date
  from 
    all_tab_columns  ut,
    all_col_comments acc,
    (select ac.owner, ac.table_name, ac.CONSTRAINT_NAME from all_constraints ac where CONSTRAINT_TYPE='P' ) atc
  where 1=1
    and ut.OWNER = l_source_owner and ut.table_name = l_source_tab 
    and ut.OWNER = acc.owner(+) and ut.table_name = acc.table_name(+) and ut.COLUMN_NAME = acc.column_name(+)
    and ut.OWNER = atc.owner(+) and ut.table_name = atc.table_name(+) 
  order by 
    ut.column_id;
    
  l_cnt := sql%rowcount;
  
  update d_mkhst_col_sets 
     set clmtp_clmtp_id= case when column_name in('BILLING_FILIAL_ID') then 1 
                              when column_name in('IS_MGR')            then 3 
															else null end
  where clset_id = l_max_cset;

  o_clset_id := l_max_cset;
  commit;
	job_log_psv.write_detail_log('ВНИМАНИЕ. В настройки '||user||'.d_mkhst_col_sets успешно добавлен набор ('||l_max_cset||') для таблицы "'||l_source_owner||'.'||l_source_tab||'". '
     ||'Обязательно отметьте поля, логически являющиеся первичными ключами. Для сопоставления по ним снимков за разные даты. '||crlf
     ||'Например: subs_subs_id, filial_id, billing_filial_id для таблицы отслеживающей аналитиу абонентов в филиале', l_cnt);

  return res_success;
EXCEPTION
   WHEN OTHERS THEN
      l_message := substr(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace(),1,1800);
      job_log_psv.write_detail_log(l_message); -- текст исключения
      job_log_psv.end_log_with_error (in_error => l_message);
      RAISE;
END;
  

-----------------------------------------------------------------------------------------------------------------------------------
-- проверка возможности поместить данные исходной таблицы в целевую по правилам данного набора
/*
  возвращает либо "успех"=1 либо "ошибка" = -1
  возвращаемый параметр "o_res" - содержит либо информационный\ошибочный текст либо SQL оператор для выполнения приведения целевой таблицы
  вход:
     p_cset_id        обязательный  идентификатор набора правил по работе со столбцами исходной таблицы
     p_source_name    обязательный  имя исходной таблицы
     o_res            обязательный  цлоб переменная для информативного результата
     p_target_name    не обязат     имя целевой таблицы - вычисляется как замена первой буквы на "H"
     p_mode           не обязат     режим. либо gpv_OnlyCheckMode - только проверка, либо gpv_CheckAndAlterMode - проверка и исправление
     p_filial_id      не обязат     проверка наличия субпартиций в указанном филиале или во всех таблице     
*/
function s101_CheckAbility2use(
	p_cset_id       number, 
  p_source_name   varchar2, 
  o_res           out nocopy clob, 
  p_target_name   varchar2 default null, 
	p_mode number   default 0,
  p_filial_id     number default 0) return number
is
  l_cnt           number :=0;      -- временная 
  l_mdfCnt        number :=0;      -- сколько изменений накопилось
  l_sql           clob ;           -- для SQL под изменения
--  l_sql_add       varchar2(30000):='';
  l_link          varchar2(30);
  l_source_owner  varchar2(30);
  l_source_tab    varchar2(30);
  l_target_owner  varchar2(30);
  l_target_tab    varchar2(30);
  L_HasAddFld     number;
begin
  job_log_psv.write_detail_log('s101_CheckAbility2use: p_source_name='||lower(p_source_name)
                       ||', p_target_name='||lower(p_target_name)
                       ||', p_mode='||case when p_mode=gpc_OnlyCheck then 'gpv_OnlyCheck'
                                           when p_mode=gpc_CheckAndAlter then 'gpv_CheckAndAlter'
																			else 'undefined' end
                       ||', p_filial_id='||p_filial_id, 0);
  
  -- проверка на допустимый режим работы
  if p_mode not in (gpc_OnlyCheck, gpc_CheckAndAlter) then 
		o_res := 'Ошибка: Задан недопустимый режим работы. Используйте один из gpv_OnlyCheck, gpv_CheckAndAlter';
		job_log_psv.write_detail_log(o_res, 0);
		return res_error;
  end if;
  
  -- раскладываем имя входной таблицы на компоненты - главное получить и L_SOURCE_OWNER и L_SOURCE_TAB
  decodeTabName(p_source_name, l_source_owner, l_source_tab, l_link);
  -- исли исходной нет - выходим с ошибкой
  if IfExists(p_source_name)=0 then 
		o_res := 'Ошибка: Исходная таблица или представление '||p_source_name||' не существует';
		job_log_psv.write_detail_log(o_res, 0);
		return res_error;
	end if; 
  
  -- если целевой нет, выходим с ошибкой и рекомендацией использовать другой метод пакета
  l_target_owner := user;
  l_target_tab := nvl(upper(p_target_name), 'H'||substr(l_source_tab,2));
  if ifExists(l_target_tab)=0 then 
		o_res := 'Ошибка: Целевой таблицы '||l_target_tab||' не существует - используйте предварительно процеду "s100_CreateTargetTable" данного пакета для создания';
		job_log_psv.write_detail_log(o_res, 0);
    return res_error;
  else
		 -- раскладываем имя целевой таблицы на компоненты - главное получить "l_target_owner" и "l_target_tab" - только имя таблицы
     decodeTabName(l_target_tab, l_target_owner, l_target_tab, l_link);
	end if; 
  

	for i in (select * from pub_ds.d_filials f 
		        where f.filial_id = decode(p_filial_id, 0,f.filial_id, p_filial_id)
              and filial_id<>0
              order by f.filial_id)
	loop 
    select count(1) into l_cnt
    from all_tab_subpartitions 
    where 1=1
      and table_owner= l_target_owner
      and table_name = l_target_tab||'_TMP' 
      and partition_name='P'||i.filial_id
      and substr(subpartition_name,1, length(subpartition_name)-length(to_char(i.filial_id))) 
          in ('SP_RSLT_','SP_COPY_','SP_UNCG_','SP_NEW__','SP_DELT_','SP_CHNG_');
      
    if l_cnt<>6 then 
      job_log_psv.write_detail_log('В партиции P'||i.filial_id||' таблицы '||l_target_owner||'.'||l_target_tab||'_TMP обнаружено только '||l_cnt||' субпартиций. Лучше пересоздать всю партицию заново');
      dbms_output.put_line('В партиции P'||i.filial_id||' таблицы '||l_target_owner||'.'||l_target_tab||'_TMP обнаружено только '||l_cnt||' субпартиций. Лучше пересоздать всю партицию заново');
      l_mdfCnt := l_mdfCnt +1;
      o_res := o_res || 'execute immediate ''alter table '||l_target_tab||'_TMP drop partition P'||i.filial_id||''';'||crlf;
      o_res := o_res || 'execute immediate ''alter table '||l_target_tab||'_TMP add partition P'||i.filial_id||' values ('||i.filial_id||')  pctfree 0
            (
              subpartition SP_copy_'||i.filial_id||' values (-1)  /* для копии H_ таблицы перед анализом */ 
             ,subpartition SP_RSLT_'||i.filial_id||' values (-2)  /* для результата слияния снапшота и H_ таблицы */
             ,subpartition SP_uncg_'||i.filial_id||' values ( 0)  /* неизменные записи */
             ,subpartition SP_new__'||i.filial_id||' values ( 1)  /* новые */
             ,subpartition SP_delt_'||i.filial_id||' values ( 2)  /* удаленные */
             ,subpartition SP_chng_'||i.filial_id||' values ( 3)  /* изменившиеся */
            )'';' ||crlf;
    end if;
  end loop;  
  
         
  -- все проверки пройдены - можно работать
  for i in (select 
              ats.column_id, 
              atS.COLUMN_NAME src_column_name, atD.COLUMN_NAME dest_column_name, atT.COLUMN_NAME temp_column_name
              ,dcs.svmtd_svmtd_id 
              ,atS.DATA_TYPE       src_data_type,      atd.DATA_TYPE       dest_data_type
              ,atS.DATA_LENGTH     src_DATA_LENGTH,    atd.DATA_LENGTH     dest_DATA_LENGTH 
              ,atS.DATA_PRECISION  src_DATA_PRECISION, atd.DATA_PRECISION  dest_DATA_PRECISION
              ,atS.data_scale      src_data_scale,     atd.data_scale      dest_data_scale 
              ,atS.NULLABLE        src_NULLABLE,       atd.NULLABLE        dest_NULLABLE 
              ,acc.COMMENTS
            from 
               (select * from all_tab_columns      where owner=l_source_owner  and table_name = l_source_tab ) atS
              ,(select * from all_tab_columns      where owner=user            and table_name = upper(l_target_tab) ) atD
              ,(select * from all_tab_columns      where owner=user            and table_name =  upper(l_target_tab)||'_TMP' ) atT
              ,(select * from d_mkhst_col_sets where clset_id =p_cset_id    and end_date>sysdate ) dcs
              ,(select * from all_col_comments     where owner=l_source_owner  and table_name = l_source_tab ) acc
            where 1=1
              and ats.column_name = atd.column_name(+)
              and ats.column_name = att.column_name(+)
              and ats.column_name = dcs.column_name(+)
              and ats.column_name = acc.column_name(+)
            order by atS.column_id)
	loop
		-- поля нет в наборе - выходим с аварийной ошибкой, т.к. только человек может сказать - как обрабатывать при сохранении поле
/*    if i.svmtd_svmtd_id is null or i.svmtd_svmtd_id=-1 then 
			o_res := 'Ошибка: поле "'||i.src_column_name||'" нет в наборе clset_id='||p_cset_id||' /только человек может сказать - как обрабатывать при сохранении поле';
  		job_log_psv.write_detail_log(o_res, 0);
			return res_error;
		end if;*/
		-- поле есть в наборе но отмечено как не сохраняемое - идем дальше
 	  if i.svmtd_svmtd_id=0 then continue; end if;
    -- поле есть в наборе и отличается тип!
    if i.src_data_type<>i.dest_data_type then 
			execute immediate 'select count(1) from '||l_target_owner||'.'||l_target_tab||' where '||i.dest_column_name||' is not null and rownum<=1' 
                    into l_cnt;
      if l_cnt>0 then 
			  o_res := 'Ошибка: поле "'||i.src_column_name||'" изменило тип с '||i.src_data_type||' на '||i.dest_data_type||'. Измените вручную тип в целевой таблице '||l_target_tab;
  		  job_log_psv.write_detail_log(o_res, 0);
			  return res_error;
      else
				l_mdfCnt := l_mdfCnt + 1;
				job_log_psv.write_detail_log('Обнаружено изменение типа с '||i.src_data_type||' на '||i.dest_data_type||' у колонки '||i.src_column_name||'. Повезло, что в целевой таблице это поле не имеет значений. Можно сразу сделать ALTER на замену типа.');
        o_res := o_res || ' execute immediate ''alter table '||l_target_tab||' modify ('
                       || i.src_column_name||' '||i.src_data_type 
                       || l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
											 || ')'';'||crlf;
        o_res := o_res || ' execute immediate ''alter table '||l_target_tab||'_TMP modify ('
                       || i.src_column_name||' '||i.src_data_type 
                       || l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
											 || ')'';'||crlf;
			end if;
		end if;
    
    -- поменяли размерность у строк или чисел
    if (i.src_data_type in ('CHAR', 'VARCHAR', 'VARCHAR2', 'NCHAR') and i.src_DATA_LENGTH>i.dest_DATA_LENGTH) or
			 ((i.src_data_type in ('NUMBER') and 
		    (i.src_DATA_PRECISION>i.dest_DATA_PRECISION or i.src_data_scale > i.dest_data_scale)) )
		then 
			l_mdfCnt := l_mdfCnt + 1;
      l_sql := l_sql || '  ' || case when length(l_sql)>0 then ',' else null end 
                     || i.src_column_name ||' ' || i.src_data_type 
                     || l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
                     || crlf;
      job_log_psv.write_detail_log('поменяли размерность с ' ||l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
                                || ' на ' || l_getSize(i.dest_data_type, i.dest_data_length, i.dest_data_precision, i.dest_data_scale) 
                                || ' у '||i.src_data_type||' поля '|| i.src_column_name, 0);
		end if;
    
    -- добавили новое ноле. Оно отсутствует в исторической таблице
    if i.dest_column_name is null then 
			l_mdfCnt := l_mdfCnt + 1;
			o_res := o_res || ' execute immediate ''alter table '||l_target_tab||' add ('||i.src_column_name ||' ' || i.src_data_type 
                     || l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
                     || ')'';' || crlf;
		  -- причем оно отсутствует в наборе d_mkhst_col_sets
      if i.svmtd_svmtd_id is null then 
        job_log_psv.write_detail_log('В набор '||p_cset_id||' добавлено поле '||i.src_column_name||' с методом -1. Требуется ручная настройка метода сохранения, чтобы продолжала работать автоматическая процедера сохранения истории');
        L_HasAddFld := L_HasAddFld + 1;
        insert into d_mkhst_col_sets(
            CLSET_ID,
            COLUMN_NAME,      
            SVMTD_SVMTD_ID, 
            COLUMN_COMM,
            START_DATE,
            END_DATE)
        values(p_cset_id,       
               i.src_column_name,            
               -1,/* метод -1 будет вызывать exception и требовать ручного присвоения метода сохранения*/
               '/*'||l_source_tab||','||to_char(i.column_id,'0000')||'*/'||i.COMMENTS, 
               to_date('01.01.2000','dd.mm.yyyy'), 
               to_date('31.12.2999','dd.mm.yyyy'));
        commit;
      end if;
		end if;
    -- ноле отсутствует в темпорари таблице
    if i.temp_column_name is null then 
			l_mdfCnt := l_mdfCnt + 1;
			o_res := o_res || ' execute immediate ''alter table '||l_target_tab||'_TMP add ('||i.src_column_name ||' ' || i.src_data_type 
                     || l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
                     || ')'';' || crlf;
		end if;
  end loop;
  
  -- формируем одним запросом, если изменили размерности у нескольких полей
  if length(l_sql)>0 then 
    o_res := o_res || ' execute immediate ''alter table '||l_target_tab||' modify (' ||crlf
                   || l_sql ||' )'';'
                   ||crlf;
    o_res := o_res || ' execute immediate ''alter table '||l_target_tab||'_TMP modify (' ||crlf
                   || l_sql ||' )'';'
                   ||crlf;
  end if;
  
  -- преверяем наличие у _TMP таблицы служебных колонок
  for i in (select 
              tml.cname, tml.ctype, atc.COLUMN_NAME 
            from 
              (select column_name from user_tab_columns where table_name = upper(l_target_tab)||'_TMP' ) atc,
              (select 'H_START_DATE'          cname, 'date'         ctype from dual union all
               select 'H_END_DATE'            cname, 'date'         ctype from dual union all
               select 'SUB1PART2TYPE3COLUMN4' cname, 'number'       ctype from dual union all
               select 'MOSTLY3CHANGE_COLUMNS' cname, 'varchar(3999)'ctype from dual) tml
            where tml.cname = atc.column_name(+)
            ) 
  loop
    if i.column_name is null then 
      l_mdfCnt := l_mdfCnt + 1;
      o_res := o_res || ' execute immediate ''alter table '||l_target_tab||'_TMP add ('||i.cname||' ' || i.ctype || ')'';' || crlf;
    end if;
  end loop;

  if l_mdfCnt = 0 then   
    o_res := 's101_CheckAbility2use - структуры таблиц '||l_source_owner||'.'||l_source_tab||' и '||l_target_owner||'.'||l_target_tab||' совпадают';
		job_log_psv.write_detail_log(o_res);
  else    
		job_log_psv.write_detail_log('s101_CheckAbility2use - структуры таблиц '||l_source_owner||'.'||l_source_tab||' и '||l_target_owner||'.'||l_target_tab||' различаются');
    -- обрамляем в анонимный PL\SQL блок если требуется несколько alter для целевой таблицы
    o_res := 'begin'||crlf
                    || o_res
                    ||'end;'||crlf;
    if p_mode = gpc_CheckAndAlter then 
	  	job_log_psv.write_detail_log('Выполняю SQL:'||o_res);
  	  l_runSql(o_res);
      o_res := 'выполнен запрос:'||crlf||o_res;
    else
  		job_log_psv.write_detail_log('Выполнение запрещено, получился SQL:'||o_res);
    end if;
  end if;
  
  if L_HasAddFld>0 then 
			o_res := 'В настроечный набор '||p_cset_id||' добавили '||L_HasAddFld||' новых полей с методом SVMTD_SVMTD_ID=-1. Требуется ручная настройка.'||
       'select * from d_mkhst_col_sets where clset_id='||p_cset_id||' and SVMTD_SVMTD_ID=-1 order by COLUMN_COMM';
  		job_log_psv.write_detail_log(o_res, 0);
			return res_error;
  end if;
  
  return res_success;
end;


/*
-------------------------------------------------------------------------------------------------------
Procedure AddSnap2start(in_filial_id IN NUMBER default 0, in_snap_date IN VARCHAR2, in_scr_table varchar2)
is
   v_lockhandle   NUMBER;
   v_lock_release NUMBER;
begin
   job_log_psv.start_log(in_filial_id, $$plsql_unit, 'Начали работать');

   -- выставляем блокировку, чтобы запретить вызов пакета для данного филиала в другом процессе
   DBMS_LOCK.allocate_unique (lockname => USER || '_' || $$plsql_unit, lockhandle => v_lockhandle);
   -- ждём, пока блокировка не будет снята
   WHILE DBMS_LOCK.request (lockhandle => v_lockhandle) <> 0
   LOOP
     DBMS_LOCK.sleep (60);
   END LOOP;
   job_log_psv.write_detail_log('Теперь к делу', 0);
   
   job_log_psv.end_log ('Закончили работу');
   -- Снимаем блокировку
   v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
EXCEPTION
   WHEN OTHERS THEN
      -- Снимаем блокировку
      v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
      job_log_psv.end_log_with_error (in_error => SUBSTR (dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace(), 1, 1800));
      RAISE;
END;
*/

--------------------------------------------------------------------------------------
-- зачитать настройки из таблицы и тривиальные проверки
procedure readAndCheckTabParam(in_scr_table in varchar2, in_snap_date varchar2
	 ,in_out_clset_id in out number
   ,out_mkhst       out number
   ,out_target_tab  out varchar2
   ,out_dayDiff     out number
   ,out_snapDate    out date

   ,out_keyFiledOne   out varchar2/*---< clmtp_clmtp_id=2 ---------  как бы первичный ключ. будет сипользоваться в определени новых или удаленных. перспективно можно создать локальный индекс по нему*/ 
   ,out_colList       out varchar2/*---< svmtd_svmtd_id>0 ---------  все колонки набора кроме исключаемых в перечисление insert(...)*/
   ,out_keyFields12   out varchar2/*---< clmtp_clmtp_id =1,2       - все ключевые колонки набора в динамический select  */
   ,out_keyFields3    out varchar2/*---< clmtp_clmtp_id =3         - все ключевые колонки набора в динамический select  */
   ,out_whereKeyFlds12 out varchar2/*---<--------- получится        alc.field1 = src.field1          and        alc.field2=src.field2 */
   ,out_whereKeyFlds3 out varchar2 /*---<--------- получится  nvl(alc.field1,0)=nvl(src.field1(+),0) and nvl(alc.field2,0)=nvl(src.field2(+),0) */
)
is
  l_cnt number;
  l_link varchar2(50);
  l_message varchar2(1000);
  l_datePicture varchar2(50);
begin
	if in_out_clset_id is null then 
		select min(clset_clset_id), count(1) into in_out_clset_id, l_cnt
      from m_mkhst_tables m 
    where upper(m.source_tab)=upper(in_scr_table);
    
    if l_cnt=0 then 
      l_message:='У таблицы "'||in_scr_table||'" отсутствует какой либо набор в настроечной таблице m_mkhst_tables';
      job_log_psv.write_detail_log(l_message);
 	    raise_application_error(-20000,l_message);
    elsif l_cnt>1 then 
      job_log_psv.write_detail_log('У таблицы "'||in_scr_table||'" есть '||l_cnt||' наборов. Использую минимальный - '||in_out_clset_id);
    else
      job_log_psv.write_detail_log('У таблицы "'||in_scr_table||'" использую набор - '||in_out_clset_id);
		end if;
  end if;
  
	select max(mkhst_id)
          ,max(enabl)
          ,max(target_tab)
          ,max(maxDayDiff)
          ,max(datePicture) 
     into  out_mkhst
          ,l_cnt
          ,out_target_tab /*---<---------*/
          ,out_dayDiff    /*---<---------*/
          ,l_datePicture/*---<---------*/ 
   from m_mkhst_tables m 
   where upper(m.source_tab)=upper(in_scr_table) and clset_clset_id = in_out_clset_id;
   -- преобразовать имя исторической таблицы к "нормальному" виду
   decodeTabName(out_target_tab, l_link, out_target_tab, l_link);
   out_snapDate := to_date(in_snap_date, l_datePicture);

   if l_cnt is null then 
     l_message:='Указанной таблицы "'||in_scr_table||'" с указанным clset_id='||in_out_clset_id||' не найдено в настроечной таблице m_mkhst_tables';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif l_cnt=0 then
     l_message:='Для указанной таблицы "'||in_scr_table||'" с указанным clset_id='||in_out_clset_id||' запрещена работа (enabl<>1)';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   else
     job_log_psv.write_detail_log('Работаем в '||out_target_tab||'. допустимое расстояние в днях='||out_dayDiff);
	 end if;
   
   -- инициализируем переменные для списка колонок
   select 
          nvl(max(case when t.clmtp_clmtp_id=2         then column_name else null end),
              max(case when t.clmtp_clmtp_id in (1,2,3)then column_name else null end))
     ,listagg(case when svmtd_svmtd_id>0           then column_name else null end,',') within group (order by column_name)
     ,listagg(case when t.clmtp_clmtp_id in (1,2)  then column_name else null end,',') within group (order by column_name)
     ,listagg(case when t.clmtp_clmtp_id in (3)    then column_name else null end,',') within group (order by column_name)
     ,listagg(case when t.clmtp_clmtp_id in (1,2)  then 'alc.'||column_name||'=src.'||column_name||'(+)' else null end,' and ') within group (order by column_name)
     ,listagg(case when t.clmtp_clmtp_id=3         then 'nvl(alc.'||column_name||',0)=nvl(src.'||column_name||'(+),0)' else null end,' and ') within group (order by column_name)
     ,sum(case when nvl(svmtd_svmtd_id,-1)=-1 then 1 else 0 end)
   into 
      out_keyFiledOne   /*---< clmtp_clmtp_id=2 ---------  как бы первичный ключ. будет сипользоваться в определени новых или удаленных. перспективно можно создать локальный индекс по нему*/ 
     ,out_colList       /*---< svmtd_svmtd_id>0 ---------  все колонки набора кроме исключаемых в перечисление insert(...)*/
     ,out_keyFields12   /*---< clmtp_clmtp_id =1,2       - все ключевые колонки набора в динамический select  */
     ,out_keyFields3    /*---< clmtp_clmtp_id =3         - все ключевые колонки набора в динамический select  */
     ,out_whereKeyFlds12 /*---<--------- получится        alc.field1 = src.field1          and        alc.field2=src.field2 */
     ,out_whereKeyFlds3  /*---<--------- получится  nvl(alc.field1,0)=nvl(src.field1(+),0) and nvl(alc.field2,0)=nvl(src.field2(+),0) */
     ,l_cnt
   from d_mkhst_col_sets t
   where clset_id=in_out_clset_id
     and out_snapDate between start_date and end_date;
     
   if out_colList is null then 
     l_message:='не указано ни одного столбца для сохранения';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif out_keyFields12 is null then
     l_message:='не указано ни одного столбца в качестве ключевого. проверьте и отметьте filial_id, billing_filial_id, [subs|clnt|nset|srcd|cntr|...]';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   end if;
   
   if l_cnt>0 then 
     l_message:='Есть столбцы с методом сохранения -1 или NULL. для них требуется ручная настройка метода.';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   end if;
		 
end;				 


--------------------------------------------------------------------------------------
--
procedure checkSnapTable(in_filial_id number, in_scr_table in varchar2, in_snapDate date
	  ,out_snap_SubPart out varchar2
    ,out_snapCnt      out number
)    
is 
  l_cnt number;
  l_tmp number;
  l_owner varchar2(50);
  l_message varchar2(1000);
  l_source_tab varchar2(50);
  l_link  varchar2(50);
  l_PartColumnName varchar2(50);
  l_SubPartColumnName varchar2(50);
begin
   -- раскладываем имя вход.таблицы на компоненты
   decodeTabName(in_scr_table, l_owner, l_source_tab  /*---<---------*/, l_link);
   -- как называется колонка партиционирования?
   select column_name into l_PartColumnName from ALL_PART_KEY_COLUMNS where owner=l_owner and name=l_source_tab;
   -- как называется колонка субпартиционирования?
   select column_name into l_SubPartColumnName from ALL_SUBPART_KEY_COLUMNS where owner=l_owner and name=l_source_tab;
   
   -- в заказанном филиале и дате сколько записей? БОнусом - определям имя субпартиции, которая хранит эти данные
   execute immediate 'select  max(dbms_mview.pmarker(rowid)), min(dbms_mview.pmarker(rowid)) o_id, count(1)
         from '||l_owner||'.'||l_source_tab||' s 
         where '||l_PartColumnName   ||' = :f 
           and '||l_SubPartColumnName||' = :d'
   into l_cnt, l_tmp, out_snapCnt
   using in_filial_id, in_snapDate;
   
   if l_cnt <> l_tmp then
		 l_message:='Ошибка: данные лежат в разных партициях. Данный алгоритм не сможет корректно заменить данные новой порцией';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message); 
	 end if;
   
   -- снимок тестовый?
   if out_snapCnt <= gpv_snapLimitCount then 
     l_message:='Ошибка: в снимке '||l_owner||'.'||l_source_tab
       ||' where '||l_PartColumnName ||' = '||in_filial_id ||' and '||l_SubPartColumnName||' = '||to_char(in_snapDate)
                                   ||' кол-во записей '||out_snapCnt
                                   ||'. Это меньше порога make_hist_from_s.gpv_snapLimitCount='||gpv_snapLimitCount;
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
	 end if;
   
   select subobject_name into out_snap_SubPart/*---<---------*/ 
     from all_objects 
    where data_object_id = l_tmp;
   job_log_psv.write_detail_log('В субпартиции "'||out_snap_SubPart||'" филиала '||to_char(in_filial_id)
                                             ||' исх. таблицы '||lower(l_owner||'.'||l_source_tab)
                                             ||' записей '||num2ch(out_snapCnt),out_snapCnt);
   	
end;

-------------------------------------------------------------------------------------------------------
procedure checkColumns_clmtp(
	 in_clset_id number, in_keyFields12 varchar2, in_keyFields3 varchar2 
   ,in_owner varchar2, in_table varchar2, in_snap_SubPart varchar2
	 ,out_sqlClob  out clob)
is
  l_cnt   number;
  l_tmp   number;
  l_tmp2  number;
  l_message varchar2(1000);
begin
	-- Если один из ключевых столбцов источника! может содержать пустые значения, то анализа истории не получится.
   -- почти все будет считаться новым!
   out_sqlClob := 'select '||crlf
              ||'  count(1)'
              ||'  ,sum(case when '||replace(in_keyFields12,',',' is null or ')||' is null then 1 else 0 end) '||crlf
              ||'  ,sum(case when '||case when length(in_keyFields3)>0 then replace(in_keyFields3 ,',',' is null or ') 
                                          else '1 ' end
                                   ||' is null then 1 else 0 end) '||crlf
              ||'from '||in_owner||'.'||in_table||' subpartition ('||in_snap_SubPart||')';
   execute immediate out_sqlClob into l_cnt, l_tmp, l_tmp2;
   if l_tmp>0 then 
     l_message:='Ошибка: один из ключевых столбцов ('||in_keyFields12||') таблицы '||in_owner||'.'||in_table||' содержит пустые значения. И не может использоваться в качестве опорного при сравнении изменился\новый\старый. Проверьте и снимите флаг clmtp_clmtp_id с такого в наборе clset_id='||in_clset_id;
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif l_tmp2=l_cnt and l_cnt >0 and length(in_keyFields3)>0 then 
     job_log_psv.write_detail_log('Оптимизация: ключевые поля допускающие NULL обраружено в кол-ве '||num2ch(l_tmp2)||', '||num2ch(l_tmp2/l_cnt,2)||'% от всех');
   elsif l_tmp2=0     and l_cnt >0 and length(in_keyFields3)>0 then 
     job_log_psv.write_detail_log('Оптимизация: ключевые поля допускающее NULL не обнаружены в снимке, можно поменять им clmtp_clmtp_id=3 на 1');
	 end if;
end;

----------------------------------------------------------------------------------
function checkDayDistance(in_snapDate date, in_h_date date, in_dateDiff number, in_filial_id number, in_mkhst_id number) return number
is
  l_message  varchar2(1000);
  l_cnt      number;
  l_lastDat  date;
begin
	 if in_h_date is null then
     return 0;
 	 elsif trunc(in_snapDate) < trunc(in_h_date) then 
     l_message:='Ошибка: Дата снимка('||to_char(in_snapDate)||') меньше чем в истории('||to_char(in_h_date)||'). Вероятно данные из снимка уже учтены в Н_ таблице. Если это не так используйте процедуру "AddSnapInsert"';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif trunc(in_snapDate) = trunc(in_h_date) then 
     l_message:='Ошибка: Дата снимка('||to_char(in_snapDate)||') равна дате в истории('||to_char(in_h_date)||'). Вероятно данные из снимка уже учтены в Н_ таблице.';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif trunc(in_snapDate)-trunc(in_h_date) > in_dateDiff then 
		 -- при тестировании S_DEALER_ACCOUNTS_DAILY по КФ за 01/01/2017 не было изменений\добавлений\удалений. снимок обработан, 
     -- но в истории максимальная H_START_DATE осталась 31/12/2017. поэтому проверим что в результатах прошлый снимок был обработан с нулевыми изменениями
     select max(snapDate) 
       into l_lastDat
     from f_mkhst_results
       where filial_id = in_filial_id
         and mkhst_mkhst_id = in_mkhst_id
         and snapDate<>in_snapDate;
     -- проверяем предыдущую запись в рез.таблице - если она ОК - то функция тоже вернет успех    
     select count(1) 
       into l_cnt
     from f_mkhst_results
       where filial_id = in_filial_id
         and mkhst_mkhst_id = in_mkhst_id
         and snapDate = l_lastDat
         and (newly+closed+CHNG_CLOSED+CHNG_opened)=0
         and TOTAL_RELOAD=UNCHANGED
         and exitcode=1;
     if l_cnt=1 /*and (trunc(l_lastDat)-trunc(in_h_date)<=in_dateDiff )*/then
       job_log_psv.write_detail_log('Дистанция '||to_char(trunc(in_snapDate)-trunc(in_h_date))||' дней. лимит превышен. Но прошлый снимок('||to_char(l_lastDat)||') был нормально обработан');
     elsif in_dateDiff=1 and trunc(in_snapDate) = trunc(last_day(in_snapDate)) 
                         and trunc(in_h_date)   = trunc(last_day(in_h_date))
                         and months_between(in_snapDate, in_h_date)=1
     then
       job_log_psv.write_detail_log('Дистанция '||to_char(trunc(in_snapDate)-trunc(in_h_date))||' дней. лимит превышен. '
                    ||'Но снимок('||to_char(in_snapDate)||') является концом следующего месяца за крайней датой в истории('||to_char(in_h_date)||')');
		 else
       l_message:='Ошибка: Дата снимка('||to_char(in_snapDate)||') отстоит от даты в истории('||to_char(in_h_date)||') на "'
                    ||to_char(trunc(in_snapDate)-trunc(in_h_date))||'" дней'||crlf
                    ||'Это превышает указанный предел maxDayDiff в m_mkhst_tables для указанной таблицы и набора. Попробуйте сначала докатить историю последовательно вызвав AddSnap2End';
       job_log_psv.write_detail_log(l_message);
       raise_application_error(-20000,l_message);
     end if;
   end if; 
	 return res_success;
end;
  
---------------------------------------------------------------------------------------------------------
-- когда в крайней партии начинают появлятся закрытые записи по ключевым полям l_keyFields12
-- например по набору  FILIAL_ID, SK_SUBS_ID, IS_MGR
procedure checkResAndSplitPartition(in_filial_id number, in_target_tab varchar2, in_H_lastSubPart varchar2
	 ,l_keyFields12 varchar2 ,l_keyFields3 varchar2)
is
  l_tmp number;
begin
  execute immediate 'select count(1) from ('||crlf
        ||'select '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||', count(1)'||crlf
        ||'from '||in_target_tab||' subpartition ('||in_H_lastSubPart||')'||crlf
        ||' group by '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
        ||'having count(1)>1)'
  into l_tmp;
  if l_tmp>0 then 
    job_log_psv.write_detail_log('Надо делить последнюю партицию. в нее начала попадать история по закрытым записям ('||l_tmp||')штук');
    l_tmp := add_partition(in_target_tab, in_filial_id);
	end if;
end;

--==================================================================================================
-- основная точка входа
/*
  in_filial_id    по умолчанию 0 = обозначает что целевая таблица в схеме PUB_DS
  in_snap_date    добавляемый день
  in_scr_table    таблица-источник снимков
*/
-------------------------------------------------------------------------------------------------------
Procedure AddSnap2End(in_filial_id IN NUMBER, in_scr_table varchar2, in_snap_date IN VARCHAR2, in_clset_id number default null)
is
   v_lockhandle   NUMBER;
   v_lock_release NUMBER;
   
   l_cnt          number:=0; -- временная переменная 
   l_tmp          number:=0; -- временная переменная 
   l_n0           number:=0; -- временная переменная (неизменные)
   l_n1           number:=0; -- временная переменная (новые)
   l_n2           number:=0; -- временная переменная (удаленные)
   l_n3           number:=0; -- временная переменная (изменившиеся)
   l_n3unq        number:=0; -- временная переменная (изменившиеся-уникальные \ если во вставляемом снимке происходит детализация по ключевым полям. например месяц дебета(ключевое поле) разбивается на bill_bill_id(сохраняемые значения)
   l_mkhst        number;    -- PK из настроечной таблицы к нашим параметрам
   l_clset_id     number;
   l_target_tab   varchar2(50);       -- имя целевой талицы (берем из настроек)
  l_owner       varchar2(50); -- владелец p_source_tab
  l_source_tab  varchar2(50); -- таблица в которой ищем последнюю партицию ф указанном филиала p_filial_id
  l_link        varchar2(50); -- возможный линк из p_source_tab / временная переменная
  l_snap_SubPart varchar2(50);
  l_snapDate   date; -- дата в требуемой субпартиции исходной таблицы этого филиала
  l_snapCnt    number; -- колво записей в исходной субпартиции
  l_message     varchar2(6000);
  l_sqlClob     clob;
  l_msgCase     varchar2(16000); -- для вычисления полей, изменения значения которых должны быть записаны в историю
  l_msgCaseFld  varchar2(26000); -- для вычисления названий полей, изменения значения которых произошли. выявим полявносяших наибольший вклад в изменение истории
  l_H_lastSubPart varchar2(50); -- имя последней субпартиции в целевой "H_" таблице для данного филиала
  l_H_lastDate    date;         -- дата, крайняя сохраненненая в "H_" таблице
  l_H_cnt         number;       -- кол-во записей в крайней партиции "H_" таблицы (Было записей перед апдейтом)
  l_dayDiff       number;       -- настроечная таблица - насколько дней могут отстоять снапшот и накопленная история

  l_keyFiledOne    varchar2(300); -- как бы первичный ключ. будет сипользоваться в определени новых или удаленных. перспективно можно создать локальный индекс по нему 
  l_colList        varchar2(6000);-- все колонки набора кроме исключаемых в перечисление insert(...)
  l_keyFields12    varchar2(300); -- колонки отмеченные как ключевые в наборе clset_id clmtp_id in (1,2) -- не допускают null
  l_keyFields3     varchar2(300); -- колонки отмеченные как ключевые в наборе clset_id clmtp_id in (3)   -- допускают null
  l_whereKeyFlds12 varchar2(600); -- колонки отмеченные как ключевые в наборе clset_id типом clmtp_id in (1,2) -- не допускают null
  l_whereKeyFlds3  varchar2(600); -- колонки отмеченные как ключевые в наборе clset_id типом clmtp_id in (3)   -- допускают null
begin
   job_log_psv.start_log(in_filial_id, $$plsql_unit, 'Начали. филиал='||in_filial_id||', таблица '||in_scr_table||', набор '||in_clset_id||', дата '||in_snap_date);
   job_log_psv.write_detail_log(' ');
   -- выставляем блокировку, чтобы запретить вызов пакета для данного филиала в другом процессе
---   DBMS_LOCK.allocate_unique (lockname => USER || '_' || $$plsql_unit, lockhandle => v_lockhandle);
   -- ждём, пока блокировка не будет снята
/*   WHILE DBMS_LOCK.request (lockhandle => v_lockhandle) <> 0
   LOOP
     DBMS_LOCK.sleep (60);
   END LOOP;*/
   
   l_runSql('alter session force parallel query parallel '||to_char(gpv_ParallelCount));
   l_runSql('alter session force parallel DDL   parallel '||to_char(gpv_ParallelCount));
   -- Исходная таблица: можно работать со входной таблицей? и в какую собирать данные? --------------------------------------------------
   l_clset_id := in_clset_id;
   readAndCheckTabParam( in_scr_table, in_snap_date, l_clset_id
      -- из m_mkhst_tables   -- инициализируем общие переменные
      ,l_mkhst, l_target_tab, l_dayDiff, l_snapDate 
      -- из d_mkhst_col_sets -- инициализируем переменные для списка колонок
      ,l_keyFiledOne      ,l_colList 
      ,l_keyFields12      ,l_keyFields3 
      ,l_whereKeyFlds12   ,l_whereKeyFlds3 
   );  
   
   -- раскладываем имя вход.таблицы на компоненты
   decodeTabName(in_scr_table, l_owner, l_source_tab  /*---<---------*/, l_link);
   
   -- проверям кол-во записей на лимит и принадлежность одной партиции   
   checkSnapTable(to_number(in_filial_id), in_scr_table, l_snapDate,
     l_snap_SubPart, l_snapCnt
   );

   -- Целевая таблица: как называется последняя субпартиция в целевой таблице? -------------------------------------------------------
   if l_getLastPartition(l_target_tab, in_filial_id, l_H_lastSubPart/*---<---------*/ )=res_error then
     l_message:='Ошибка получения имени последней субпартиция целевой таблице:'||l_H_lastSubPart;
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000, l_message);
	 end if;
   -- какой последний день помещен в снимок?
   execute immediate 'select max(h_START_DATE), count(1) 
                      from '||l_target_tab||' subpartition ('||l_H_lastSubPart||')' 
                 into l_H_lastDate /*---<---------*/
                      ,l_H_cnt;
   job_log_psv.write_detail_log('В последней субпартиции "'||l_H_lastSubPart||'" филиала "'||in_filial_id
                               ||'" целевой таблицы "'||lower(l_target_tab)||'" дата "'||to_char(l_H_lastDate,'dd.mm.yyyy hh24:mi:ss')
                               ||'" записей '||lower(l_H_cnt), l_H_cnt);
   
   -- проверка соответствия целевого агрегата и исходной структуры.----------------------------------------------------------------------------
   -- простые разногласия исправим сразу в процедуре
   -- сложные - сгенерируем исключении и опишем в выходной переменной+логе
   if s101_CheckAbility2use(l_clset_id, l_owner||'.'||l_source_tab, l_sqlClob, l_target_tab, gpc_CheckAndAlter,in_filial_id) = res_error then 
     l_message:= substr(l_sqlClob,1,3999);
 	   raise_application_error(-20000,l_message);
	 end if;
   
   -- проверяем можно ли добавлять данные снимка в историю ? 0-приемник пуск, Exception - не укладываемся в лимит или залезли в прошлое
   -- если все нормально возвращается res_success (1)
   if checkDayDistance(l_snapDate, l_H_lastDate, l_dayDiff, in_filial_id, l_mkhst)=0 then 
     job_log_psv.write_detail_log('Целевая таблица пустая - просто закачиваем снимок в H_ таблицу по правилам');
     l_runSql('  insert into '||l_target_tab||'
                   (h_start_date, h_end_date,
                   '||l_colList||'
                   )
                 select 
                   '||dat2str(l_snapDate)||', '||dat2str(to_date('31.12.2999','dd.mm.yyyy'))||',
                   '||l_colList||'
                 from 
                   '||l_owner||'.'||l_source_tab||' subpartition ('||l_snap_SubPart||') s');
                 l_cnt := SQL%rowcount;
     commit;
     l_message := 'Закончили работу - впервые вставлено '||num2ch(l_cnt)||' строк';
     job_log_psv.write_detail_log (l_message, l_cnt);
     job_log_psv.end_log (l_message);
     l_logResult(l_mkhst , in_filial_id, l_snapDate, l_clset_id, l_target_tab, '2End', 1, l_cnt, 0, l_cnt, 0, 0, 0, l_message);
     -- соберем статистики
		 job_log_psv.write_detail_log('подсоберем статистики по нашей таблице "'||l_target_tab||'" и партиции "'||l_H_lastSubPart||'"');
     DBMS_STATS.gather_table_stats (USER, l_target_tab, partname =>l_H_lastSubPart, estimate_percent => 1, degree => gpv_parallelCount);
     -- Снимаем блокировку и выходим
     v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
     return;
   end if; 
   
   -- Если один из ключевых столбцов источника! может содержать пустые значения, то анализа истории не получится.
   -- почти все будет считаться новым!
   checkColumns_clmtp(l_clset_id, l_keyFields12, l_keyFields3,  l_owner, l_source_tab, l_snap_SubPart,    l_sqlClob);
   -- Если ключевой столбец источника начали заполнять только в отчетном месяце, то в истории он еще пустой - анализа истории не получится.
   -- почти все будет считаться новым!
   checkColumns_clmtp(l_clset_id, l_keyFields12, l_keyFields3,  user,    l_target_tab, l_H_lastSubPart,    l_sqlClob);
   
  
   job_log_psv.write_detail_log('Приступаем к анализу изменений. Ключевые поля: '||l_keyFields12||' первого типа(обязательного).'
                            ||case when length(l_keyFields3)>0 then ' Ключевые поля третьего типа(допускающего null)'||l_keyFields3 else '' end);
   <<p3>> -- генерируем селекта который соединяет снапшот и крайнюю партицию в истории
   -- результат - для каждой записи одно из 4х значений: 0-неизменная\1-новая\2-удалена\3-изменилась 
   l_colList := ''; -- запишем список колонок, в том порядке, который вернет встроенный ниже loop
   --открыли грандиозный селект
   l_sqlClob := 'with'||crlf
     ||'  src as (select * from '||l_owner||'.'||l_source_tab||' subpartition ('||l_snap_SubPart||'))'||crlf
     ||' ,hst as (select * from '||l_target_tab||' subpartition ('||l_H_lastSubPart||') where h_end_date>sysdate)'||crlf
     ||' ,alc as (select '||l_keyFields12||case when length(l_keyFields3)>0 then ', '||l_keyFields3 else '' end||crlf
     ||'          from '||l_owner||'.'||l_source_tab||' subpartition ('||l_snap_SubPart||') union 
                  select '||l_keyFields12||case when length(l_keyFields3)>0 then ', '||l_keyFields3 else '' end||crlf
     ||'          from '||l_target_tab||' subpartition ('||l_H_lastSubPart||') where h_end_date>sysdate)'||crlf
     ||'select nvl(hst.h_start_date, '||dat2str(l_snapDate)||')h_start_date, to_date(''31.12.2999'',''dd.mm.yyyy'')'||crlf
     ||' ,alc.'||replace(l_keyFields12, ',', ',alc.')
     ||case when length(l_keyFields3)>0 then ', alc.'||replace(l_keyFields3, ',', ',alc.') else '' end||crlf;
   -- начали наполнять грандиозный селект полями
   for i in (select * from d_mkhst_col_sets t
              where clset_id=l_clset_id
                 and svmtd_svmtd_id > 0
                 and clmtp_clmtp_id is null
                 and l_snapDate between start_date and end_date-1/86400
              order by column_comm)
   loop
     -- для тела селекта список полей, которые надо анализировать. Берем исходное значение, т.к. новое будет взято из S_ снимка
		 l_sqlClob := l_sqlClob ||  --1  сохранять, если изменилось всегда /когда требуется контролировать все значения      
           case when i.svmtd_svmtd_id=1 then ' /*1*/,nvl(src.'||i.column_name||', hst.'||i.column_name||')'||crlf
						                    --2  сохранять, если изменилось только когда дата снимка = последний день месяца /например баланс на конец месяца
						    when i.svmtd_svmtd_id=2 then ' /*2*/,nvl(src.'||i.column_name||', hst.'||i.column_name||')'||crlf
								                --3  обновлять историю на последнее актуальное значение / например время жизни в сети
                when i.svmtd_svmtd_id=3 then ' /*3*/,nvl(src.'||i.column_name||', hst.'||i.column_name||')'||crlf
								                --4  сохранять только первое не пустое значение, дальнейшие изменения игнорировать  /например INIT_ аналитики
                when i.svmtd_svmtd_id=4 then ' /*4*/,nvl(src.'||i.column_name||', hst.'||i.column_name||')'||crlf
								                --5  Обновлять. добавляя к уже накопленному от start_date или открывая новую историю.  /например, sum(DURATION) за день при добавлении в историю
                when i.svmtd_svmtd_id=5 then ' /*5*/,nvl(src.'||i.column_name||',0)+nvl(hst.'||i.column_name||',0)'||crlf
						else ' !!! неизвестный метод-'||i.svmtd_svmtd_id||' !!! ' end;
     -- для списка полей в шапке insert into table(...l_col_list...)
     l_colList := l_colList ||','|| i.column_name;
     -- суммирование 1/0 если метод сохранения требует открытия новой инсторической записи
     l_msgCase := l_msgCase || 
           case when i.svmtd_svmtd_id=1 then crlf||'+decode(src.'||i.column_name||', hst.'||i.column_name||', 0, 1)'
                when i.svmtd_svmtd_id=2 and trunc(l_snapDate,'month') between l_H_lastDate and l_snapDate --trunc(last_day(l_snapDate)) 
									                      then crlf||'+decode(src.'||i.column_name||', hst.'||i.column_name||', 0, 1)'
                when i.svmtd_svmtd_id=2 then crlf||'+case when hst.h_start_date<'||dat2str(trunc(l_snapDate,'month'))||crlf
   					                                     ||'      then decode(src.'||i.column_name||', hst.'||i.column_name||', 0, 1) else 0 end'
                when i.svmtd_svmtd_id=4 then crlf||'+case when src.'||i.column_name||' is null and hst.'||i.column_name||' is not null then 1 else 0 end'
									                         -- '+nvl2(src.'||i.column_name||',0,nvl2(hst.'||i.column_name||',1,0))'
   						  else null end;
     -- конкатенация названий полей в MOSTLY3CHANGE_COLUMNS
     l_msgCaseFld := l_msgCaseFld ||
           case when i.svmtd_svmtd_id=1 then crlf||'||decode(src.'||i.column_name||', hst.'||i.column_name||', null, '','||i.column_name||''')'
                when i.svmtd_svmtd_id=2 and trunc(l_snapDate,'month') between l_H_lastDate and l_snapDate --trunc(last_day(l_snapDate)) 
									                      then crlf||'||decode(src.'||i.column_name||', hst.'||i.column_name||', null, '','||i.column_name||''')'
                when i.svmtd_svmtd_id=2 then crlf||'||case when hst.h_start_date<'||dat2str(trunc(l_snapDate,'month'))||crlf
   					                                     ||'      then decode(src.'||i.column_name||', hst.'||i.column_name||', null, '','||i.column_name||''') else null end'
                when i.svmtd_svmtd_id=4 then crlf||'||case when src.'||i.column_name||' is null and hst.'||i.column_name||' is not null then '','||i.column_name||''' else null end'
									                         -- '+nvl2(src.'||i.column_name||',0,nvl2(hst.'||i.column_name||',1,0))'
   						  else null end; 
	 end loop;
   -- закрыли грандиознвй селект
   l_sqlClob := l_sqlClob 
     ||' ,case when hst.'||l_keyFiledOne||' is null then 1 /*new record*/'||crlf
		 ||'      when src.'||l_keyFiledOne||' is null then 2 /*deleted record*/'||crlf
 		 ||'	    when '||'(0 '||l_msgCase||') '||crlf
  	 ||'	             >0 then 3 /*changed in monitor field*/'||crlf
 		 ||'	else 0 end sub1part2type3column4 /*ключ партиционирования в темповой таблице, куда будет помещен результат*/'||crlf
     ||' ,case when hst.'||l_keyFiledOne||' is null or  src.'||l_keyFiledOne||' is null then null'||crlf
     ||'       else substr('||substr(l_msgCaseFld, 5, length(l_msgCaseFld)-4) ||',1,3999) end MOSTLY3CHANGE_COLUMNS '||crlf
     ||'from src, hst, alc'||crlf
     ||'where '||l_whereKeyFlds12
               ||case when length(l_whereKeyFlds3)>0 then ' and '||l_whereKeyFlds3 else '' end||crlf
     ||'  and '||replace(l_whereKeyFlds12,'src.','hst.')
               ||case when length(l_whereKeyFlds3)>0 then ' and '||replace(l_whereKeyFlds3,'src.','hst.') else '' end||crlf
     ||crlf;
   -- пишем заголовок грандиозного селекта т.к. только сейчас известен порядок столбцов при вставке
   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date,'||l_keyFields12
                              ||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
                              ||l_colList||',sub1part2type3column4, MOSTLY3CHANGE_COLUMNS)'||crlf||l_sqlClob;   
   
  
   <<p4>> --================ формируем результат
   job_log_psv.write_detail_log('Начинаю выполнять запрос поиска отличий истории и снимка(добавление в конец)');
   l_runSql('alter table '||l_target_tab||'_tmp truncate partition(P'||in_filial_id||')');
   --dbms_output.put_line(l_msgClob);
   l_runSql(l_sqlClob);
   commit;
   -- фиксируем что получилось
   execute immediate 'select 
                         sum(case when sub1part2type3column4=0 then 1 else 0 end) uncg_cnt,
                         sum(case when sub1part2type3column4=1 then 1 else 0 end) new_cnt,
                         sum(case when sub1part2type3column4=2 then 1 else 0 end) del_cnt,
                         sum(case when sub1part2type3column4=3 then 1 else 0 end) chng_cnt
                       from '||l_target_tab||'_tmp partition (p'||in_filial_id||')' 
   into l_n0, l_n1, l_n2, l_n3;
   execute immediate 'select count(1)'||crlf
          ||'from ('||crlf
          ||'   select distinct '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
          ||'     from '||l_target_tab||'_tmp subpartition (sp_chng_'||in_filial_id||')'||crlf
          ||')'
   into l_n3unq;

   job_log_psv.write_detail_log('обнаружено: неизменных('||num2ch(l_n0)
     ||') новых('||num2ch(l_n1)
     ||') удаленных('||num2ch(l_n2)
     ||case when l_n3 = l_n3unq then ') изменившихся('||num2ch(l_n3)||')'
                                else ') изменившихся('||num2ch(l_n3)||', с том числе уникальных '||num2ch(l_n3unq)||')'
       end																	
     , l_n0+l_n1+l_n2+l_n3);
   
      
   <<p5>> --запрос слияния истории
   -- магия. Выполняется расчет новой порции данных для истории взамен существующей
   job_log_psv.write_detail_log('запрос слияния истории (1я порция) неизменные, новые, закрывшиеся');

   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date, '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||',sub1part2type3column4)'||crlf;
   l_sqlClob := l_sqlClob || -- неизменные и новые копируем как есть из _TMP таблицы
      'select --неизменные и новые копируем как есть из _TMP таблицы'||crlf
    ||' h_start_date, h_end_date, '
    ||  l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||', -2 sub1part2type3column4 '||crlf
    ||'from '||l_target_tab||'_tmp partition (p'||in_filial_id||') where sub1part2type3column4 in (0, 1)'||crlf
    ||'union all'||crlf
    -- закрывшиеся копируем из _TMP с заменой END_DATE = SNAP_DATE-1/86400
    ||'select -- закрывшиеся копируем из _TMP с заменой END_DATE = SNAP_DATE-1/86400'||crlf
    ||' h_start_date, '||dat2str(l_snapDate-1/86400)||' h_end_date,'||crlf
    ||  l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||', -2 sub1part2type3column4 '||crlf
    ||'from '||l_target_tab||'_tmp subpartition (sp_delt_'||in_filial_id||')';
    l_runSql(l_sqlClob);
    l_tmp := SQL%rowcount;
    commit;
    
    -- изменившиеся сначала копируем из l_H_lastSubPart с заменой End_date = SNAP_DATE-1/86400
   job_log_psv.write_detail_log('запрос слияния истории (2я порция)-изменившиеся закрываем');
   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date, '||crlf
    ||     l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||',sub1part2type3column4)'||crlf
    ||'select -- изменившиеся сначала копируем из l_H_lastSubPart с заменой End_date = SNAP_DATE-1/86400'||crlf
    ||'  alc.h_start_date, '||dat2str(l_snapDate-1/86400)
    ||', alc.'||replace(l_keyFields12,',',',alc.')||case when length(l_keyFields3)>0 then ', alc.'||replace(l_keyFields3,',',',alc.') else '' end ||crlf
    ||'      '||replace(l_colList,',',',alc.')||', -2 sub1part2type3column4'||crlf
    ||' from '||l_target_tab||' subpartition ('||l_H_lastSubPart||') alc,'||crlf
    ||'      (select distinct '||l_keyFields12 ||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end
     --т.к при появлении значений в новых снимках детализирующих ключевое поле в истории-одна запись, а в новой снимке - две и более
    ||'       from '||l_target_tab||'_tmp subpartition (sp_chng_'||in_filial_id||')) src'||crlf
    ||' where alc.h_end_date>sysdate and '||crlf
               ||replace(l_whereKeyFlds12,'(+)','')
               ||case when length(l_whereKeyFlds3)>0 then ' and '||l_whereKeyFlds3 else '' end;
    l_runSql(l_sqlClob);
    l_cnt := SQL%rowcount; 
    l_tmp := l_tmp + l_cnt;
    commit; 
    if l_cnt not in (l_n3,l_n3unq) then
			l_message:='Ошибка: вставка в результат "изменившихся закрываемых" записей прошла в количестве '||num2ch(l_cnt)
                 ||'. При этом ожидалось, что вставится '||num2ch(l_n3)
                 ||case when l_n3<>l_n3unq then ' или '||num2ch(l_n3unq) else '' end||crlf
                 ||'Подумайте над неизменными колонками clmtp_clmtp_id в наборе. Возможно одна из них не отмечена флагом. Возможно начала формироваться детализация внутри вашего набора clmtp_clmtp_id'||crlf
                 ||'Подробности как выявить такие записи - в логе.'||crlf;
      job_log_psv.write_detail_log(l_message);
      job_log_psv.write_detail_log('попробуйте следующий запрос для выявления записей приведших к задвоениям');
      job_log_psv.write_detail_log( 'select '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||', count(1)'||crlf
                              ||'from '||l_target_tab||'_tmp '||crlf
                              ||'where filial_id='||in_filial_id||' and sub1part2type3column4 not in (-1, -2) /*-1 копия, -2 результат*/'||crlf
                              ||'group by '||crlf
                              ||'  '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
                              ||'having count(1)>1');
 	    raise_application_error(-20000,l_message);
		end if;

    -- изменившиеся затем копируем из нового снимка с установкой start_date = SNAP_DATE
   job_log_psv.write_detail_log('запрос слияния истории (3я порция)-изменившиеся вставляем');
   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date, '||crlf
    ||     l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||',sub1part2type3column4)'||crlf
    ||'select -- изменившиеся затем копируем из нового снимка с установкой start_date = SNAP_DATE'||crlf
    ||' '||dat2str(l_snapDate)||' h_start_date,'||dat2str(to_date('31.12.2999','dd.mm.yyyy'))
    ||', alc.'||replace(l_keyFields12,',',',alc.')||case when length(l_keyFields3)>0 then ', alc.'||replace(l_keyFields3,',',',alc.') else '' end ||crlf
    ||'      '||replace(l_colList,',',',alc.')||', -2 sub1part2type3column4'||crlf
    ||' from '||l_owner||'.'||l_source_tab||' subpartition ('||l_snap_SubPart||') alc,'||crlf
    ||'      (select distinct '||l_keyFields12 ||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end
     --т.к при появлении значений в новых снимках детализирующих ключевое поле в истории-одна запись, а в новой снимке - две и более
    ||'       from '||l_target_tab||'_tmp subpartition (sp_chng_'||in_filial_id||') where h_end_date>sysdate) src'||crlf
    ||' where '||crlf
               ||replace(l_whereKeyFlds12,'(+)','')
               ||case when length(l_whereKeyFlds3)>0 then ' and '||l_whereKeyFlds3 else '' end;
    l_runSql(l_sqlClob);
    l_cnt := SQL%rowcount; 
    l_tmp := l_tmp + l_cnt;
    commit; 
    if l_cnt not in (l_n3,l_n3unq) then
			l_message:='Ошибка: вставка в результат "изменившихся открываемых" записей прошла в количестве '||num2ch(l_cnt)
                 ||'. При этом ожидалось, что вставится '||num2ch(l_n3)
                 ||case when l_n3<>l_n3unq then ' или '||num2ch(l_n3unq) else '' end||crlf
                 ||'Подумайте над неизменными колонками clmtp_clmtp_id в наборе. Возможно одна из них не отмечена флагом. Возможно начала формироваться детализация внутри вашего набора clmtp_clmtp_id'||crlf
                 ||'Подробности как выявить такие записи - в логе.'||crlf;
      job_log_psv.write_detail_log(l_message);
      job_log_psv.write_detail_log('попробуйте следующий запрос для выявления записей приведших к задвоениям');
      job_log_psv.write_detail_log( 'select '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
                              ||'from '||l_target_tab||'_tmp '||crlf
                              ||'where filial_id='||in_filial_id||' and sub1part2type3column4<>-1'||crlf
                              ||'group by '||crlf
                              ||'  '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
                              ||'having count(1)>1');
 	    raise_application_error(-20000,l_message);
		end if;

   l_sqlClob := '';
   job_log_psv.write_detail_log('получили результат слияния '||num2ch(l_tmp)||' строк', l_tmp);
   commit;

   -- ну как? получилось?
   job_log_psv.write_detail_log('(Получилось) ?= (неизменные)+(новые)+(закрылись)+(изменились.закрылись + изменились.открылись)');
   job_log_psv.write_detail_log('('||num2ch(l_tmp)||') ?= ('||num2ch(l_n0)||')+('||num2ch(l_n1)||')+('||num2ch(l_n2)||')+('||num2ch(l_n3)||'+'||num2ch(l_n3unq)||') ?= '||num2ch(l_n0+l_n1+l_n2+l_n3+l_n3unq));
   if l_tmp <> (l_n0+l_n1+l_n2+l_n3+l_n3unq) then
		 l_message := 'Ошибка. Полученный результат не соответствует ожидаемому по колву строк. разница ('||num2ch( l_tmp-(l_n0+l_n1+l_n2+l_n3+l_n3unq))
      ||'). Подробности в логе. Подумайте над неизменными колонками clmtp_clmtp_id в наборе. Возможно одна из них не отмечена флагом. Возможно начала формироваться детализация внутри вашего набора clmtp_clmtp_id';
     job_log_psv.write_detail_log(l_message, l_n0+l_n1+l_n2+l_n3+l_n3unq );
     raise_application_error(-20000, l_message);
   end if;
   
   -- выполняем слияние
   job_log_psv.write_detail_log('Получилось. было '||num2ch(l_h_cnt)||' стало '||num2ch(l_n0+l_n1+l_n2+l_n3+l_n3unq)||'. '
                                          ||num2ch(((l_n0+l_n1+l_n2+l_n3+l_n3unq)-l_h_cnt)*100/l_h_cnt, 2)||'% строк добавится. '
                                          ||num2ch((l_n1+l_n3)/l_h_cnt, 3)||'% строк новых'); 
   <<p2>> -- сохраняем данные крайней секции для отката 
   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date, '||crlf
              ||     l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
              ||' '||l_colList||',sub1part2type3column4)'||crlf
              ||'select h_start_date, h_end_date, '||crlf
              ||     l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
              ||' '||l_colList||',-1 sub1part2type3column4 /*0=копия данных для отката*/'||crlf
              ||'from '||l_target_tab||' subpartition ('||l_H_lastSubPart||') '||crlf
              ||'where h_end_date>sysdate';
   l_runSql(l_sqlClob);
   commit;
   -- сохраням ключевые параметры перед обновлением 
   lpv_chk_mode := lpc_ByEnd;
   if checkData(lpc_collectData, l_target_tab, l_H_lastSubPart, l_clset_id,l_H_lastDate)<> res_success then 
     null; --job_log_psv.write_detail_log('что-ьл пошло не так!');
   end if;
   -- очистка крайней партиции
   l_sqlClob := 'alter table '||l_target_tab||' truncate subpartition ('||l_H_lastSubPart||')';
   --job_log_psv.write_detail_log(l_sqlClob);
   l_runSql(l_sqlClob);
   -- вставка данных
   l_sqlClob := 'insert into '||l_target_tab||'(h_start_date, h_end_date,'|| l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end
     ||l_colList||')'||crlf
     ||'select h_start_date, h_end_date,'|| l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end
     ||l_colList||crlf
     ||' from '||l_target_tab||'_tmp subpartition (sp_rslt_'||in_filial_id||')';
   --job_log_psv.write_detail_log(l_sqlClob);
   l_runSql(l_sqlClob);
   commit;

   -- пора делать split partition ? - когда в крайней партиции начинают появлятся дубли по ключевым полям
   checkResAndSplitPartition(in_filial_id, l_target_tab, l_H_lastSubPart, l_keyFields12, l_keyFields3 );

   -- проверяем что получилось после вставки. необходимый контроль новой порции выл выполнен выше. теперь еще раз на рез-т
   if checkData(lpc_checkData, l_target_tab, l_H_lastSubPart, l_clset_id,l_H_lastDate)<> res_success then 
     l_message := 'что-то пошло не так! Запретим дальнейшую работу по этому набору с этой таблицей!';
     job_log_psv.write_detail_log(l_message||' не совпали данные по исторической таблице после обновления с теми данными которые там были до втавки новой порции. Откатить изменения можно так:'||crlf
        ||'alter table '||l_target_tab||' subpartition '||l_H_lastSubPart||';'||crlf
        ||'insert into '||l_target_tab||'(h_start_date, h_end_date,'|| l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||l_colList||')'||crlf
        ||                        'select h_start_date, h_end_date,'|| l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||l_colList||crlf
        ||'from '||l_target_tab||'_tmp subpartition (sp_copy_'||in_filial_id||');');
     update m_mkhst_tables t 
        set t.enabl = 0
      where t.mkhst_id=l_mkhst;
      commit;
     raise_application_error(-20000, l_message||' подробности смотрите в логе');
   end if;

   -- соберем статистику
   if to_char(l_snapDate,'dd')in('01','11','22') or trunc(l_snapDate)=trunc(last_day(l_snapDate)) then 
		 job_log_psv.write_detail_log('подсоберем статистики по нашей таблице "'||l_target_tab||'" и партиции "'||l_H_lastSubPart||'"');
     DBMS_STATS.gather_table_stats (USER, l_target_tab, partname =>l_H_lastSubPart, estimate_percent => 1, degree => gpv_parallelCount);
   end if;
   
   -- конец. кто разобрал до конца - молодец!
   l_message := 'Перезалили '||num2ch(l_n0+l_n1+l_n2+l_n3+l_n3unq)||' строк ('||num2ch(((l_n0+l_n1+l_n2+l_n3+l_n3unq)-l_h_cnt)*100/l_h_cnt, 2)
                ||'%). Неизм('||num2ch(l_n0)||'), новых('||num2ch(l_n1)||'), удал('||num2ch(l_n2)||'), изм.закр('||num2ch(l_n3)||'), изм.откр('||num2ch(l_n3unq)||')';
   
   job_log_psv.write_detail_log(l_message, l_n0+l_n1+l_n2+l_n3+l_n3unq);

   l_logResult(l_mkhst , in_filial_id, l_snapDate, l_clset_id, l_target_tab, '2End', 1,
                l_n0+l_n1+l_n2+l_n3+l_n3unq, l_n0, l_n1, l_n2, l_n3, l_n3unq, l_message);

   job_log_psv.end_log (l_message);
   -- Снимаем блокировку
   v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
EXCEPTION
   WHEN OTHERS THEN
      -- Снимаем блокировку
      v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
      l_message := substr(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace(),1,1800);
      job_log_psv.write_detail_log(l_message); -- текст исключения
      if length(l_sqlClob)>0 and (lower(l_sqlClob) like '%select %' or lower(l_sqlClob) like '%insert%' or lower(l_sqlClob) like '%alter%') then 
				  job_log_psv.write_detail_log(l_sqlClob); --текст возможного SQL
      end if;          
      job_log_psv.end_log_with_error (in_error => l_message);
      l_logResult(l_mkhst , in_filial_id, l_snapDate, l_clset_id, l_target_tab, '2End', sqlcode,
                l_n0+l_n1+l_n2+l_n3+l_n3unq, l_n0, l_n1, l_n2, l_n3, l_n3unq, l_message);
      RAISE;
END;
  

end;
/
