CREATE OR REPLACE PACKAGE PUB_DEV.make_hist_from_s
AUTHID CURRENT_USER
IS

-- 20.12.2016 �������� �.�. ������ ������ ������
-- 13/01/2017 �������� �.�. �������������� ����������� �������
-- 04/02/2017 �������� �.�. ������� ���������� �� ���������� "�" �������� � ��������� ���������� 
-- ����������� ����� �������������� "�������� ���������� � ����������", ��������� � ������������ ������


/*   ����� ������� �� ��������� ������ ������������
  
  ---------- ����������� ���������:---------------------------------------------------
    1) d_mkhst_columns_sets  
                            - �������������� ��������� ������������� ��� ������� �������
                              ����������� ������������� ����� ����� s102_CreateColumnSetRules
                              �� ���������, ����������� ������ ����� �������� ����.
                              ����������� ��������� ���������� �����
    2) d_mkhst_columns_method 
                            - ���������� ������� ���������� � �������. 
                              ������������ ������ �������. ����������� ����� ��� �� ��������� ������ ������
    3) d_mkhst_columns_def  
                            - ��������� �� ������������ � ������ ������ ��� �������
                              ������������ ������������� ��� ��������� ��� ������ s102_CreateColumnSetRules ��� ������
                              ������ ����������
    4) m_mkhst_tables
                            - �������� ������� ����������� ��� ���������, ���� ���������, �� ������ ������ ������ ������
                              ����� ���������� ����������� ������� ������� ������������ �������.
                              ����� ������� ����� ������ s100_CreateTargetTable.
    
  --------- �������� ������ �� ���������� ������ ������ � ����� �������------------------
    AddSnap2End (ID �������, �������� �������, ����� ������, ����������� ����)
    
  --------- �������� ���������� � ���������� ---------------------------------------------
    in_   ������� ����������, ������������ ��� ��������� � �������
    p_    ���� �����
    o_    ������� ����������, ������������ ���������� � ������� ����� ������� ��� ���������� ��������� ������
    l_    ������� ��������� ���������� ������ �-���
          ��� ������� �������� �-���, ����������� ��� ������
    gpv_  Global Package Variable (���������� ���������� ������ - ������ ������� - � ������������)
    gpc_  Global Package Variable (���������� ��������� ������ - ������ ������� - � ������������)
    lpv_  Local  Package Variable (���������� ���������� ������ - �� ������ �������)
  */

-- ���������� ������ �-���
  res_error         constant number := -1;
  res_success       constant number := 1;

--������ ������ ��������������� �-��� S_xxx 
  gpc_OnlyCheck     constant number      :=0;
  gpc_CheckAndAlter constant number      :=1;

-- ������ ������ �-��� ��������������� EXECUTE IMMEDIATE (������� �����)
  gpc_Exec          constant number      :=1; -- 0001
  gpc_Log           constant number      :=2; -- 0010
  gpc_Print         constant number      :=4; -- 0100
  gpv_runMode                number      :=gpc_Exec; -- ��� ����� ������� ����� �������� �������� 
                                                     -- ����� ���������� gpv_runMode:=gpv_Exec+gpv_Log

-- ����������� ��������� ����� ������������ ��� ���������� ���������������� ��������� � �������������� �������
  gpv_snapLimitCount             number      :=1000; -- ���������� ������� � ������, ������ �������� ������� ������ �������� � �� ������������
  gpv_ParallelCount              number      := 5;   -- ���-�� ���������� �� ������� ����� ����������� �������

-----------------------------------------------------------------------------------------------------
-- ��������������� �-��� �� ������������ ����������� ������ � ����������� ��������
function s100_CreateTargetTable(p_cset_id number, p_source_name varchar2, p_target_name varchar2 default null, o_sql out nocopy clob, p_mode number default 1) return number;
function s101_CheckAbility2use    (p_cset_id     number,   p_source_name varchar2, o_res out nocopy clob, p_target_name varchar2 default null, p_mode number default 0, p_filial_id number default 0) return number;
function s102_CreateColumnSetRules(p_source_name varchar2, o_clset_id out number) return number;

-----------------------------------------------------------------------------------------------------
-- �������� ������� �-��� ���������� ������������� ��� ������� �������
--Procedure AddSnap2start(in_filial_id IN NUMBER default 0, in_snap_date IN VARCHAR2, in_scr_table varchar2);
--Procedure AddSnapInsert(in_filial_id IN NUMBER default 0, in_snap_date IN VARCHAR2, in_scr_table varchar2);
Procedure AddSnap2End(in_filial_id IN NUMBER, in_scr_table varchar2, in_snap_date IN VARCHAR2, in_clset_id number default null);


-----------------------------------------------------------------------------------------------------
-- ���������� ��������������� �-��� - ����� ������ ������ ������ ����� ������� � �������� � ����
function l_getlong( p_tname in varchar2, p_cname in varchar2, p_where in varchar2 ) return varchar2;
function l_getLastPartition(p_source_tab varchar2, p_filial_id number, o_res out varchar) return number;
function add_partition(p_part_table in varchar2, p_filial_id number) return number;
function checkData(p_mode number, 
	p_h_table varchar2, p_h_subpartname varchar2, p_clset_id number, p_h_date date) return number;
  
END;
/
CREATE OR REPLACE PACKAGE BODY PUB_DEV.make_hist_from_s IS

-- 20.12.2016 �������� �.�. ������ ������ ������
-- 13/01/2017 �������� �.�. �������������� ����������� �������
-- 04/02/2017 �������� �.�. ������� ���������� �� ���������� "�" �������� � ��������� ���������� 
-- ����������� ����� �������������� "�������� ���������� � ����������", ��������� � ������������ ������

  crlf              constant varchar2(2) := chr(13)||chr(10);
  
  -- ����� � ������� ������� �-��� "checkData" - ���� ������ ��� ��������
  lpc_collectData   constant number := 1;
  lpc_checkData     constant number := 2;

  -- ������ ����� ���������� ������ �-���� "checkData"
  lpc_ByEnd         constant number := 1;
  lpc_ByStart       constant number := 2;
  lpc_ByDate        constant number := 3;
  
  -- ��������� ������ �-��� �������� ������
  lpv_CollectResult number := 0; -- 0=�� ���������� ���� lpv_chk% ����������
                                 -- 1=����������, �� �� ������ ���������� lpv_chk_sumfld, ������� ��� �����
                                 -- 2=����������, ��� �������

  -- ���������� ���������� � ������� �-��� �������� �������� ����������� ��������
  lpv_chk_mode   number := lpc_ByEnd;-- �����, � ������� �������� ���� ����������� ����������
  lpv_chk_date   date;        -- ���� �� ������� ������� ����������� ���������� ����� ����������� ����� ������� �������
  lpv_chk_maxd   date;        -- ������������ ���� � ������ (h_start_date)
  lpv_chk_cnt    number;      -- ���-�� ������� � ������
  lpv_chk_sum    number;      -- ����� �� ���� - ������� ������� �������� 
  lpv_chk_sumfld varchar2(20);-- �������� ����, �� �������� ������� ����� ����
  

--=========================================================================================================
-- ��������������� ������������� ��������� - �������� �� ����� � UTILS �����
-- ����� � ������ � ��������� ���������, � ���������� ������������� � ��� ���������� �������
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
  -- -- ������� ������� � ������ "�������� ������"
  if p_mode = lpc_collectData then 
    lpv_CollectResult := 0; 
    lpv_chk_date := null; lpv_chk_maxd := null; lpv_chk_cnt:=null; lpv_chk_sum:=null; lpv_chk_sumfld := null;
    -- ���� ���� ������ �������� �� ����� ������ ������ ����� ���� � �������(2)
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
        when NO_DATA_FOUND then null; -- ������ ����������� ����� ����
      end;
    end if; -- ��������� �� ���� ������ ���� � ���������� �������������� � ������������� � �_������

    -- ����������� ���� ������ ���� ��� ������������
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
        when NO_DATA_FOUND then null; -- ������. ��� ����� ���������� �� �������� ����� ������������
      end;
    end if;
   
    lpv_chk_date := p_h_date;
    if l_sumfld is not null then 
      lpv_chk_sumfld := l_sumfld;
      job_log_psv.write_detail_log('������� ��� �������� '||lpv_chk_sumfld||' ���� � �������� l_svmtd='||l_svmtd||', l_clmtp='||l_clmtp||', nullable='||l_nable||', num distinct ='||l_numdist||', num nulls='||l_numnulls); 
               
      if lpv_chk_mode=lpc_ByEnd then 
        execute immediate 'select max(h_start_date), count(1), sum('||lpv_chk_sumfld||')
                          from '||p_h_table||' subpartition ('||p_h_subpartname||')
                          where :d between h_start_date and h_end_date'
        into lpv_chk_maxd, lpv_chk_cnt, lpv_chk_sum
        using lpv_chk_date;
      end if;
      lpv_CollectResult := 2;
    else
      job_log_psv.write_detail_log('�� ������ ����� ���� ��� ���������� �������� ������������'); 
        execute immediate 'select max(h_start_date), count(1)
                          from '||p_h_table||' subpartition ('||p_h_subpartname||')
                          where :d between h_start_date and h_end_date'
        into lpv_chk_maxd, lpv_chk_cnt
        using lpv_chk_date;
      lpv_CollectResult := 1;
   end if;
   job_log_psv.write_detail_log('��������� ��� ��������: �� ����='||to_char(lpv_chk_date)
                             ||', ���� h_start_date='||to_char(lpv_chk_maxd)
                             ||', ���-�� �������='||num2ch(lpv_chk_cnt)
                             ||', ����� (�� '||l_sumfld||')='||to_char(lpv_chk_sum)); 
   return res_success;
    
  -- ������� ������� � ������ �������� ���������
	elsif p_mode=lpc_checkData then 
 	  if lpv_CollectResult=0 then
      job_log_psv.write_detail_log('������� �������� ���������� ��� ���������������� ����� ����������� ����'); 
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
        job_log_psv.write_detail_log('�� �������: '
                         ||case when l_chk_maxd<>lpv_chk_maxd then ' ����� ���� ('||to_char(l_chk_maxd)||')<> ���� ����('||to_date(lpv_chk_maxd)||')'
                                when l_chk_cnt<>lpv_chk_cnt  then ' ����� ������� ('||l_chk_cnt||')<> ���� �������('||lpv_chk_cnt||')'
                                when  l_chk_sum<>lpv_chk_sum then ' ����� ����� ('||l_chk_sum||')<> ���� �����('||lpv_chk_sum||')'
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
        job_log_psv.write_detail_log('�� �������: '
                         ||case when l_chk_maxd<>lpv_chk_maxd then ' ����� ���� ('||to_char(l_chk_maxd)||')<> ���� ����('||to_date(lpv_chk_maxd)||')'
                                when l_chk_cnt<>lpv_chk_cnt  then ' ����� ������� ('||l_chk_cnt||')<> ���� �������('||lpv_chk_cnt||')'
                           else null end
                         ); 
        return res_error;
      end if;
    end if;
    
    return res_success;
    
  -- ������� ������� � ����������� ������ - ������ ������ ���-�� �� �� �������
	else
    job_log_psv.write_detail_log('���������������� ����� ������'); 
    return res_error;
	end if;
end;

----------------------------------------------------------------------------------------------------
-- ��������������� ������������� ��������� - �������� �� ����� � UTILS �����
-- ��������� ���������� ������ "�����.�������@"������" �� ��� ��������� ������ 
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
            dbms_output.put_line('������ ������ ��-���� '||po_link);
          end;
      end;
    end if;
  end if;
end;

----------------------------------------------------------------------------------------------------
-- ��������������� ������������� ��������� - �������� �� ����� � UTILS �����
-- ���������� � ������������� ������� � ����. 
-- ����� �������� ������ � ALL_OBJECTS, �.�. ��������� ������� ������ ������������ ������� ������� �������������
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
-- ��������������� ������������� ��������� - �������� �� ����� � UTILS �����
-- ���������� � ������������� ������� � ����. 
-- ���������� ���� ������ ��� ��������� � ���������� ��������� �����\�������\�����
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
-- ��������������� ������������� ��������� - �������� �� ����� � UTILS �����
-- ����� �� �������� ���� ��������� ����� ���������� ��� ������������ ������ ������������� �������
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
--��������������� ������������� ��������� - �������� �� ����� � UTILS �����
-- ���������� ��� LONG �� ������������ �������������� �����. ������� ���������� �������������� � ������
-- ����� �� ������������ - ����� ��� ���-�� ����������\��������
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
--��������������� ������������� ��������� - �������� �� ����� � UTILS �����
-- ��� ������������������ (��� �����������) ������ �� RANGE �� ����� ��������� "����������" ����� ���������� ����� ���������
-- � ������ SPLIT ��� ������� ��������. �������� ����� �������� �� ������� "��� �������" + "_" + "yyyymm"
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
       return -1; -- �� ����������������� �������
  end;
 
  if l_t<>'LIST' then 
    job_log_psv.write_detail_log('�� �������������� ��� ����������������� - '||l_t||' ������������ ����������������� �� �������� ������� LIST');
    return -2;
  end if;
  
  if l_st<>'RANGE' then 
    job_log_psv.write_detail_log('�������������������� �� ����� �� ������� RANGE - '||l_st ||'. �������');
    return -3;
  end if;

  if l_pc<3 then 
    job_log_psv.write_detail_log('������� ����� ���-�� ��������('||l_pc||'), ����� ��������� ������ � ���������');
    return -4;
  end if;

  -- ��������� �������� ��������� ����������� � ���� ������� (���������� ����� � Alter)
  if l_getLastPartition(l_tab, p_filial_id , l_lpname)<> res_success then 
		job_log_psv.write_detail_log('�� ���� ��������� ������� ����������� � ������� '||p_filial_id);
    return -5;
	end if;
  
  -- ��������� ����� ������������� ����������� (�� ��� ����� �������� High_Value) 
  select max(SUBPARTITION_POSITION)
  into l_delta
  from user_tab_subpartitions 
  where 1=1
     and table_name = upper(p_part_table)
     and partition_name= (select partition_name from user_tab_subpartitions 
                          where table_name = upper(p_part_table) and subpartition_name=l_lpname)
     and subpartition_name <> l_lpname;
  
  -- ������ High_Value (������ � �����)
  l_pt3 := l_getlong('user_tab_subpartitions', 'HIGH_VALUE', 'table_name ='''||l_tab||''' and SUBPARTITION_POSITION = '||to_char(l_delta-1));
  -- ������ High_Value (������ � �����)
  l_pt2 := l_getlong('user_tab_subpartitions', 'HIGH_VALUE', 'table_name ='''||l_tab||''' and SUBPARTITION_POSITION = '||to_char(l_delta-0));
  
  --------------------------------------------------
  if upper(trim(l_pt3)) like 'TO_DATE%' then -- ���� �������������������� �� �����
    l_err := 'select MONTHS_BETWEEN('||l_pt2||','||l_pt3||') from dual';
  else                                       -- ���� �������������������� �� ������
    l_err := 'select '||l_pt2||'-'||l_pt3||' from dual';
  end if;
  begin
    execute immediate l_err into l_delta;
    job_log_psv.write_detail_log('���������� ����� ������� ����������� HIGH_VALUE, l_delta='||l_delta);
  exception
    when others then 
      job_log_psv.write_detail_log(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace());
      job_log_psv.write_detail_log('�� ���������� ��������� ������ ����� ('||l_pt2||') and ('||l_pt3||') �� ��������� ('||l_err||')');
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
      job_log_psv.write_detail_log('�� ���������� ����� �������� l_newpoint �� ��������� ('||l_err||')');
      return -7;
   end;

  --------------------------------------------------
  -- �������� �������� ����������� �� ������� P(YYYYMM)_(p_filial_id)
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
    
  job_log_psv.write_detail_log('��������� ������� ����������� '||l_lpname||' �� ���� '||l_newpoint||' � ��� �����:P'||l_suffix||'_'||p_filial_id||','||l_lpname||' ��������='||l_err);
  
  execute immediate l_err;
  return 0;
  
exception 
  when others then
    job_log_psv.write_detail_log(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace());
    return sqlcode;
end;




-------------------------------------------------------------------------------------------------------
-- ������ ��� ��������� ����������� � p_source_tab �� ������� p_filial_id
function l_getLastPartition(p_source_tab varchar2, p_filial_id number, o_res out varchar) return number
is
  l_owner       varchar2(50); -- �������� p_source_tab
  l_source_tab  varchar2(50); -- ������� � ������� ���� ��������� �������� � ��������� ������� p_filial_id
  l_link        varchar2(50); -- ��������� ���� �� p_source_tab / ��������� ����������
  l_partField   varchar2(50); -- ���� �� �������� ���������� ���������������� �������
  l_part_name   varchar2(50); -- ��� ��������, ������� �������� ������ �������
  l_cnt         number;
begin
  -- ������������ ��� ����.������� �� ����������
  decodeTabName(p_source_tab, l_owner, l_source_tab, l_link);
	select count(1), max(PARTITIONING_TYPE) into l_cnt, l_link
    from all_part_tables 
   where owner=l_owner 
     and table_name = l_source_tab;
     
  if l_cnt=0 then 
		o_res := '������: ������� �� �������� ������������������';
		return res_error;
  end if;
  if l_link<>'LIST' then 
		o_res := '������: ������� ������������������ �� �� ������ LIST';
		return res_error;
  end if;
  
  select column_name into l_partField from ALL_PART_KEY_COLUMNS  where owner=l_owner and name = l_source_tab;
  if l_partField <> 'FILIAL_ID' then 
		o_res := '������: ����������������� �� �� ���� FILIAL_ID';
		return res_error;
  end if;

  -- ���������� �������� �������
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

  -- ���������� ��� ��������� �����������
  select max(SUBPARTITION_NAME) keep (dense_rank last order by subpartition_position) into o_res
  from all_tab_subpartitions 
  where 1=1
    and table_owner= l_owner
    and table_name = l_source_tab
    and partition_name = l_part_name;
  
  return res_success;
end;

-------------------------------------------------------------------------------------------------------
-- ������ ���-�� ����� varchar2(20) ��� number(10,2)
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
    job_log_psv.write_detail_log(p_sql); --����� ���������� SQL
		raise;		
end;

---------------------------------------------------------------------------------------------------------------
-- ���������� � ������� �����������
procedure l_logResult(p_mkhst number, p_filial number, p_snapDate date, 
    p_clset number, p_target_tab varchar2, p_addMethod varchar2, p_exitCode number,
	  p_total number, p_unch number, p_new number, p_closed number, p_chng_cls number, p_chng_open number, 
    p_comm varchar2)
is
  l_cnt   number;
  l_eCode number;
begin
  job_log_psv.write_detail_log('�������� ����������');
  
  select count(1), max(exitCode)  
    into l_cnt, l_eCode
  from f_mkhst_results 
  where mkhst_mkhst_id = p_mkhst and filial_id = p_filial and snapdate=p_snapDate;
  
  if (l_cnt>0 and l_eCode>0 and p_exitCode<0) then 
    job_log_psv.write_detail_log('��� ���� ������������� ���������, ������� ��������� ������ ���������� � f_mkhst_results �� ����');
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
/* ���������� ����� SQL ������� �� �������� ������������������ ������� ������� ��� ��������� �� ������ � ��������� o_sql
   ��� �������� p_mode = gpv_CheckAndAlter ��������� ������� �� �������� �������
-- �� ����:
p_cset_id     = ������������� ������ ������� �� �������� ������� � �������� �������� ���� �������
p_source_name = ��� ������� �������
p_target_name = �������������� ��� �������� ������� (�� ��������� ������ ����� �������� ������� ����� �������� �� "H")
o_sql         = ����� ������� �� �������� ��������� ������
p_mode        = ����� ������ �� ����: gpv_OnlyCheck, gpv_CheckAndAlter 
*/
function s100_CreateTargetTable(p_cset_id number, p_source_name varchar2, p_target_name varchar2 default null, o_sql out nocopy clob, p_mode number default 1) return number
is
  l_cnt     number;          -- ��������� 
  l_sql     clob;
  l_sql2tmp clob ;           -- ���������
  l_sql_add varchar2(30000):='';
  l_link    varchar2(30);
  l_owner   varchar2(30);
  l_source_tab varchar2(30);
  l_target_tab varchar2(30);
  l_save_mthd  number; -- ����� ���������� ��� �� ���������, ���� �� ������ � d_mkhst_col_sets
  l_partColumn varchar(30); -- ������� �� ������� ���������������� �������� �������
begin
	job_log_psv.start_log(null, $$plsql_unit, '������ ��������');
  job_log_psv.write_detail_log('p_cset_id='||p_cset_id||', p_source_name='||p_source_name||', p_target_name='||p_target_name, 0);
  select count(1) into l_cnt from d_mkhst_col_sets where clset_id=p_cset_id;
  -- ��������� ������� � ����������� ������� ������ ������ �������
  if l_cnt=0 then
		o_sql := '������: ����� ������������� ����� clset_id='||p_cset_id||' �� ������� d_mkhst_col_sets';
		job_log_psv.write_detail_log(o_sql, 0);
    job_log_psv.end_log ('��������� ������');
		return res_error;
	end if;
  -- ������������ ��� ����.������� �� ����������
  decodeTabName(p_source_name, l_owner, l_source_tab, l_link);
  -- ���� �������� ��� - ������� � �������
  if IfExists(p_source_name)=0 then 
		o_sql := '������: �������� ������� ��� ������������� '||p_source_name||' �� ����������';
		job_log_psv.write_detail_log(o_sql, 0);
    job_log_psv.end_log ('��������� ������');
		return res_error;
	end if; 
  
  select max(a.column_name) into l_partColumn from all_part_key_columns a where a.owner=l_owner and a.name = l_source_tab;
  if l_partColumn is null then 
		o_sql := '������: �������� ������� �� ����������������. ����� �������� ������ � ��������� ������������������� �� �������� � ���������������������� �� �����';
		job_log_psv.write_detail_log(o_sql, 0);
    job_log_psv.end_log ('��������� ������');
		return res_error;
	end if;
  
  -- ���� ������� ����, ������� � ������� � ������������� ������������ ������ ����� ������
  l_target_tab := nvl(upper(p_target_name), 'H'||substr(l_source_tab,2));
  if ifExists(l_target_tab)=1 then 
		o_sql := '������: ������� ������� '||l_target_tab||' ��� ���������� - ����������� ������� "s101_checkAbility2use" ������� ������ ��� �������� ����������� ������� ��� ��������� ������� ��������';
		job_log_psv.write_detail_log(o_sql, 0);
    job_log_psv.end_log ('��������� ������');
    return res_error;
	end if; 
  --�������������
  o_sql := 'execute immediate ''create table '||l_target_tab||'('||crlf||
    ' h_start_date date'||crlf||
    ',h_end_date   date'||crlf;
  -- ��������� SQL �� ������ ������� ���������� � ��������������� ������� �� ������ p_cset_id � ������ ������� �������� �������
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
                       dcs.clset_id = p_cset_id --���� �� ��������� ������
                   and atc.OWNER = upper(l_owner) -- �������� ������� (��������)
                   and atc.TABLE_NAME = upper(l_source_tab) -- �������� ������� (��� �������)
                  )
            order by atc.column_id )
	loop
		if i.column_name in ('H_START_DATE', 'H_END_DATE') then 
		  o_sql := '������: �������� ������� ��� ������������� �������� ������� '||i.column_name||' ���������� ��������. �� ����� �������� � ����� ��������� ��������, �.�. � ������� ������������ ��� ��������� ����';
	  	job_log_psv.write_detail_log(o_sql, 0);
      job_log_psv.end_log ('��������� ������');
		  return res_error;
	  end if;
    
    l_sql := '';
	  if i.svmtd_svmtd_id is null then 
			-- ������� ����� � ��������� �������
			select svmtd_svmtd_id into l_save_mthd 
      from D_MKHST_COL_DEFAULTS
      where i.COLUMN_NAME like upper(column_mask) and rownum<=1;
      if l_save_mthd is null then
				l_save_mthd := 1;
        job_log_psv.write_detail_log('-- ��� ������� '||i.column_name||' ��� ������ ���������� � d_mkhst_col_sets � ������ �� ������� �� ��������� � d_mkhst_columns_def. ��������� "1"=��������� ������ ���� ����������. ����� ��������� �������������� ��� ��������',0);
      else
				job_log_psv.write_detail_log('-- ��� ������� '||i.column_name||' ��� ������ ���������� � d_mkhst_col_sets. ������� �� ��������� � d_mkhst_columns_def ����� '||to_char(l_save_mthd), 0);
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
      job_log_psv.write_detail_log('��� ������� ' || i.column_name || ' ���������� �� ��������� ������� ��������', 0);
		end if;
    o_sql := o_sql||l_sql;
	end loop;
  
  --�������� sql ��������
  l_sql2tmp := replace(o_sql, l_target_tab, l_target_tab||'_TMP'); -- ������ ����� ������� ������� � ��������� _TMP 
  --����������� �������� �������
  if o_sql like 'execute immediate ''create%' then -- ���� ��� �������� ���� ��� �������� ��� ������ ��� ������, �� sql ���������� �� ����� create
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

  --����������� _TMP �������
  if o_sql like 'execute immediate ''create%' then -- ���� ��� �������� ���� ��� �������� ��� ������ ��� ������, �� sql ���������� �� ����� create
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
          '    subpartition SP_copy_'||i.filial_id||' values (-1) '|| crlf|| -- ��� ����� H_ ������� ����� ��������  
          '   ,subpartition SP_RSLT_'||i.filial_id||' values (-2) '|| crlf|| -- ��� ���������� ������� �������� � H_ �������
          '   ,subpartition SP_uncg_'||i.filial_id||' values ( 0) '|| crlf|| -- ���������� ������
          '   ,subpartition SP_new__'||i.filial_id||' values ( 1) '|| crlf|| -- �����
          '   ,subpartition SP_delt_'||i.filial_id||' values ( 2) '|| crlf|| -- ���������
          '   ,subpartition SP_chng_'||i.filial_id||' values ( 3) '|| crlf|| -- ������������
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
  job_log_psv.end_log ('��������� ������');
  return res_success;
end;


-----------------------------------------------------------------------------------------------------------------------------------
-- �������� ����������� ��������� ������ �������� ������� � ������� �� �������� ������� ������
/*
  ���������� ���� "�����"=1 ���� "������" = -1
  ������������ �������� "o_cset_id" - �������� ID ����������� ����� ��� ���������� �������
  ����:
     p_source_name    ������������  ��� �������� �������
     o_cset_id        ������������  ������������� ������ ������ �� ������ �� ��������� �������� �������
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
  -- ������������ ��� ������� ������� �� ���������� - ������� �������� � L_SOURCE_OWNER � L_SOURCE_TAB
  decodeTabName(p_source_name, l_source_owner, l_source_tab, l_link);
  -- ���� �������� ��� - ������� � �������
  if IfExists(p_source_name)=0 then 
		l_message := '������: �������� ������� ��� ������������� '||p_source_name||' �� ����������';
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
	job_log_psv.write_detail_log('��������. � ��������� '||user||'.d_mkhst_col_sets ������� �������� ����� ('||l_max_cset||') ��� ������� "'||l_source_owner||'.'||l_source_tab||'". '
     ||'����������� �������� ����, ��������� ���������� ���������� �������. ��� ������������� �� ��� ������� �� ������ ����. '||crlf
     ||'��������: subs_subs_id, filial_id, billing_filial_id ��� ������� ������������� �������� ��������� � �������', l_cnt);

  return res_success;
EXCEPTION
   WHEN OTHERS THEN
      l_message := substr(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace(),1,1800);
      job_log_psv.write_detail_log(l_message); -- ����� ����������
      job_log_psv.end_log_with_error (in_error => l_message);
      RAISE;
END;
  

-----------------------------------------------------------------------------------------------------------------------------------
-- �������� ����������� ��������� ������ �������� ������� � ������� �� �������� ������� ������
/*
  ���������� ���� "�����"=1 ���� "������" = -1
  ������������ �������� "o_res" - �������� ���� ��������������\��������� ����� ���� SQL �������� ��� ���������� ���������� ������� �������
  ����:
     p_cset_id        ������������  ������������� ������ ������ �� ������ �� ��������� �������� �������
     p_source_name    ������������  ��� �������� �������
     o_res            ������������  ���� ���������� ��� �������������� ����������
     p_target_name    �� ������     ��� ������� ������� - ����������� ��� ������ ������ ����� �� "H"
     p_mode           �� ������     �����. ���� gpv_OnlyCheckMode - ������ ��������, ���� gpv_CheckAndAlterMode - �������� � �����������
     p_filial_id      �� ������     �������� ������� ����������� � ��������� ������� ��� �� ���� �������     
*/
function s101_CheckAbility2use(
	p_cset_id       number, 
  p_source_name   varchar2, 
  o_res           out nocopy clob, 
  p_target_name   varchar2 default null, 
	p_mode number   default 0,
  p_filial_id     number default 0) return number
is
  l_cnt           number :=0;      -- ��������� 
  l_mdfCnt        number :=0;      -- ������� ��������� ����������
  l_sql           clob ;           -- ��� SQL ��� ���������
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
  
  -- �������� �� ���������� ����� ������
  if p_mode not in (gpc_OnlyCheck, gpc_CheckAndAlter) then 
		o_res := '������: ����� ������������ ����� ������. ����������� ���� �� gpv_OnlyCheck, gpv_CheckAndAlter';
		job_log_psv.write_detail_log(o_res, 0);
		return res_error;
  end if;
  
  -- ������������ ��� ������� ������� �� ���������� - ������� �������� � L_SOURCE_OWNER � L_SOURCE_TAB
  decodeTabName(p_source_name, l_source_owner, l_source_tab, l_link);
  -- ���� �������� ��� - ������� � �������
  if IfExists(p_source_name)=0 then 
		o_res := '������: �������� ������� ��� ������������� '||p_source_name||' �� ����������';
		job_log_psv.write_detail_log(o_res, 0);
		return res_error;
	end if; 
  
  -- ���� ������� ���, ������� � ������� � ������������� ������������ ������ ����� ������
  l_target_owner := user;
  l_target_tab := nvl(upper(p_target_name), 'H'||substr(l_source_tab,2));
  if ifExists(l_target_tab)=0 then 
		o_res := '������: ������� ������� '||l_target_tab||' �� ���������� - ����������� �������������� ������� "s100_CreateTargetTable" ������� ������ ��� ��������';
		job_log_psv.write_detail_log(o_res, 0);
    return res_error;
  else
		 -- ������������ ��� ������� ������� �� ���������� - ������� �������� "l_target_owner" � "l_target_tab" - ������ ��� �������
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
      job_log_psv.write_detail_log('� �������� P'||i.filial_id||' ������� '||l_target_owner||'.'||l_target_tab||'_TMP ���������� ������ '||l_cnt||' �����������. ����� ����������� ��� �������� ������');
      dbms_output.put_line('� �������� P'||i.filial_id||' ������� '||l_target_owner||'.'||l_target_tab||'_TMP ���������� ������ '||l_cnt||' �����������. ����� ����������� ��� �������� ������');
      l_mdfCnt := l_mdfCnt +1;
      o_res := o_res || 'execute immediate ''alter table '||l_target_tab||'_TMP drop partition P'||i.filial_id||''';'||crlf;
      o_res := o_res || 'execute immediate ''alter table '||l_target_tab||'_TMP add partition P'||i.filial_id||' values ('||i.filial_id||')  pctfree 0
            (
              subpartition SP_copy_'||i.filial_id||' values (-1)  /* ��� ����� H_ ������� ����� �������� */ 
             ,subpartition SP_RSLT_'||i.filial_id||' values (-2)  /* ��� ���������� ������� �������� � H_ ������� */
             ,subpartition SP_uncg_'||i.filial_id||' values ( 0)  /* ���������� ������ */
             ,subpartition SP_new__'||i.filial_id||' values ( 1)  /* ����� */
             ,subpartition SP_delt_'||i.filial_id||' values ( 2)  /* ��������� */
             ,subpartition SP_chng_'||i.filial_id||' values ( 3)  /* ������������ */
            )'';' ||crlf;
    end if;
  end loop;  
  
         
  -- ��� �������� �������� - ����� ��������
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
		-- ���� ��� � ������ - ������� � ��������� �������, �.�. ������ ������� ����� ������� - ��� ������������ ��� ���������� ����
/*    if i.svmtd_svmtd_id is null or i.svmtd_svmtd_id=-1 then 
			o_res := '������: ���� "'||i.src_column_name||'" ��� � ������ clset_id='||p_cset_id||' /������ ������� ����� ������� - ��� ������������ ��� ���������� ����';
  		job_log_psv.write_detail_log(o_res, 0);
			return res_error;
		end if;*/
		-- ���� ���� � ������ �� �������� ��� �� ����������� - ���� ������
 	  if i.svmtd_svmtd_id=0 then continue; end if;
    -- ���� ���� � ������ � ���������� ���!
    if i.src_data_type<>i.dest_data_type then 
			execute immediate 'select count(1) from '||l_target_owner||'.'||l_target_tab||' where '||i.dest_column_name||' is not null and rownum<=1' 
                    into l_cnt;
      if l_cnt>0 then 
			  o_res := '������: ���� "'||i.src_column_name||'" �������� ��� � '||i.src_data_type||' �� '||i.dest_data_type||'. �������� ������� ��� � ������� ������� '||l_target_tab;
  		  job_log_psv.write_detail_log(o_res, 0);
			  return res_error;
      else
				l_mdfCnt := l_mdfCnt + 1;
				job_log_psv.write_detail_log('���������� ��������� ���� � '||i.src_data_type||' �� '||i.dest_data_type||' � ������� '||i.src_column_name||'. �������, ��� � ������� ������� ��� ���� �� ����� ��������. ����� ����� ������� ALTER �� ������ ����.');
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
    
    -- �������� ����������� � ����� ��� �����
    if (i.src_data_type in ('CHAR', 'VARCHAR', 'VARCHAR2', 'NCHAR') and i.src_DATA_LENGTH>i.dest_DATA_LENGTH) or
			 ((i.src_data_type in ('NUMBER') and 
		    (i.src_DATA_PRECISION>i.dest_DATA_PRECISION or i.src_data_scale > i.dest_data_scale)) )
		then 
			l_mdfCnt := l_mdfCnt + 1;
      l_sql := l_sql || '  ' || case when length(l_sql)>0 then ',' else null end 
                     || i.src_column_name ||' ' || i.src_data_type 
                     || l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
                     || crlf;
      job_log_psv.write_detail_log('�������� ����������� � ' ||l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
                                || ' �� ' || l_getSize(i.dest_data_type, i.dest_data_length, i.dest_data_precision, i.dest_data_scale) 
                                || ' � '||i.src_data_type||' ���� '|| i.src_column_name, 0);
		end if;
    
    -- �������� ����� ����. ��� ����������� � ������������ �������
    if i.dest_column_name is null then 
			l_mdfCnt := l_mdfCnt + 1;
			o_res := o_res || ' execute immediate ''alter table '||l_target_tab||' add ('||i.src_column_name ||' ' || i.src_data_type 
                     || l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
                     || ')'';' || crlf;
		  -- ������ ��� ����������� � ������ d_mkhst_col_sets
      if i.svmtd_svmtd_id is null then 
        job_log_psv.write_detail_log('� ����� '||p_cset_id||' ��������� ���� '||i.src_column_name||' � ������� -1. ��������� ������ ��������� ������ ����������, ����� ���������� �������� �������������� ��������� ���������� �������');
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
               -1,/* ����� -1 ����� �������� exception � ��������� ������� ���������� ������ ����������*/
               '/*'||l_source_tab||','||to_char(i.column_id,'0000')||'*/'||i.COMMENTS, 
               to_date('01.01.2000','dd.mm.yyyy'), 
               to_date('31.12.2999','dd.mm.yyyy'));
        commit;
      end if;
		end if;
    -- ���� ����������� � ��������� �������
    if i.temp_column_name is null then 
			l_mdfCnt := l_mdfCnt + 1;
			o_res := o_res || ' execute immediate ''alter table '||l_target_tab||'_TMP add ('||i.src_column_name ||' ' || i.src_data_type 
                     || l_getSize(i.src_data_type, i.src_data_length, i.src_data_precision, i.src_data_scale)
                     || ')'';' || crlf;
		end if;
  end loop;
  
  -- ��������� ����� ��������, ���� �������� ����������� � ���������� �����
  if length(l_sql)>0 then 
    o_res := o_res || ' execute immediate ''alter table '||l_target_tab||' modify (' ||crlf
                   || l_sql ||' )'';'
                   ||crlf;
    o_res := o_res || ' execute immediate ''alter table '||l_target_tab||'_TMP modify (' ||crlf
                   || l_sql ||' )'';'
                   ||crlf;
  end if;
  
  -- ��������� ������� � _TMP ������� ��������� �������
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
    o_res := 's101_CheckAbility2use - ��������� ������ '||l_source_owner||'.'||l_source_tab||' � '||l_target_owner||'.'||l_target_tab||' ���������';
		job_log_psv.write_detail_log(o_res);
  else    
		job_log_psv.write_detail_log('s101_CheckAbility2use - ��������� ������ '||l_source_owner||'.'||l_source_tab||' � '||l_target_owner||'.'||l_target_tab||' �����������');
    -- ��������� � ��������� PL\SQL ���� ���� ��������� ��������� alter ��� ������� �������
    o_res := 'begin'||crlf
                    || o_res
                    ||'end;'||crlf;
    if p_mode = gpc_CheckAndAlter then 
	  	job_log_psv.write_detail_log('�������� SQL:'||o_res);
  	  l_runSql(o_res);
      o_res := '�������� ������:'||crlf||o_res;
    else
  		job_log_psv.write_detail_log('���������� ���������, ��������� SQL:'||o_res);
    end if;
  end if;
  
  if L_HasAddFld>0 then 
			o_res := '� ����������� ����� '||p_cset_id||' �������� '||L_HasAddFld||' ����� ����� � ������� SVMTD_SVMTD_ID=-1. ��������� ������ ���������.'||
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
   job_log_psv.start_log(in_filial_id, $$plsql_unit, '������ ��������');

   -- ���������� ����������, ����� ��������� ����� ������ ��� ������� ������� � ������ ��������
   DBMS_LOCK.allocate_unique (lockname => USER || '_' || $$plsql_unit, lockhandle => v_lockhandle);
   -- ���, ���� ���������� �� ����� �����
   WHILE DBMS_LOCK.request (lockhandle => v_lockhandle) <> 0
   LOOP
     DBMS_LOCK.sleep (60);
   END LOOP;
   job_log_psv.write_detail_log('������ � ����', 0);
   
   job_log_psv.end_log ('��������� ������');
   -- ������� ����������
   v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
EXCEPTION
   WHEN OTHERS THEN
      -- ������� ����������
      v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
      job_log_psv.end_log_with_error (in_error => SUBSTR (dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace(), 1, 1800));
      RAISE;
END;
*/

--------------------------------------------------------------------------------------
-- �������� ��������� �� ������� � ����������� ��������
procedure readAndCheckTabParam(in_scr_table in varchar2, in_snap_date varchar2
	 ,in_out_clset_id in out number
   ,out_mkhst       out number
   ,out_target_tab  out varchar2
   ,out_dayDiff     out number
   ,out_snapDate    out date

   ,out_keyFiledOne   out varchar2/*---< clmtp_clmtp_id=2 ---------  ��� �� ��������� ����. ����� �������������� � ���������� ����� ��� ���������. ������������ ����� ������� ��������� ������ �� ����*/ 
   ,out_colList       out varchar2/*---< svmtd_svmtd_id>0 ---------  ��� ������� ������ ����� ����������� � ������������ insert(...)*/
   ,out_keyFields12   out varchar2/*---< clmtp_clmtp_id =1,2       - ��� �������� ������� ������ � ������������ select  */
   ,out_keyFields3    out varchar2/*---< clmtp_clmtp_id =3         - ��� �������� ������� ������ � ������������ select  */
   ,out_whereKeyFlds12 out varchar2/*---<--------- ���������        alc.field1 = src.field1          and        alc.field2=src.field2 */
   ,out_whereKeyFlds3 out varchar2 /*---<--------- ���������  nvl(alc.field1,0)=nvl(src.field1(+),0) and nvl(alc.field2,0)=nvl(src.field2(+),0) */
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
      l_message:='� ������� "'||in_scr_table||'" ����������� ����� ���� ����� � ����������� ������� m_mkhst_tables';
      job_log_psv.write_detail_log(l_message);
 	    raise_application_error(-20000,l_message);
    elsif l_cnt>1 then 
      job_log_psv.write_detail_log('� ������� "'||in_scr_table||'" ���� '||l_cnt||' �������. ��������� ����������� - '||in_out_clset_id);
    else
      job_log_psv.write_detail_log('� ������� "'||in_scr_table||'" ��������� ����� - '||in_out_clset_id);
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
   -- ������������� ��� ������������ ������� � "�����������" ����
   decodeTabName(out_target_tab, l_link, out_target_tab, l_link);
   out_snapDate := to_date(in_snap_date, l_datePicture);

   if l_cnt is null then 
     l_message:='��������� ������� "'||in_scr_table||'" � ��������� clset_id='||in_out_clset_id||' �� ������� � ����������� ������� m_mkhst_tables';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif l_cnt=0 then
     l_message:='��� ��������� ������� "'||in_scr_table||'" � ��������� clset_id='||in_out_clset_id||' ��������� ������ (enabl<>1)';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   else
     job_log_psv.write_detail_log('�������� � '||out_target_tab||'. ���������� ���������� � ����='||out_dayDiff);
	 end if;
   
   -- �������������� ���������� ��� ������ �������
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
      out_keyFiledOne   /*---< clmtp_clmtp_id=2 ---------  ��� �� ��������� ����. ����� �������������� � ���������� ����� ��� ���������. ������������ ����� ������� ��������� ������ �� ����*/ 
     ,out_colList       /*---< svmtd_svmtd_id>0 ---------  ��� ������� ������ ����� ����������� � ������������ insert(...)*/
     ,out_keyFields12   /*---< clmtp_clmtp_id =1,2       - ��� �������� ������� ������ � ������������ select  */
     ,out_keyFields3    /*---< clmtp_clmtp_id =3         - ��� �������� ������� ������ � ������������ select  */
     ,out_whereKeyFlds12 /*---<--------- ���������        alc.field1 = src.field1          and        alc.field2=src.field2 */
     ,out_whereKeyFlds3  /*---<--------- ���������  nvl(alc.field1,0)=nvl(src.field1(+),0) and nvl(alc.field2,0)=nvl(src.field2(+),0) */
     ,l_cnt
   from d_mkhst_col_sets t
   where clset_id=in_out_clset_id
     and out_snapDate between start_date and end_date;
     
   if out_colList is null then 
     l_message:='�� ������� �� ������ ������� ��� ����������';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif out_keyFields12 is null then
     l_message:='�� ������� �� ������ ������� � �������� ���������. ��������� � �������� filial_id, billing_filial_id, [subs|clnt|nset|srcd|cntr|...]';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   end if;
   
   if l_cnt>0 then 
     l_message:='���� ������� � ������� ���������� -1 ��� NULL. ��� ��� ��������� ������ ��������� ������.';
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
   -- ������������ ��� ����.������� �� ����������
   decodeTabName(in_scr_table, l_owner, l_source_tab  /*---<---------*/, l_link);
   -- ��� ���������� ������� �����������������?
   select column_name into l_PartColumnName from ALL_PART_KEY_COLUMNS where owner=l_owner and name=l_source_tab;
   -- ��� ���������� ������� ��������������������?
   select column_name into l_SubPartColumnName from ALL_SUBPART_KEY_COLUMNS where owner=l_owner and name=l_source_tab;
   
   -- � ���������� ������� � ���� ������� �������? ������� - ��������� ��� �����������, ������� ������ ��� ������
   execute immediate 'select  max(dbms_mview.pmarker(rowid)), min(dbms_mview.pmarker(rowid)) o_id, count(1)
         from '||l_owner||'.'||l_source_tab||' s 
         where '||l_PartColumnName   ||' = :f 
           and '||l_SubPartColumnName||' = :d'
   into l_cnt, l_tmp, out_snapCnt
   using in_filial_id, in_snapDate;
   
   if l_cnt <> l_tmp then
		 l_message:='������: ������ ����� � ������ ���������. ������ �������� �� ������ ��������� �������� ������ ����� �������';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message); 
	 end if;
   
   -- ������ ��������?
   if out_snapCnt <= gpv_snapLimitCount then 
     l_message:='������: � ������ '||l_owner||'.'||l_source_tab
       ||' where '||l_PartColumnName ||' = '||in_filial_id ||' and '||l_SubPartColumnName||' = '||to_char(in_snapDate)
                                   ||' ���-�� ������� '||out_snapCnt
                                   ||'. ��� ������ ������ make_hist_from_s.gpv_snapLimitCount='||gpv_snapLimitCount;
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
	 end if;
   
   select subobject_name into out_snap_SubPart/*---<---------*/ 
     from all_objects 
    where data_object_id = l_tmp;
   job_log_psv.write_detail_log('� ����������� "'||out_snap_SubPart||'" ������� '||to_char(in_filial_id)
                                             ||' ���. ������� '||lower(l_owner||'.'||l_source_tab)
                                             ||' ������� '||num2ch(out_snapCnt),out_snapCnt);
   	
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
	-- ���� ���� �� �������� �������� ���������! ����� ��������� ������ ��������, �� ������� ������� �� ���������.
   -- ����� ��� ����� ��������� �����!
   out_sqlClob := 'select '||crlf
              ||'  count(1)'
              ||'  ,sum(case when '||replace(in_keyFields12,',',' is null or ')||' is null then 1 else 0 end) '||crlf
              ||'  ,sum(case when '||case when length(in_keyFields3)>0 then replace(in_keyFields3 ,',',' is null or ') 
                                          else '1 ' end
                                   ||' is null then 1 else 0 end) '||crlf
              ||'from '||in_owner||'.'||in_table||' subpartition ('||in_snap_SubPart||')';
   execute immediate out_sqlClob into l_cnt, l_tmp, l_tmp2;
   if l_tmp>0 then 
     l_message:='������: ���� �� �������� �������� ('||in_keyFields12||') ������� '||in_owner||'.'||in_table||' �������� ������ ��������. � �� ����� �������������� � �������� �������� ��� ��������� ���������\�����\������. ��������� � ������� ���� clmtp_clmtp_id � ������ � ������ clset_id='||in_clset_id;
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif l_tmp2=l_cnt and l_cnt >0 and length(in_keyFields3)>0 then 
     job_log_psv.write_detail_log('�����������: �������� ���� ����������� NULL ���������� � ���-�� '||num2ch(l_tmp2)||', '||num2ch(l_tmp2/l_cnt,2)||'% �� ����');
   elsif l_tmp2=0     and l_cnt >0 and length(in_keyFields3)>0 then 
     job_log_psv.write_detail_log('�����������: �������� ���� ����������� NULL �� ���������� � ������, ����� �������� �� clmtp_clmtp_id=3 �� 1');
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
     l_message:='������: ���� ������('||to_char(in_snapDate)||') ������ ��� � �������('||to_char(in_h_date)||'). �������� ������ �� ������ ��� ������ � �_ �������. ���� ��� �� ��� ����������� ��������� "AddSnapInsert"';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif trunc(in_snapDate) = trunc(in_h_date) then 
     l_message:='������: ���� ������('||to_char(in_snapDate)||') ����� ���� � �������('||to_char(in_h_date)||'). �������� ������ �� ������ ��� ������ � �_ �������.';
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000,l_message);
   elsif trunc(in_snapDate)-trunc(in_h_date) > in_dateDiff then 
		 -- ��� ������������ S_DEALER_ACCOUNTS_DAILY �� �� �� 01/01/2017 �� ���� ���������\����������\��������. ������ ���������, 
     -- �� � ������� ������������ H_START_DATE �������� 31/12/2017. ������� �������� ��� � ����������� ������� ������ ��� ��������� � �������� �����������
     select max(snapDate) 
       into l_lastDat
     from f_mkhst_results
       where filial_id = in_filial_id
         and mkhst_mkhst_id = in_mkhst_id
         and snapDate<>in_snapDate;
     -- ��������� ���������� ������ � ���.������� - ���� ��� �� - �� ������� ���� ������ �����    
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
       job_log_psv.write_detail_log('��������� '||to_char(trunc(in_snapDate)-trunc(in_h_date))||' ����. ����� ��������. �� ������� ������('||to_char(l_lastDat)||') ��� ��������� ���������');
     elsif in_dateDiff=1 and trunc(in_snapDate) = trunc(last_day(in_snapDate)) 
                         and trunc(in_h_date)   = trunc(last_day(in_h_date))
                         and months_between(in_snapDate, in_h_date)=1
     then
       job_log_psv.write_detail_log('��������� '||to_char(trunc(in_snapDate)-trunc(in_h_date))||' ����. ����� ��������. '
                    ||'�� ������('||to_char(in_snapDate)||') �������� ������ ���������� ������ �� ������� ����� � �������('||to_char(in_h_date)||')');
		 else
       l_message:='������: ���� ������('||to_char(in_snapDate)||') ������� �� ���� � �������('||to_char(in_h_date)||') �� "'
                    ||to_char(trunc(in_snapDate)-trunc(in_h_date))||'" ����'||crlf
                    ||'��� ��������� ��������� ������ maxDayDiff � m_mkhst_tables ��� ��������� ������� � ������. ���������� ������� �������� ������� ��������������� ������ AddSnap2End';
       job_log_psv.write_detail_log(l_message);
       raise_application_error(-20000,l_message);
     end if;
   end if; 
	 return res_success;
end;
  
---------------------------------------------------------------------------------------------------------
-- ����� � ������� ������ �������� ��������� �������� ������ �� �������� ����� l_keyFields12
-- �������� �� ������  FILIAL_ID, SK_SUBS_ID, IS_MGR
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
    job_log_psv.write_detail_log('���� ������ ��������� ��������. � ��� ������ �������� ������� �� �������� ������� ('||l_tmp||')����');
    l_tmp := add_partition(in_target_tab, in_filial_id);
	end if;
end;

--==================================================================================================
-- �������� ����� �����
/*
  in_filial_id    �� ��������� 0 = ���������� ��� ������� ������� � ����� PUB_DS
  in_snap_date    ����������� ����
  in_scr_table    �������-�������� �������
*/
-------------------------------------------------------------------------------------------------------
Procedure AddSnap2End(in_filial_id IN NUMBER, in_scr_table varchar2, in_snap_date IN VARCHAR2, in_clset_id number default null)
is
   v_lockhandle   NUMBER;
   v_lock_release NUMBER;
   
   l_cnt          number:=0; -- ��������� ���������� 
   l_tmp          number:=0; -- ��������� ���������� 
   l_n0           number:=0; -- ��������� ���������� (����������)
   l_n1           number:=0; -- ��������� ���������� (�����)
   l_n2           number:=0; -- ��������� ���������� (���������)
   l_n3           number:=0; -- ��������� ���������� (������������)
   l_n3unq        number:=0; -- ��������� ���������� (������������-���������� \ ���� �� ����������� ������ ���������� ����������� �� �������� �����. �������� ����� ������(�������� ����) ����������� �� bill_bill_id(����������� ��������)
   l_mkhst        number;    -- PK �� ����������� ������� � ����� ����������
   l_clset_id     number;
   l_target_tab   varchar2(50);       -- ��� ������� ������ (����� �� ��������)
  l_owner       varchar2(50); -- �������� p_source_tab
  l_source_tab  varchar2(50); -- ������� � ������� ���� ��������� �������� � ��������� ������� p_filial_id
  l_link        varchar2(50); -- ��������� ���� �� p_source_tab / ��������� ����������
  l_snap_SubPart varchar2(50);
  l_snapDate   date; -- ���� � ��������� ����������� �������� ������� ����� �������
  l_snapCnt    number; -- ����� ������� � �������� �����������
  l_message     varchar2(6000);
  l_sqlClob     clob;
  l_msgCase     varchar2(16000); -- ��� ���������� �����, ��������� �������� ������� ������ ���� �������� � �������
  l_msgCaseFld  varchar2(26000); -- ��� ���������� �������� �����, ��������� �������� ������� ���������. ������ ������������ ���������� ����� � ��������� �������
  l_H_lastSubPart varchar2(50); -- ��� ��������� ����������� � ������� "H_" ������� ��� ������� �������
  l_H_lastDate    date;         -- ����, ������� ������������� � "H_" �������
  l_H_cnt         number;       -- ���-�� ������� � ������� �������� "H_" ������� (���� ������� ����� ��������)
  l_dayDiff       number;       -- ����������� ������� - ��������� ���� ����� �������� ������� � ����������� �������

  l_keyFiledOne    varchar2(300); -- ��� �� ��������� ����. ����� �������������� � ���������� ����� ��� ���������. ������������ ����� ������� ��������� ������ �� ���� 
  l_colList        varchar2(6000);-- ��� ������� ������ ����� ����������� � ������������ insert(...)
  l_keyFields12    varchar2(300); -- ������� ���������� ��� �������� � ������ clset_id clmtp_id in (1,2) -- �� ��������� null
  l_keyFields3     varchar2(300); -- ������� ���������� ��� �������� � ������ clset_id clmtp_id in (3)   -- ��������� null
  l_whereKeyFlds12 varchar2(600); -- ������� ���������� ��� �������� � ������ clset_id ����� clmtp_id in (1,2) -- �� ��������� null
  l_whereKeyFlds3  varchar2(600); -- ������� ���������� ��� �������� � ������ clset_id ����� clmtp_id in (3)   -- ��������� null
begin
   job_log_psv.start_log(in_filial_id, $$plsql_unit, '������. ������='||in_filial_id||', ������� '||in_scr_table||', ����� '||in_clset_id||', ���� '||in_snap_date);
   job_log_psv.write_detail_log(' ');
   -- ���������� ����������, ����� ��������� ����� ������ ��� ������� ������� � ������ ��������
---   DBMS_LOCK.allocate_unique (lockname => USER || '_' || $$plsql_unit, lockhandle => v_lockhandle);
   -- ���, ���� ���������� �� ����� �����
/*   WHILE DBMS_LOCK.request (lockhandle => v_lockhandle) <> 0
   LOOP
     DBMS_LOCK.sleep (60);
   END LOOP;*/
   
   l_runSql('alter session force parallel query parallel '||to_char(gpv_ParallelCount));
   l_runSql('alter session force parallel DDL   parallel '||to_char(gpv_ParallelCount));
   -- �������� �������: ����� �������� �� ������� ��������? � � ����� �������� ������? --------------------------------------------------
   l_clset_id := in_clset_id;
   readAndCheckTabParam( in_scr_table, in_snap_date, l_clset_id
      -- �� m_mkhst_tables   -- �������������� ����� ����������
      ,l_mkhst, l_target_tab, l_dayDiff, l_snapDate 
      -- �� d_mkhst_col_sets -- �������������� ���������� ��� ������ �������
      ,l_keyFiledOne      ,l_colList 
      ,l_keyFields12      ,l_keyFields3 
      ,l_whereKeyFlds12   ,l_whereKeyFlds3 
   );  
   
   -- ������������ ��� ����.������� �� ����������
   decodeTabName(in_scr_table, l_owner, l_source_tab  /*---<---------*/, l_link);
   
   -- �������� ���-�� ������� �� ����� � �������������� ����� ��������   
   checkSnapTable(to_number(in_filial_id), in_scr_table, l_snapDate,
     l_snap_SubPart, l_snapCnt
   );

   -- ������� �������: ��� ���������� ��������� ����������� � ������� �������? -------------------------------------------------------
   if l_getLastPartition(l_target_tab, in_filial_id, l_H_lastSubPart/*---<---------*/ )=res_error then
     l_message:='������ ��������� ����� ��������� ����������� ������� �������:'||l_H_lastSubPart;
     job_log_psv.write_detail_log(l_message);
 	   raise_application_error(-20000, l_message);
	 end if;
   -- ����� ��������� ���� ������� � ������?
   execute immediate 'select max(h_START_DATE), count(1) 
                      from '||l_target_tab||' subpartition ('||l_H_lastSubPart||')' 
                 into l_H_lastDate /*---<---------*/
                      ,l_H_cnt;
   job_log_psv.write_detail_log('� ��������� ����������� "'||l_H_lastSubPart||'" ������� "'||in_filial_id
                               ||'" ������� ������� "'||lower(l_target_tab)||'" ���� "'||to_char(l_H_lastDate,'dd.mm.yyyy hh24:mi:ss')
                               ||'" ������� '||lower(l_H_cnt), l_H_cnt);
   
   -- �������� ������������ �������� �������� � �������� ���������.----------------------------------------------------------------------------
   -- ������� ����������� �������� ����� � ���������
   -- ������� - ����������� ���������� � ������ � �������� ����������+����
   if s101_CheckAbility2use(l_clset_id, l_owner||'.'||l_source_tab, l_sqlClob, l_target_tab, gpc_CheckAndAlter,in_filial_id) = res_error then 
     l_message:= substr(l_sqlClob,1,3999);
 	   raise_application_error(-20000,l_message);
	 end if;
   
   -- ��������� ����� �� ��������� ������ ������ � ������� ? 0-�������� ����, Exception - �� ������������ � ����� ��� ������� � �������
   -- ���� ��� ��������� ������������ res_success (1)
   if checkDayDistance(l_snapDate, l_H_lastDate, l_dayDiff, in_filial_id, l_mkhst)=0 then 
     job_log_psv.write_detail_log('������� ������� ������ - ������ ���������� ������ � H_ ������� �� ��������');
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
     l_message := '��������� ������ - ������� ��������� '||num2ch(l_cnt)||' �����';
     job_log_psv.write_detail_log (l_message, l_cnt);
     job_log_psv.end_log (l_message);
     l_logResult(l_mkhst , in_filial_id, l_snapDate, l_clset_id, l_target_tab, '2End', 1, l_cnt, 0, l_cnt, 0, 0, 0, l_message);
     -- ������� ����������
		 job_log_psv.write_detail_log('���������� ���������� �� ����� ������� "'||l_target_tab||'" � �������� "'||l_H_lastSubPart||'"');
     DBMS_STATS.gather_table_stats (USER, l_target_tab, partname =>l_H_lastSubPart, estimate_percent => 1, degree => gpv_parallelCount);
     -- ������� ���������� � �������
     v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
     return;
   end if; 
   
   -- ���� ���� �� �������� �������� ���������! ����� ��������� ������ ��������, �� ������� ������� �� ���������.
   -- ����� ��� ����� ��������� �����!
   checkColumns_clmtp(l_clset_id, l_keyFields12, l_keyFields3,  l_owner, l_source_tab, l_snap_SubPart,    l_sqlClob);
   -- ���� �������� ������� ��������� ������ ��������� ������ � �������� ������, �� � ������� �� ��� ������ - ������� ������� �� ���������.
   -- ����� ��� ����� ��������� �����!
   checkColumns_clmtp(l_clset_id, l_keyFields12, l_keyFields3,  user,    l_target_tab, l_H_lastSubPart,    l_sqlClob);
   
  
   job_log_psv.write_detail_log('���������� � ������� ���������. �������� ����: '||l_keyFields12||' ������� ����(�������������).'
                            ||case when length(l_keyFields3)>0 then ' �������� ���� �������� ����(������������ null)'||l_keyFields3 else '' end);
   <<p3>> -- ���������� ������� ������� ��������� ������� � ������� �������� � �������
   -- ��������� - ��� ������ ������ ���� �� 4� ��������: 0-����������\1-�����\2-�������\3-���������� 
   l_colList := ''; -- ������� ������ �������, � ��� �������, ������� ������ ���������� ���� loop
   --������� ����������� ������
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
   -- ������ ��������� ����������� ������ ������
   for i in (select * from d_mkhst_col_sets t
              where clset_id=l_clset_id
                 and svmtd_svmtd_id > 0
                 and clmtp_clmtp_id is null
                 and l_snapDate between start_date and end_date-1/86400
              order by column_comm)
   loop
     -- ��� ���� ������� ������ �����, ������� ���� �������������. ����� �������� ��������, �.�. ����� ����� ����� �� S_ ������
		 l_sqlClob := l_sqlClob ||  --1  ���������, ���� ���������� ������ /����� ��������� �������������� ��� ��������      
           case when i.svmtd_svmtd_id=1 then ' /*1*/,nvl(src.'||i.column_name||', hst.'||i.column_name||')'||crlf
						                    --2  ���������, ���� ���������� ������ ����� ���� ������ = ��������� ���� ������ /�������� ������ �� ����� ������
						    when i.svmtd_svmtd_id=2 then ' /*2*/,nvl(src.'||i.column_name||', hst.'||i.column_name||')'||crlf
								                --3  ��������� ������� �� ��������� ���������� �������� / �������� ����� ����� � ����
                when i.svmtd_svmtd_id=3 then ' /*3*/,nvl(src.'||i.column_name||', hst.'||i.column_name||')'||crlf
								                --4  ��������� ������ ������ �� ������ ��������, ���������� ��������� ������������  /�������� INIT_ ���������
                when i.svmtd_svmtd_id=4 then ' /*4*/,nvl(src.'||i.column_name||', hst.'||i.column_name||')'||crlf
								                --5  ���������. �������� � ��� ������������ �� start_date ��� �������� ����� �������.  /��������, sum(DURATION) �� ���� ��� ���������� � �������
                when i.svmtd_svmtd_id=5 then ' /*5*/,nvl(src.'||i.column_name||',0)+nvl(hst.'||i.column_name||',0)'||crlf
						else ' !!! ����������� �����-'||i.svmtd_svmtd_id||' !!! ' end;
     -- ��� ������ ����� � ����� insert into table(...l_col_list...)
     l_colList := l_colList ||','|| i.column_name;
     -- ������������ 1/0 ���� ����� ���������� ������� �������� ����� ������������� ������
     l_msgCase := l_msgCase || 
           case when i.svmtd_svmtd_id=1 then crlf||'+decode(src.'||i.column_name||', hst.'||i.column_name||', 0, 1)'
                when i.svmtd_svmtd_id=2 and trunc(l_snapDate,'month') between l_H_lastDate and l_snapDate --trunc(last_day(l_snapDate)) 
									                      then crlf||'+decode(src.'||i.column_name||', hst.'||i.column_name||', 0, 1)'
                when i.svmtd_svmtd_id=2 then crlf||'+case when hst.h_start_date<'||dat2str(trunc(l_snapDate,'month'))||crlf
   					                                     ||'      then decode(src.'||i.column_name||', hst.'||i.column_name||', 0, 1) else 0 end'
                when i.svmtd_svmtd_id=4 then crlf||'+case when src.'||i.column_name||' is null and hst.'||i.column_name||' is not null then 1 else 0 end'
									                         -- '+nvl2(src.'||i.column_name||',0,nvl2(hst.'||i.column_name||',1,0))'
   						  else null end;
     -- ������������ �������� ����� � MOSTLY3CHANGE_COLUMNS
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
   -- ������� ����������� ������
   l_sqlClob := l_sqlClob 
     ||' ,case when hst.'||l_keyFiledOne||' is null then 1 /*new record*/'||crlf
		 ||'      when src.'||l_keyFiledOne||' is null then 2 /*deleted record*/'||crlf
 		 ||'	    when '||'(0 '||l_msgCase||') '||crlf
  	 ||'	             >0 then 3 /*changed in monitor field*/'||crlf
 		 ||'	else 0 end sub1part2type3column4 /*���� ����������������� � �������� �������, ���� ����� ������� ���������*/'||crlf
     ||' ,case when hst.'||l_keyFiledOne||' is null or  src.'||l_keyFiledOne||' is null then null'||crlf
     ||'       else substr('||substr(l_msgCaseFld, 5, length(l_msgCaseFld)-4) ||',1,3999) end MOSTLY3CHANGE_COLUMNS '||crlf
     ||'from src, hst, alc'||crlf
     ||'where '||l_whereKeyFlds12
               ||case when length(l_whereKeyFlds3)>0 then ' and '||l_whereKeyFlds3 else '' end||crlf
     ||'  and '||replace(l_whereKeyFlds12,'src.','hst.')
               ||case when length(l_whereKeyFlds3)>0 then ' and '||replace(l_whereKeyFlds3,'src.','hst.') else '' end||crlf
     ||crlf;
   -- ����� ��������� ������������ ������� �.�. ������ ������ �������� ������� �������� ��� �������
   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date,'||l_keyFields12
                              ||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
                              ||l_colList||',sub1part2type3column4, MOSTLY3CHANGE_COLUMNS)'||crlf||l_sqlClob;   
   
  
   <<p4>> --================ ��������� ���������
   job_log_psv.write_detail_log('������� ��������� ������ ������ ������� ������� � ������(���������� � �����)');
   l_runSql('alter table '||l_target_tab||'_tmp truncate partition(P'||in_filial_id||')');
   --dbms_output.put_line(l_msgClob);
   l_runSql(l_sqlClob);
   commit;
   -- ��������� ��� ����������
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

   job_log_psv.write_detail_log('����������: ����������('||num2ch(l_n0)
     ||') �����('||num2ch(l_n1)
     ||') ���������('||num2ch(l_n2)
     ||case when l_n3 = l_n3unq then ') ������������('||num2ch(l_n3)||')'
                                else ') ������������('||num2ch(l_n3)||', � ��� ����� ���������� '||num2ch(l_n3unq)||')'
       end																	
     , l_n0+l_n1+l_n2+l_n3);
   
      
   <<p5>> --������ ������� �������
   -- �����. ����������� ������ ����� ������ ������ ��� ������� ������ ������������
   job_log_psv.write_detail_log('������ ������� ������� (1� ������) ����������, �����, �����������');

   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date, '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||',sub1part2type3column4)'||crlf;
   l_sqlClob := l_sqlClob || -- ���������� � ����� �������� ��� ���� �� _TMP �������
      'select --���������� � ����� �������� ��� ���� �� _TMP �������'||crlf
    ||' h_start_date, h_end_date, '
    ||  l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||', -2 sub1part2type3column4 '||crlf
    ||'from '||l_target_tab||'_tmp partition (p'||in_filial_id||') where sub1part2type3column4 in (0, 1)'||crlf
    ||'union all'||crlf
    -- ����������� �������� �� _TMP � ������� END_DATE = SNAP_DATE-1/86400
    ||'select -- ����������� �������� �� _TMP � ������� END_DATE = SNAP_DATE-1/86400'||crlf
    ||' h_start_date, '||dat2str(l_snapDate-1/86400)||' h_end_date,'||crlf
    ||  l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||', -2 sub1part2type3column4 '||crlf
    ||'from '||l_target_tab||'_tmp subpartition (sp_delt_'||in_filial_id||')';
    l_runSql(l_sqlClob);
    l_tmp := SQL%rowcount;
    commit;
    
    -- ������������ ������� �������� �� l_H_lastSubPart � ������� End_date = SNAP_DATE-1/86400
   job_log_psv.write_detail_log('������ ������� ������� (2� ������)-������������ ���������');
   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date, '||crlf
    ||     l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||',sub1part2type3column4)'||crlf
    ||'select -- ������������ ������� �������� �� l_H_lastSubPart � ������� End_date = SNAP_DATE-1/86400'||crlf
    ||'  alc.h_start_date, '||dat2str(l_snapDate-1/86400)
    ||', alc.'||replace(l_keyFields12,',',',alc.')||case when length(l_keyFields3)>0 then ', alc.'||replace(l_keyFields3,',',',alc.') else '' end ||crlf
    ||'      '||replace(l_colList,',',',alc.')||', -2 sub1part2type3column4'||crlf
    ||' from '||l_target_tab||' subpartition ('||l_H_lastSubPart||') alc,'||crlf
    ||'      (select distinct '||l_keyFields12 ||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end
     --�.� ��� ��������� �������� � ����� ������� �������������� �������� ���� � �������-���� ������, � � ����� ������ - ��� � �����
    ||'       from '||l_target_tab||'_tmp subpartition (sp_chng_'||in_filial_id||')) src'||crlf
    ||' where alc.h_end_date>sysdate and '||crlf
               ||replace(l_whereKeyFlds12,'(+)','')
               ||case when length(l_whereKeyFlds3)>0 then ' and '||l_whereKeyFlds3 else '' end;
    l_runSql(l_sqlClob);
    l_cnt := SQL%rowcount; 
    l_tmp := l_tmp + l_cnt;
    commit; 
    if l_cnt not in (l_n3,l_n3unq) then
			l_message:='������: ������� � ��������� "������������ �����������" ������� ������ � ���������� '||num2ch(l_cnt)
                 ||'. ��� ���� ���������, ��� ��������� '||num2ch(l_n3)
                 ||case when l_n3<>l_n3unq then ' ��� '||num2ch(l_n3unq) else '' end||crlf
                 ||'��������� ��� ����������� ��������� clmtp_clmtp_id � ������. �������� ���� �� ��� �� �������� ������. �������� ������ ������������� ����������� ������ ������ ������ clmtp_clmtp_id'||crlf
                 ||'����������� ��� ������� ����� ������ - � ����.'||crlf;
      job_log_psv.write_detail_log(l_message);
      job_log_psv.write_detail_log('���������� ��������� ������ ��� ��������� ������� ��������� � ����������');
      job_log_psv.write_detail_log( 'select '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||', count(1)'||crlf
                              ||'from '||l_target_tab||'_tmp '||crlf
                              ||'where filial_id='||in_filial_id||' and sub1part2type3column4 not in (-1, -2) /*-1 �����, -2 ���������*/'||crlf
                              ||'group by '||crlf
                              ||'  '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
                              ||'having count(1)>1');
 	    raise_application_error(-20000,l_message);
		end if;

    -- ������������ ����� �������� �� ������ ������ � ���������� start_date = SNAP_DATE
   job_log_psv.write_detail_log('������ ������� ������� (3� ������)-������������ ���������');
   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date, '||crlf
    ||     l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
    ||' '||l_colList||',sub1part2type3column4)'||crlf
    ||'select -- ������������ ����� �������� �� ������ ������ � ���������� start_date = SNAP_DATE'||crlf
    ||' '||dat2str(l_snapDate)||' h_start_date,'||dat2str(to_date('31.12.2999','dd.mm.yyyy'))
    ||', alc.'||replace(l_keyFields12,',',',alc.')||case when length(l_keyFields3)>0 then ', alc.'||replace(l_keyFields3,',',',alc.') else '' end ||crlf
    ||'      '||replace(l_colList,',',',alc.')||', -2 sub1part2type3column4'||crlf
    ||' from '||l_owner||'.'||l_source_tab||' subpartition ('||l_snap_SubPart||') alc,'||crlf
    ||'      (select distinct '||l_keyFields12 ||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end
     --�.� ��� ��������� �������� � ����� ������� �������������� �������� ���� � �������-���� ������, � � ����� ������ - ��� � �����
    ||'       from '||l_target_tab||'_tmp subpartition (sp_chng_'||in_filial_id||') where h_end_date>sysdate) src'||crlf
    ||' where '||crlf
               ||replace(l_whereKeyFlds12,'(+)','')
               ||case when length(l_whereKeyFlds3)>0 then ' and '||l_whereKeyFlds3 else '' end;
    l_runSql(l_sqlClob);
    l_cnt := SQL%rowcount; 
    l_tmp := l_tmp + l_cnt;
    commit; 
    if l_cnt not in (l_n3,l_n3unq) then
			l_message:='������: ������� � ��������� "������������ �����������" ������� ������ � ���������� '||num2ch(l_cnt)
                 ||'. ��� ���� ���������, ��� ��������� '||num2ch(l_n3)
                 ||case when l_n3<>l_n3unq then ' ��� '||num2ch(l_n3unq) else '' end||crlf
                 ||'��������� ��� ����������� ��������� clmtp_clmtp_id � ������. �������� ���� �� ��� �� �������� ������. �������� ������ ������������� ����������� ������ ������ ������ clmtp_clmtp_id'||crlf
                 ||'����������� ��� ������� ����� ������ - � ����.'||crlf;
      job_log_psv.write_detail_log(l_message);
      job_log_psv.write_detail_log('���������� ��������� ������ ��� ��������� ������� ��������� � ����������');
      job_log_psv.write_detail_log( 'select '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
                              ||'from '||l_target_tab||'_tmp '||crlf
                              ||'where filial_id='||in_filial_id||' and sub1part2type3column4<>-1'||crlf
                              ||'group by '||crlf
                              ||'  '||l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
                              ||'having count(1)>1');
 	    raise_application_error(-20000,l_message);
		end if;

   l_sqlClob := '';
   job_log_psv.write_detail_log('�������� ��������� ������� '||num2ch(l_tmp)||' �����', l_tmp);
   commit;

   -- �� ���? ����������?
   job_log_psv.write_detail_log('(����������) ?= (����������)+(�����)+(���������)+(����������.��������� + ����������.���������)');
   job_log_psv.write_detail_log('('||num2ch(l_tmp)||') ?= ('||num2ch(l_n0)||')+('||num2ch(l_n1)||')+('||num2ch(l_n2)||')+('||num2ch(l_n3)||'+'||num2ch(l_n3unq)||') ?= '||num2ch(l_n0+l_n1+l_n2+l_n3+l_n3unq));
   if l_tmp <> (l_n0+l_n1+l_n2+l_n3+l_n3unq) then
		 l_message := '������. ���������� ��������� �� ������������� ���������� �� ����� �����. ������� ('||num2ch( l_tmp-(l_n0+l_n1+l_n2+l_n3+l_n3unq))
      ||'). ����������� � ����. ��������� ��� ����������� ��������� clmtp_clmtp_id � ������. �������� ���� �� ��� �� �������� ������. �������� ������ ������������� ����������� ������ ������ ������ clmtp_clmtp_id';
     job_log_psv.write_detail_log(l_message, l_n0+l_n1+l_n2+l_n3+l_n3unq );
     raise_application_error(-20000, l_message);
   end if;
   
   -- ��������� �������
   job_log_psv.write_detail_log('����������. ���� '||num2ch(l_h_cnt)||' ����� '||num2ch(l_n0+l_n1+l_n2+l_n3+l_n3unq)||'. '
                                          ||num2ch(((l_n0+l_n1+l_n2+l_n3+l_n3unq)-l_h_cnt)*100/l_h_cnt, 2)||'% ����� ���������. '
                                          ||num2ch((l_n1+l_n3)/l_h_cnt, 3)||'% ����� �����'); 
   <<p2>> -- ��������� ������ ������� ������ ��� ������ 
   l_sqlClob := 'insert into '||l_target_tab||'_tmp(h_start_date, h_end_date, '||crlf
              ||     l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
              ||' '||l_colList||',sub1part2type3column4)'||crlf
              ||'select h_start_date, h_end_date, '||crlf
              ||     l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||crlf
              ||' '||l_colList||',-1 sub1part2type3column4 /*0=����� ������ ��� ������*/'||crlf
              ||'from '||l_target_tab||' subpartition ('||l_H_lastSubPart||') '||crlf
              ||'where h_end_date>sysdate';
   l_runSql(l_sqlClob);
   commit;
   -- �������� �������� ��������� ����� ����������� 
   lpv_chk_mode := lpc_ByEnd;
   if checkData(lpc_collectData, l_target_tab, l_H_lastSubPart, l_clset_id,l_H_lastDate)<> res_success then 
     null; --job_log_psv.write_detail_log('���-�� ����� �� ���!');
   end if;
   -- ������� ������� ��������
   l_sqlClob := 'alter table '||l_target_tab||' truncate subpartition ('||l_H_lastSubPart||')';
   --job_log_psv.write_detail_log(l_sqlClob);
   l_runSql(l_sqlClob);
   -- ������� ������
   l_sqlClob := 'insert into '||l_target_tab||'(h_start_date, h_end_date,'|| l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end
     ||l_colList||')'||crlf
     ||'select h_start_date, h_end_date,'|| l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end
     ||l_colList||crlf
     ||' from '||l_target_tab||'_tmp subpartition (sp_rslt_'||in_filial_id||')';
   --job_log_psv.write_detail_log(l_sqlClob);
   l_runSql(l_sqlClob);
   commit;

   -- ���� ������ split partition ? - ����� � ������� �������� �������� ��������� ����� �� �������� �����
   checkResAndSplitPartition(in_filial_id, l_target_tab, l_H_lastSubPart, l_keyFields12, l_keyFields3 );

   -- ��������� ��� ���������� ����� �������. ����������� �������� ����� ������ ��� �������� ����. ������ ��� ��� �� ���-�
   if checkData(lpc_checkData, l_target_tab, l_H_lastSubPart, l_clset_id,l_H_lastDate)<> res_success then 
     l_message := '���-�� ����� �� ���! �������� ���������� ������ �� ����� ������ � ���� ��������!';
     job_log_psv.write_detail_log(l_message||' �� ������� ������ �� ������������ ������� ����� ���������� � ���� ������� ������� ��� ���� �� ������ ����� ������. �������� ��������� ����� ���:'||crlf
        ||'alter table '||l_target_tab||' subpartition '||l_H_lastSubPart||';'||crlf
        ||'insert into '||l_target_tab||'(h_start_date, h_end_date,'|| l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||l_colList||')'||crlf
        ||                        'select h_start_date, h_end_date,'|| l_keyFields12||case when length(l_keyFields3)>0 then ','||l_keyFields3 else '' end||l_colList||crlf
        ||'from '||l_target_tab||'_tmp subpartition (sp_copy_'||in_filial_id||');');
     update m_mkhst_tables t 
        set t.enabl = 0
      where t.mkhst_id=l_mkhst;
      commit;
     raise_application_error(-20000, l_message||' ����������� �������� � ����');
   end if;

   -- ������� ����������
   if to_char(l_snapDate,'dd')in('01','11','22') or trunc(l_snapDate)=trunc(last_day(l_snapDate)) then 
		 job_log_psv.write_detail_log('���������� ���������� �� ����� ������� "'||l_target_tab||'" � �������� "'||l_H_lastSubPart||'"');
     DBMS_STATS.gather_table_stats (USER, l_target_tab, partname =>l_H_lastSubPart, estimate_percent => 1, degree => gpv_parallelCount);
   end if;
   
   -- �����. ��� �������� �� ����� - �������!
   l_message := '���������� '||num2ch(l_n0+l_n1+l_n2+l_n3+l_n3unq)||' ����� ('||num2ch(((l_n0+l_n1+l_n2+l_n3+l_n3unq)-l_h_cnt)*100/l_h_cnt, 2)
                ||'%). �����('||num2ch(l_n0)||'), �����('||num2ch(l_n1)||'), ����('||num2ch(l_n2)||'), ���.����('||num2ch(l_n3)||'), ���.����('||num2ch(l_n3unq)||')';
   
   job_log_psv.write_detail_log(l_message, l_n0+l_n1+l_n2+l_n3+l_n3unq);

   l_logResult(l_mkhst , in_filial_id, l_snapDate, l_clset_id, l_target_tab, '2End', 1,
                l_n0+l_n1+l_n2+l_n3+l_n3unq, l_n0, l_n1, l_n2, l_n3, l_n3unq, l_message);

   job_log_psv.end_log (l_message);
   -- ������� ����������
   v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
EXCEPTION
   WHEN OTHERS THEN
      -- ������� ����������
      v_lock_release := DBMS_LOCK.release (lockhandle => v_lockhandle);
      l_message := substr(dbms_utility.format_error_stack()||crlf||dbms_utility.format_error_backtrace(),1,1800);
      job_log_psv.write_detail_log(l_message); -- ����� ����������
      if length(l_sqlClob)>0 and (lower(l_sqlClob) like '%select %' or lower(l_sqlClob) like '%insert%' or lower(l_sqlClob) like '%alter%') then 
				  job_log_psv.write_detail_log(l_sqlClob); --����� ���������� SQL
      end if;          
      job_log_psv.end_log_with_error (in_error => l_message);
      l_logResult(l_mkhst , in_filial_id, l_snapDate, l_clset_id, l_target_tab, '2End', sqlcode,
                l_n0+l_n1+l_n2+l_n3+l_n3unq, l_n0, l_n1, l_n2, l_n3, l_n3unq, l_message);
      RAISE;
END;
  

end;
/
