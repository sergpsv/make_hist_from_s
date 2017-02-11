-- select * from dba_objects where object_name like '%MKHST%'
------------------------------------------------------------------------------------------------------------
CREATE SEQUENCE mkhst_seq
 START WITH     1
 INCREMENT BY   1
/ 
------------------------------------------------------------------------------------------------------------
-- drop table d_mkhst_col_methods
create table d_mkhst_col_methods
(
   svmtd_id         number
  ,save_method_def  varchar2(100)
  ,save_method_comm varchar2(500)
  ,constraint d_mkhst_col_methods  primary key (svmtd_id)
)
/
comment on table d_mkhst_col_methods is '������ ��� ���������� ����� ������� � ������������ �������';
comment on column d_mkhst_col_methods.svmtd_id         is 'ID ������';
comment on column d_mkhst_col_methods.save_method_def  is '�������� ������';
comment on column d_mkhst_col_methods.save_method_comm is '����������� � ������';

------------------------------------------------------------------------------------------------------------
--drop table d_mkhst_col_defaults
create table d_mkhst_col_defaults
(
  column_mask    varchar2(30),
  svmtd_svmtd_id integer,
  constraint d_mkhst_col_defaults_fk foreign key (svmtd_svmtd_id) references d_mkhst_col_methods(svmtd_id)
)
/
comment on table  d_mkhst_col_defaults                is '����� � ��������� �����, �� ������ ������� ���������� ����� ���������� ���� � �������';
comment on column d_mkhst_col_defaults.column_mask    is '�����';
comment on column d_mkhst_col_defaults.svmtd_svmtd_id is '�����(partition_keep_columns_method)';

create or replace trigger d_mkhst_col_defaults_up
before insert on d_mkhst_col_defaults
for each row
begin
  :new.column_mask := upper(:new.column_mask);
end;
/

------------------------------------------------------------------------------------------------------------
-- drop table d_mkhst_col_types 
create table d_mkhst_col_types
(
   clmtp_id  number
  ,def       varchar2(50)
  ,comm      varchar2(500)
  ,constraint d_mkhst_col_types_pk  primary key (clmtp_id)
)
/
comment on table  d_mkhst_col_types          is '��� ���������������� ������� � ����������� ������';
comment on column d_mkhst_col_types.clmtp_id is '����';
comment on column d_mkhst_col_types.def      is '������� ��������';
comment on column d_mkhst_col_types.comm     is '������ �������� � ������';

------------------------------------------------------------------------------------------------------------
-- select * from d_mkhst_col_sets
create table d_mkhst_col_sets 
(
   clset_id       number
  ,column_name    varchar2(30)
  ,svmtd_svmtd_id number
  ,clmtp_clmtp_id number
  ,column_comm    varchar2(200)
  ,start_date     date          default sysdate
  ,end_date       date          default to_date('31/12/2999','dd/mm/yyyy')
  ,navi_date      date          default sysdate
  ,navi_user      varchar2(100) default nvl(sys_context('userenv','proxy_user'), user)
  ,constraint cs_mkhst_pkcl_pk   primary key (clset_id, column_name, start_date)
  ,constraint cs_mkhst_svmtd_fk  foreign key (svmtd_svmtd_id) references d_mkhst_col_methods(svmtd_id)
  ,constraint cs_mkhst_clmtp_fk  foreign key (clmtp_clmtp_id) references d_mkhst_col_types(clmtp_id)
  ,constraint cs_mkhst_str_end   check       (start_date<end_date)
)
/
-- �����������, ����� � ����� ������ ��� ������ ���� ������� � ����� 2
create unique index cs_mkhst_one_clmtp_2_in_clset 
  on d_mkhst_col_sets(case when clmtp_clmtp_id=2 then clset_id       else null end,
                      case when clmtp_clmtp_id=2 then clmtp_clmtp_id else null end)
/
comment on table  d_mkhst_col_sets is '������ ������� �� ������� ������, �� ������� ���� ����������. � ����� ������� ����� ���� ������ ��� � �������, �� ������ ������.';
comment on column d_mkhst_col_sets.clset_id     is 'ID ������ �����. ����� ����� ���� �������� ��� ��������� ������(�������� ������������ �� �.������';
comment on column d_mkhst_col_sets.column_name is '������� - ���� ������� ���������';
comment on column d_mkhst_col_sets.svmtd_svmtd_id is '����� �� ��������� (d_mkhst_col_methods)';
comment on column d_mkhst_col_sets.clmtp_clmtp_id is '�������� �� ������ ������� ��������� ������? (����� ���������, ��� primary key index \ (��� ������� ���� ������� � ������ ������ ���� ��������)';
comment on column d_mkhst_col_sets.start_date   is '���� � ������� ��������� �����';
comment on column d_mkhst_col_sets.end_date     is '���� �� ������� ��������� �����';
comment on column d_mkhst_col_sets.navi_date    is '����� ����';
comment on column d_mkhst_col_sets.navi_user    is '��� ����';


------------------------------------------------------------------------------------------------------------
--drop table m_mkhst_tables;
create table m_mkhst_tables 
(
   mkhst_id      number        not null
  ,source_tab   varchar2(60)  not null
  ,target_tab   varchar2(60)  not null
  ,enabl        number        default 0
  ,clset_clset_id number       not null
  ,maxDayDiff   number        default 1 not null 
  ,datePicture  varchar(30)   default 'yyyymmdd'
  ,navi_date    date          default sysdate
  ,navi_user    varchar2(30)  default nvl(sys_context('userenv','proxy_user'), user)
  ,constraint m_mkhst_tables_en_chk  check  (enabl in (0,1))
  ,constraint m_mkhst_tables_pk      primary key (mkhst_id)
  ,constraint m_mkhst_dayDiff_chk    check (maxDayDiff >0)
)
pctfree 0 
/
create unique index m_mkhst_tables_src_upper_uk on m_mkhst_tables(upper(source_tab), clset_clset_id)
/ 
create unique index m_mkhst_tables_trg_upper_uk on m_mkhst_tables(upper(target_tab), clset_clset_id)
/

create or replace trigger m_mkhst_tables_4id
before insert on m_mkhst_tables
for each row
begin
  if :new.mkhst_id is null then 
    :new.mkhst_id := mkhst_seq.nextval; -- ora11 allow to use ":=" instead "select ..."
  end if;
  :new.source_tab := upper(:new.source_tab);
  :new.target_tab := upper(:new.target_tab);
end;
/
create or replace trigger m_mkhst_tables_chk
before insert on m_mkhst_tables
for each row
declare
  l_cnt integer;
  po_owner_src  varchar2(50);
  po_table_src  varchar2(50);
  po_link_src   varchar2(50);
  l_tab         varchar2(250) := upper(:new.source_tab); --�.�. ������ ���� �����.�������@����
  po_owner_tgt  varchar2(50);
  po_table_tgt  varchar2(50);
  po_link_tgt   varchar2(50);  
  dateFormatNotRecognized  EXCEPTION;
  PRAGMA EXCEPTION_INIT(dateFormatNotRecognized, -1821);
begin
	-- 1 -- � ������� d_mkhst_col_sets ������ ���� ������� ������������ clset_clset_id
	select count(1) into l_cnt from d_mkhst_col_sets where clset_id = :new.clset_clset_id;
  if l_cnt =0 then
    raise_application_error(-20000, '� ������� d_mkhst_col_sets ���������� �������������� ������� ������� ��� �������(cset_id) ������� '||:new.source_tab);
  end if;
  -- �������� ��������
  l_cnt := instr(l_tab,'@');  
  if l_cnt>0 then 
    po_link_src := substr(l_tab, l_cnt); -- dblink
    l_tab       := substr(l_tab, 1, l_cnt-1); -- schema.table
  end if;
  po_owner_src := nvl(substr(l_tab,1,instr(l_tab,'.')-1),user); -- nvl(null,user) or schema
  po_table_src := substr(l_tab,case when instr(l_tab,'.')=0 then 1 else instr(l_tab,'.')+1 end, 
                                 case when instr(l_tab,'@')=0 then length(l_tab) else instr(l_tab,'@')-instr(l_tab,'.')-1 end );
  -- 2 -- �������� ��������
  select count(1) into l_cnt from all_objects 
   where object_name = po_table_src and owner=po_owner_src and object_type in ('VIEW', 'TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION');
  if l_cnt =0 then
    raise_application_error(-20000, '� ���� ��� ���������� ������� ��� ������������� '||po_owner_src||'.'||po_table_src||' (��� ���� �� select)');
  end if;
  -- �������� ������� �������
  l_tab := upper(:new.target_tab);
  l_cnt := instr(l_tab,'@');  
  if l_cnt>0 then 
    po_link_tgt := substr(l_tab, l_cnt); -- dblink
    l_tab       := substr(l_tab, 1, l_cnt-1); -- schema.table
  end if;
  po_owner_tgt := nvl(substr(l_tab,1,instr(l_tab,'.')-1),user); -- nvl(null,user) or schema
  po_table_tgt := substr(l_tab,case when instr(l_tab,'.')=0 then 1 else instr(l_tab,'.')+1 end, 
                                 case when instr(l_tab,'@')=0 then length(l_tab) else instr(l_tab,'@')-instr(l_tab,'.')-1 end );
  -- 3 -- �������� ������� �������
  select count(1) into l_cnt from all_tables where owner=po_owner_tgt and table_name = po_table_tgt;
  if l_cnt =0 then
    select count(1) into l_cnt from all_views where owner=po_owner_tgt and view_name = po_table_tgt;
    if l_cnt=1 then
      raise_application_error(-20000, '� ���� ���� view '||po_owner_tgt||'.'||po_table_tgt||', � ����� ���������� �������');
    else
      raise_application_error(-20000, '� ���� ��� ���������� ������� '||po_owner_tgt||'.'||po_table_tgt||' (��� ���� �� select). ����������� make_hist_from_s.s100_CreateTargetTable(?, '||:new.source_tab||')');
    end if;
  end if;

  -- 4 -- �������� <> ����������
  if upper(po_owner_src||'.'||po_table_src)=upper(po_owner_tgt||'.'||po_table_tgt) then 
    raise_application_error(-20000, '������� �������� '||upper(po_owner_src||'.'||po_table_src)||' � ������� ������� '||upper(po_owner_tgt||'.'||po_table_tgt)||' ������ ���� �������');
  end if;  
  
  begin
		 po_owner_tgt  := to_char(sysdate, :new.datePicture);
  exception
	  when dateFormatNotRecognized then 
      raise_application_error(-20000, '�������� � ���� "datePicture" �� �������� ���������� �������� �������������� ������ � ����. ���� ����� ��� ��������� ���� �� ���� �������-��������� ('||upper(po_owner_src||'.'||po_table_src)||') �� ��������� � ��������� "make_hist_from_s.AddSnap2End"');
	end;
end;
/

comment on table m_mkhst_tables is '������ ������, ������� �������� �������������';
comment on column m_mkhst_tables.mkhst_id   is 'ID ������';
comment on column m_mkhst_tables.source_tab is '��� �������, ������� ����� ������������ (����� ������� �������� �����)';
comment on column m_mkhst_tables.target_tab is '��� �������, � ������� ����� ������������ (����� ������� �������� �����)';
comment on column m_mkhst_tables.enabl      is '0-������ \ 1-����� (��� ��������� ������������ ��������)';
comment on column m_mkhst_tables.clset_clset_id is '������ �� �����, ������� ��������� ������� ���������� source � target/ ��� ����� source ����� ���� ��������� ������� ������';
comment on column m_mkhst_tables.maxDayDiff   is '������������ ���-�� ����, ������� ��������� ����� ������� ������� � �������(start_date) � ����������� (snap_date)';
comment on column m_mkhst_tables.datePicture  is '������ �������������� �� ������ � ����. ������ �������� ��� �������� �����������. �������� ����� ������� ���� � ������ �������. ���� ������������� ��� �������������� ���������� ���� ��� ������ � ����';
comment on column m_mkhst_tables.navi_date  is '����� �������� ������';
comment on column m_mkhst_tables.navi_user  is '��� ������� ������';

----------------------------------------------------------------------------
-- drop table f_mkhst_results
create table f_mkhst_results
(
   mkhst_mkhst_id   number
  ,filial_id        number
  ,snapDate         date
  ,addMethod        varchar2(10)
  ,exitCode         number
  ,total_reload     number
  ,unchanged        number
  ,newly            number
  ,closed           number
  ,chng_closed      number
  ,chng_opened      number
  ,navi_date        date         default sysdate
  ,navi_user        varchar(100) default nvl(sys_context('userenv','proxy_user'), user)
  ,comm             varchar2(2000)
  ,constraint rslt_mkhst_fk         foreign key (mkhst_mkhst_id) references m_mkhst_tables(mkhst_id)
  ,constraint rslt_mkhst_addm_chk   check  (addMethod  in ('2End', '2Start', 'Ins'))
  ,constraint rslt_mkhst_total_chk  check  (total_reload = nvl(unchanged,0)+nvl(newly,0)+nvl(closed,0)+nvl(chng_closed,0)+nvl(chng_opened,0))
)  
/
comment on table  f_mkhst_results is '������ ������� �� ������� ������, �� ������� ���� ����������. � ����� ������� ����� ���� ������ ��� � �������, �� ������ ������.';
comment on column f_mkhst_results.mkhst_mkhst_id is 'ID ����������� �������. m_mkhst_tables';
comment on column f_mkhst_results.filial_id     is '�� ������ ������� ��������';
comment on column f_mkhst_results.snapDate      is '���� ������������ ������';
comment on column f_mkhst_results.addMethod     is '����� ���������� ������ "2End", "2Start", "Ins"';
comment on column f_mkhst_results.exitCode      is '������ ���� - ��� ��� ������, 1-�������';
comment on column f_mkhst_results.total_reload  is '����� ������������� �������';
comment on column f_mkhst_results.unchanged     is '�������� ���������� � ������� ��� ��������� ������ ������';
comment on column f_mkhst_results.newly         is '���������� ����� ������� �� �������';
comment on column f_mkhst_results.closed        is '�������� ��������� ��� ���������� ������';
comment on column f_mkhst_results.chng_closed   is '�������� ��������� � �������. �������� ����� ������ ������';
comment on column f_mkhst_results.chng_opened   is '�������� ����� ������ ��� ������������ � �������� � �������. �������� ����� ������ ������';
comment on column f_mkhst_results.navi_date     is '���� �������\����������';
comment on column f_mkhst_results.navi_user     is '��� �������';
comment on column f_mkhst_results.comm          is '����� �������';

----------------------------------------------------------------------------
-- drop table f_mkhst_results
create table f_mkhst_high_changed
(
   clset_clset_id   number
  ,filial_id        number
  ,snapDate         date not null
  ,column_name      varchar2(50) not null
  ,frequence        number not null
)
/
comment on table  f_mkhst_high_changed is '�������, ����������� ���������� ������� �� ��������� ����� ������������ ������.';
comment on column f_mkhst_high_changed.clset_clset_id is 'ID ������. d_mkhst_col_sets';
comment on column f_mkhst_high_changed.filial_id     is '�� ������ ������� ��������';
comment on column f_mkhst_high_changed.snapDate      is '���� ������������ ������';
comment on column f_mkhst_high_changed.column_name   is '��� �������';
comment on column f_mkhst_high_changed.frequnce      is '������� ������ � ������� "����������"';

--===============================================================================================================================
-- ������ �������� ��������� �������
--------------------------------------------------------------------------------------
insert into d_mkhst_col_types values (1, '���������� ��������� ����' , '�������� �� ������ � �������� ������� � �� ���� ����� ������������ ������ � �������. ��������, filial_id. ����� ���� �� ��������� ��� ��������� ���� ���������');
insert into d_mkhst_col_types values (2, '������ ��������� ����' ,     '�������� �� ������ � �������� ������� � �� ���� ����� ������������ ������ � �������. ��������, subs_subs_id. ����� ���� �� ��������� ��� ��������� ���� ���������. �� ���� ����� � �������� ������� ������� ��������� ������������������ ������');
insert into d_mkhst_col_types values (3, '��������� ����(�.�.������)', '�������� ������ � ��������� ����� �������� ������� � �� ���� ����� ������������ ������ � ������� ����� NVL. ��������, bill_bill_id. ����� ���� �� ��������� ��� ��������� ���� ���������');
commit;
--------------------------------------------------------------------------------------
insert into d_mkhst_col_methods values (-1, '��������� ������� ���� �� �������!!!','���� � ����� ���� ��������� �������������, �.�. ���������� ��������� �������');
insert into d_mkhst_col_methods values (0, '�� ��������� ���� � �������','�������� ���� ������');
insert into d_mkhst_col_methods values (1, '���������, ���� ���������� ������','����� ��������� �������������� ��� ��������');
insert into d_mkhst_col_methods values (2, '��������� ��� 1� ����� � ����.���� ������. ��������� ��� - ��������� ��� 3� �����','��������, ������ �� ����� ������');
insert into d_mkhst_col_methods values (3, '��������� ������� �� ��������� ���������� ��������','�������� ����� ����� � ����');
insert into d_mkhst_col_methods values (4, '��������� ������ ������ �� ������ ��������, ���������� ��������� ������������','�������� INIT_ ���������');
insert into d_mkhst_col_methods values (5, '���������. �������� � ��� ������������ �� start_date ��� �������� ����� �������.','��������, sum(DURATION) �� ���� ��� ���������� � ������� �������� ���������� ��������. ���� ������ ��������� ���������� �������� - �� ����������� � ��� ������������ ����� ������� ������������� ���� �� start_date �������. ���� ������ �������� � �������� start_date - �������� ����������� ��� ����.');
commit;
--------------------------------------------------------------------------------------
insert into d_mkhst_col_defaults values ('SNAP_DATE', 0);
insert into d_mkhst_col_defaults values ('SNAP_MON', 0);
insert into d_mkhst_col_defaults values ('%DUR', 3);
insert into d_mkhst_col_defaults values ('%AGE', 3);
insert into d_mkhst_col_defaults values ('BALANCE_AMOUNT', 3);
insert into d_mkhst_col_defaults values ('NAVI_DATE', 3);
commit;

--------------------------------------------------------------------------------------
-- select * from d_mkhst_col_sets order by clset_id desc, column_comm, start_date asc
declare
  res_n number :=1;
  res_c clob;
begin
	res_n := make_hist_from_s.s102_CreateColumnSetRules('pub_ds.S_SUBSCRIBERS', res_n);
	res_n := make_hist_from_s.s100_CreateTargetTable(p_cset_id => res_n, p_source_name => 'pub_ds.S_SUBSCRIBERS', o_sql=>res_c);
end;
/
update d_mkhst_col_sets set CLMTP_CLMTP_ID=2 where column_name in('SK_SUBS_ID') and clset_id = (select max(clset_id) from d_mkhst_col_sets)
/
-------------------------------------------
declare
  res_n number :=2;
  res_c clob;
begin
	res_n := make_hist_from_s.s102_CreateColumnSetRules('pub_ds.S_SUBS_ACTIVITIES', res_n);
	res_n := make_hist_from_s.s100_CreateTargetTable(p_cset_id => res_n, p_source_name => 'pub_ds.S_SUBS_ACTIVITIES', o_sql=>res_c);
end;
/
update d_mkhst_col_sets set CLMTP_CLMTP_ID=2 where column_name in('SK_SUBS_ID') and clset_id = (select max(clset_id) from d_mkhst_col_sets)
/
-------------------------------------------
-- select * from d_mkhst_col_sets where clset_id=3 order by clset_id desc, column_comm, start_date asc
declare
  res_n number :=3;
  res_c clob;
begin
	res_n := make_hist_from_s.s102_CreateColumnSetRules('pub_ds.S_CLIENTS', res_n);
	res_n := make_hist_from_s.s100_CreateTargetTable(p_cset_id => res_n, p_source_name => 'pub_ds.S_CLIENTS', o_sql=>res_c);
end;
/
update d_mkhst_col_sets set CLMTP_CLMTP_ID=2 where column_name in('SK_CLNT_ID') and clset_id = (select max(clset_id) from d_mkhst_col_sets)
/
-------------------------------------------
--select * from d_mkhst_col_sets where clset_id=4 order by column_comm
declare
  res_n number:=4;
  res_c clob;
begin
--	res_n := make_hist_from_s.s102_CreateColumnSetRules('pub_ds.S_DEALER_ACCOUNTS_DAILY', res_n);
	res_n := make_hist_from_s.s100_CreateTargetTable(p_cset_id => res_n, p_source_name => 'pub_ds.S_DEALER_ACCOUNTS_DAILY',o_sql=>res_c);
end;
/
update d_mkhst_col_sets set CLMTP_CLMTP_ID=1 where column_name in('DELR_DELR_ID') 
  and clset_id = (select max(clset_id) from d_mkhst_col_sets where column_comm like '%S_DEALER_ACCOUNTS_DAILY%')
/  
update d_mkhst_col_sets set CLMTP_CLMTP_ID=2 where column_name in('SK_CLNT_ID') 
  and clset_id = (select max(clset_id) from d_mkhst_col_sets where column_comm like '%S_DEALER_ACCOUNTS_DAILY%')
/
update d_mkhst_col_sets set SVMTD_SVMTD_ID=2 where column_name in('BALANCE', 'REST_SIM_CARDS') 
  and clset_id = (select max(clset_id) from d_mkhst_col_sets where column_comm like '%S_DEALER_ACCOUNTS_DAILY%')
/

-------------------------------------------
-- �� ���� ������� ��� ������ ������ �������������, �.�. �� ������ �������� ������ ������� �������������
-- select * from d_mkhst_col_sets where clset_id=4 order by column_comm
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'BILLING_FILIAL_ID', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0001*/ ������������� �������� �������', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'FILIAL_ID', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0002*/ id �������', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'SNAP_MON', 0, null, '/*S_CLNT_DEBET_MONTHLY, 0003*/ �������� �����', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'DEBT_MON', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0004*/ ����� ����������� ��', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-05-2016 23:59:58', 'dd-mm-yyyy hh24:mi:ss'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'DEBT_MON', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0004*/ ����� ����������� ��', to_date('31-05-2016 23:59:59', 'dd-mm-yyyy hh24:mi:ss'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'CLNT_CLNT_ID', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0005*/ id �������', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'SK_CLNT_ID', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0006*/ ����������� ������������� �������', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'SUM_OST_DEBET', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0007*/ ����� ��', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'BILL_BILL_ID', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0008*/ bill_id ������������� ��', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-05-2016 23:59:58', 'dd-mm-yyyy hh24:mi:ss'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'BILL_BILL_ID', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0008*/ bill_id ������������� ��', to_date('31-05-2016 23:59:59', 'dd-mm-yyyy hh24:mi:ss'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (5, 'SAP_CLNT_ID', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0009*/ ������������� ������� �� �������� M_SAP_CLIENTS', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'BILLING_FILIAL_ID', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0001*/ ������������� �������� �������', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'FILIAL_ID', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0002*/ id �������', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'SNAP_MON', 0, null, '/*S_CLNT_DEBET_MONTHLY, 0003*/ �������� �����', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'DEBT_MON', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0004*/ ����� ����������� ��', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-05-2016 23:59:58', 'dd-mm-yyyy hh24:mi:ss'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'DEBT_MON', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0004*/ ����� ����������� ��', to_date('31-05-2016 23:59:59', 'dd-mm-yyyy hh24:mi:ss'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'CLNT_CLNT_ID', 1, 2, '/*S_CLNT_DEBET_MONTHLY, 0005*/ id �������', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'SK_CLNT_ID', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0006*/ ����������� ������������� �������', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'SUM_OST_DEBET', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0007*/ ����� ��', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'BILL_BILL_ID', 1, 3, '/*S_CLNT_DEBET_MONTHLY, 0008*/ bill_id ������������� ��', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-05-2016 23:59:58', 'dd-mm-yyyy hh24:mi:ss'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'BILL_BILL_ID', 1, 1, '/*S_CLNT_DEBET_MONTHLY, 0008*/ bill_id ������������� ��', to_date('31-05-2016 23:59:59', 'dd-mm-yyyy hh24:mi:ss'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');
insert into d_mkhst_col_sets (CLSET_ID, COLUMN_NAME, SVMTD_SVMTD_ID, CLMTP_CLMTP_ID, COLUMN_COMM, START_DATE, END_DATE, NAVI_DATE, NAVI_USER)
values (6, 'SAP_CLNT_ID', 1, null, '/*S_CLNT_DEBET_MONTHLY, 0009*/ ������������� ������� �� �������� M_SAP_CLIENTS', to_date('01-01-2000', 'dd-mm-yyyy'), to_date('31-12-2999', 'dd-mm-yyyy'), to_date('19-01-2017 22:47:45', 'dd-mm-yyyy hh24:mi:ss'), 'SERGEY_PARSHUKOV');

declare
  res_n number:=5;
  res_c clob;
begin
	res_n := make_hist_from_s.s100_CreateTargetTable(p_cset_id => res_n, p_source_name => 'pub_ds.S_CLNT_DEBET_MONTHLY',o_sql=>res_c);
end;
/

-------------------------------------------
-- select * from d_mkhst_col_sets order by clset_id desc, column_comm, start_date asc
declare
  res_n number := 7;
  res_c clob;
begin
	res_n := make_hist_from_s.s102_CreateColumnSetRules('pub_ds.s_number_sets', res_n);
	res_n := make_hist_from_s.s100_CreateTargetTable(p_cset_id => res_n, p_source_name => 'pub_ds.s_number_sets', o_sql=>res_c);
end;
/
update d_mkhst_col_sets set CLMTP_CLMTP_ID=2 where column_name in('MSISDN') and clset_id = (select max(clset_id) from d_mkhst_col_sets)
/


-------------------------------------------
-- select * from d_mkhst_col_sets order by clset_id desc, column_comm, start_date asc
declare
  res_n number := 8;
  res_c clob;
begin
--	res_n := make_hist_from_s.s102_CreateColumnSetRules('pub_ds.f_subs_gup_profiles', res_n);
	res_n := make_hist_from_s.s100_CreateTargetTable(p_cset_id => res_n, p_source_name => 'pub_ds.f_subs_gup_profiles', o_sql=>res_c);
end;
/

select * from d_mkhst_col_sets where clset_id = 8
update d_mkhst_col_sets set svmtd_svmtd_id=0 where clset_id = 8 and column_name='END_DATE'
update d_mkhst_col_sets set clmtp_clmtp_id=2 where clset_id = 8 and column_name='SK_SUBS_ID'

delete from d_mkhst_col_sets where clset_id = 8
drop table h_subs_gup_profiles
select * from h_subs_gup_profiles


---------------------------------------------
-- ���������� ���� ������� � ������� �������������
--truncate table m_mkhst_tables
insert into m_mkhst_tables(source_tab, target_tab, enabl, clset_clset_id ) values ('pub_ds.s_subscribers',     'h_subscribers',     1, 1 );
insert into m_mkhst_tables(source_tab, target_tab, enabl, clset_clset_id ) values ('pub_ds.s_subs_activities', 'H_subs_activities', 1, 2 );
insert into m_mkhst_tables(source_tab, target_tab, enabl, clset_clset_id ) values ('pub_ds.s_clients',         'H_clients',         1, 3 );
insert into m_mkhst_tables(source_tab, target_tab, enabl, clset_clset_id ) values ('pub_ds.S_DEALER_ACCOUNTS_DAILY', 'H_DEALER_ACCOUNTS_DAILY',1, 4 );
insert into m_mkhst_tables(source_tab, target_tab, enabl, clset_clset_id, MAXDAYDIFF, datepicture) 
     values ('pub_ds.S_CLNT_DEBET_MONTHLY', 'h_CLNT_DEBET_MONTHLY',   1, 5, 31,'yyyymmdd hh24miss'); -- �� ���� ������� ��� ������ ������, ������ ��� � ������ �������� ������ ���� ������ ���������� ���� BILL_BILL_ID
insert into m_mkhst_tables(source_tab, target_tab, enabl, clset_clset_id, MAXDAYDIFF, datepicture) 
     values ('pub_ds.S_CLNT_DEBET_MONTHLY', 'h_CLNT_DEBET_MONTHLY',   1, 6, 31,'yyyymmdd hh24miss');
insert into m_mkhst_tables(source_tab, target_tab, enabl, clset_clset_id) values ('pub_ds.S_number_sets',       'h_number_sets',1, 7);
commit;

--===============================================================================================================================
-- ��������� ��� ����������:
select * from dba_objects where object_name like '%MKHST%' and owner=user order by object_type; -- ��� ������� ������ �������� ������������
select * from m_mkhst_tables;       -- ���, ���� � ����� ������� ������������ � �������
select * from d_mkhst_col_methods;  -- ������ ������������
select * from d_mkhst_col_defaults; -- ��������� ������ �� ��������� ������� ��� �������� ������
select * from d_mkhst_col_sets;     -- ������ ������� � ��������
--===================================

/*
drop table h_subscribers;
drop table h_subscribers_tmp;
drop table h_SUBS_ACTIVITIES;
drop table h_SUBS_ACTIVITIES_tmp;
drop table h_CLIENTS;
drop table h_CLIENTS_tmp;
drop table h_DEALER_ACCOUNTS_DAILY;
drop table h_DEALER_ACCOUNTS_DAILY_tmp;
drop table h_CLNT_DEBET_MONTHLY;
drop table h_CLNT_DEBET_MONTHLY_tmp;
drop table h_number_sets;
drop table h_number_sets_tmp;

drop table F_MKHST_RESULTS;
drop table M_MKHST_TABLES;
drop table D_MKHST_COL_SETS;
drop table D_MKHST_COL_TYPES;
drop table D_MKHST_COL_DEFAULTS;
drop table D_MKHST_COL_METHOD;
drop SEQUENCE MKHST_SEQ;
*/
