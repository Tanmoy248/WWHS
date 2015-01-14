drop procedure if exists sp_eligibilitymemberterminated_select;

DELIMITER $$
CREATE PROCEDURE sp_eligibilitymemberterminated_select()
BEGIN
DECLARE date_job varchar(200);
declare v_column_id, v_col_hist_id bigint;

set v_col_hist_id := 0;

select max(eligibility_data_column_history_id) into v_col_hist_id
from eligibility_data_column_history 
where column_id in 
(select column_id from eligibility_file_column where sql_column_name = 'eligibility_end_date')
;


SELECT application_property_text
INTO date_job
FROM eligibility_application_property
WHERE  application_property_name = 'Switching_Job_Run_Date_Time';

drop temporary table if exists temp_terminate_records;

create temporary table temp_terminate_records
as(
select 
distinct
ed.client_id AS Client_ID,
ed.group_id AS Group_ID,
ed.member_id AS Beneficiary_ID,
case when eligibility_end_date <= curdate() 
then eligibility_end_date else '' end AS Termination_Date,
lr.lookup_key as Relationship_Code,
lr.lookup_value as Relationship_Code_Desc,
ed.member_first_name as First_Name,
ed.member_last_name as Last_Name,
ed.date_of_birth as Date_Of_Birth,
ed.email_address as Email,
ed.zipcode as ZipCode,
case when edch.oldvalue = '1' and edch.newvalue = '0' 	then 'Y' else 'N' end as Referral_Switch

FROM eligibility_data ed
inner join track_check_eligibility_service es
ON ed.eligibility_data_id=es.eligibility_data_id
left outer join
eligibility_data_column_history as edch
on 
ed.eligibility_data_id = edch.eligibility_data_id


left outer join
eligibility_data_column_history as edch_end_dt
on 
ed.eligibility_data_id = edch_end_dt.eligibility_data_id

INNER JOIN lookup lr
on ed.relationship_code_id=lr.lookup_key
and lr.lookup_group = 'RELATIONSHIP'
where ed.modified_date> date_format(trim(date_job), '%Y-%m-%d %H:%m:%s')
);

drop temporary table if exists temp_terminate_records_1;
create temporary table temp_terminate_records_1 as
select * from temp_terminate_records where 1=2
;

drop temporary table if exists temp_terminate_records_2;
create temporary table temp_terminate_records_2 as
select * from temp_terminate_records where 1=2
;

insert into temp_terminate_records_1
select * from temp_terminate_records where Referral_Switch = 'Y';

insert into temp_terminate_records_2
select * from temp_terminate_records_1;

insert into temp_terminate_records_1
select * from temp_terminate_records as ttr where Referral_Switch = 'N'
and ttr.Beneficiary_ID not in (select Beneficiary_ID from temp_terminate_records_2);


insert into temp_terminate_records_1
select 
distinct
ed.client_id AS Client_ID,
ed.group_id AS Group_ID,
ed.member_id AS Beneficiary_ID,
ed.eligibility_end_date AS Termination_Date,
lr.lookup_key as Relationship_Code,
lr.lookup_value as Relationship_Code_Desc,
ed.member_first_name as First_Name,
ed.member_last_name as Last_Name,
ed.date_of_birth as Date_Of_Birth,
ed.email_address as Email,
ed.zipcode as ZipCode,
'N' as Referral_Switch
from eligibility_data as ed
inner join track_check_eligibility_service es
ON ed.eligibility_data_id=es.eligibility_data_id
left outer join 
eligibility_data_history as edh
on 
ed.eligibility_data_id = edh.eligibility_data_id
and edh.audit_type = 'I'
INNER JOIN lookup lr
on ed.relationship_code_id=lr.lookup_key
and lr.lookup_group = 'RELATIONSHIP'
where ed.modified_date> date_format(trim(date_job), '%Y-%m-%d %H:%m:%s')
and edh.audit_type = 'I'
and ed.eligibility_end_date <= curdate()
and ed.member_id not in (select Beneficiary_ID from temp_terminate_records)
;

select count(1) into @rec_count from temp_terminate_records_1;

if (@rec_count = 0) then
select 'failure';
else
	select 'Client_ID','Group_ID','Beneficiary_ID','Termination_Date','Relationship_Code','Relationship_Code_Desc','First_Name','Last_Name','Date_Of_Birth','Email','ZipCode','Referral_Switch'
	union
	select distinct * from temp_terminate_records_1 where Referral_Switch = 'Y' or Termination_Date <> '';

end if;
end$$
DELIMITER ;
