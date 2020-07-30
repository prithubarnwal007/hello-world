drop procedure contact_related_contacts_old_incr;
DELIMITER $$
CREATE DEFINER=`barracuda`@`%` PROCEDURE `contact_related_contacts_old_incr`()
BEGIN

-- Description
-- it handles only those records,for which email was present in the earlier load

-- output tables --
--  contact_master_account_incr_duplicate(matching from contacts_master_incr)
-- contact_master_many_account_master_duplicate(there is only one record for that email,account id)
-- contact_master_many_account_golden_duplicate(there are many records for a combination of email,accountid

-- before starting this proc,it is needed to drop the following tables :-
-- drop table new_email_df;
drop table old_distinct_many_account_email;
-- drop table contact_master_incr_df;
drop table contact_master_account_incr_duplicate;
drop table contact_master_many_account_duplicate; 
drop table contact_master_many_account_golden_duplicate;
drop table contact_master_many_account_master_duplicate;

-- create table old_email_df as 
-- select * from contacts.contact_consolidated_incremental_duplicate;

-- create index key_old_email_df on contact_consolidated_incremental_duplicate(email);

create table old_distinct_many_account_email 
as 
select email from contact_consolidated_incremental_duplicate group by email having count(distinct account_id)>1;

create index key_old_distinct_many_account_email on old_distinct_many_account_email(email);

-- create table contact_master_incr_df 
-- as 
-- select * from contacts.contacts_master_incr;

create table contact_master_account_incr_duplicate
as 
select A.*,1 as primary_flag,'U' as updated_flag from contacts_master_incr A 
join old_distinct_many_account_email B on A.email=B.email;

-- select * from contact_master_account_incr_duplicate limit 100;
-- create index key_contact_master_account_incr on contact_master_account_incr_duplicate(email);

create table contact_master_many_account_duplicate 
as 
select A.Id,A.account_id,
A.Alternative_Email__c,
A.email,
A.FirstName,
A.LastName,
A.LeadSource,
A.MailingCity,
A.MailingCountry,
A.MailingPostalCode,
A.MailingState,
A.MailingStreet,
A.MobilePhone,
A.On24_Profile__c,
A.OtherCity,
A.OtherCountry,
A.OtherPhone,
A.OtherPostalCode,
A.OtherState,
A.OtherStreet,
A.LinkedIn_URL__c,
A.Phone_Extension__c,
A.City,
A.Country,
A.State,
A.Street,
A.PostalCode,
A.Company,
A.street2,
A.Company_Phone__c,
A.bc_acnt_contact_email,
A.bc_oa_bill_email,
A.bc_oa_buyer_email,
A.bc_oa_ship_contact,
A.bc_oa_ship_email,
A.bc_acnt_contact_name,
A.bc_acnt_bill_email,
A.bc_acnt_buyer,
A.bc_acnt_sfid,
A.bc_acnt_name,
A.bc_acnt_ship_email,
A.bc_acnt_buyer_email,
A.bc_acnt_company_phone,
A.bu_manger_contact__c,
A.bypass_validation__c,
A.currencyisocode,
A.description,
A.contact_leadstatus__c,
A.createdbyid,
A.createddate,
A.dmr_region_vertical__c,
A.dplcheck_timeout__c,
A.emailbounceddate,
A.emailbouncedreason,
A.hasoptedoutofemail,
A.external_id__c,
A.name,
A.gdpr_source__c,
A.isemailbounced,
A.lastactivitydate,
A.lastmodifiedbyid,
A.lastmodifieddate,
A.partner_portal_access__c,
A.renewals_opt_in__c,
A.rundplcheck__c,
A.status__c,
A.unqualified_reason__c,
A.unverified_email_address__c,
A.use_alt_email_for_case__c,
A.GDPR_Date__c,
A.GDPR_Opt_In__c,
A.X18_Char_Acct_id__c,
A.authDBIDcontact__c,
A.Comments__c,
A.Contact_Counter__c,
A.Contact_Score__c,
A.Creative__c,
A.Creative_at_Distribution__c,
A.Demographic_Score__c,
A.Do_Not_Contact__c,
A.Do_Not_Send_Support_Survey__c,
A.Form_Filled_Out__c,
A.Interaction_Score__c,
A.Invalid_Email__c,
A.Lead_Source_at_Distribution__c,
A.Lead_Source_Detail__c,
A.Lead_Source_Detail_at_Distribution__c,
A.Medium_at_Distribution__c,
A.Medium_Detail_at_Distribution__c,
A.Original_Blog_Details__c,
A.Most_Recent_FeaturePS__c,
A.Most_Recent_FeatureRep__c,
A.Most_Recent_Form_Fill__c,
A.Most_Recent_Lead_Source__c,
A.Most_Recent_Lead_Source_Detail__c,
A.Most_Recent_Medium__c,
A.Most_Recent_Medium_Detail__c,
A.Most_Recent_Publication__c,
A.MREFURL__c,
A.MREFURL_at_Distribution__c,
A.Original_Blog__c,
A.Original_Medium__c,
A.Original_Medium_Detail__c,
A.PPC_Keyword__c,
A.Primary_Backup_Technician__c,
A.Primary_Business_Owner__c,
A.Primary_FW_Tech__c,
A.Trial_Start_Date__c,
A.Trial_User_Name__c,
A.Customer_Point_of_Contact__c,
A.Marketing_Point_of_Contact__c,
A.No_longer_at_company__c,
A.PM_Point_of_Contact__c,
A.Sales_Point_of_Contact__c,
A.Sonian_Admin__c,
A.unique_id,
A.pull_time,
A.group_id,
A.tag_id,
A.contact_stage_key,
A.website,
A.manufactured_record_flag,
A.Contact_Status__c,
A.contact_18_digit_id__c,
A.department,
A.ownerid,
A.Phone,
A.photourl,
A.informalname,
A.Salutation,
A.Title,
A.table_source,
A.email_without_domain_flag,
A.tablesource_priority,0 as primary_flag,'U' as updated_flag
 from contact_consolidated_incremental_duplicate A
join contacts_master_incr B 
join old_distinct_many_account_email C
on A.email=B.email 
and A.email=C.email
where A.account_id!=B.account_id;

create index key_contact_master_many_account on contact_master_many_account_duplicate(email);

create table contact_master_many_account_golden_duplicate
as select A.* from contact_master_many_account_duplicate A 
join 
(select email,account_id from contact_master_many_account_duplicate group by email,account_id having count(1)>1) B
 on A.email=B.email and A.account_id=B.account_id;

create table contact_master_many_account_master_duplicate 
as select A.* from contact_master_many_account_duplicate A
join (select email,account_id from contact_master_many_account_duplicate group by email,account_id having count(1)=1) B
on A.email=B.email and A.account_id=B.account_id;

-- select * from contact_master_many_accou	nt_master_duplicate;


END$$
DELIMITER ;
