create table poc_table
(
id int,
name_value varchar(256),
primary key(id)
);

ALTER DATABASE poc_database
SET CHANGE_TRACKING = ON  
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

ALTER TABLE poc_table  
ENABLE CHANGE_TRACKING  
WITH (TRACK_COLUMNS_UPDATED = ON);

select * from poc_database.dbo.poc_table

insert poc_database.dbo.poc_table
values(5,'eren')