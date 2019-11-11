/* 
libname calls econ and gridwork library: 
depending on which library you work with, include the path in the quotes.
Step 1: run this first. 
*/
libname econ '/corptreas24/CECL/inputs/Econ_data/';
libname gridwork '/egwork/SAS_work025300007FEA_ga016ada5.suntrust.com/'; 
run;
/* 
This creates a 'sample' table for econ/sas files, ensures that all of the data is present.
Step 2: run this proc. 
*/
proc sql; 
create table meta as  
select * 
from dictionary.columns
where memname like '%201905%' and libname = 'ECON' or libname = 'GRIDWORK' 
; 
quit;
/* 
declares cnt variable: 
takes the name of the column and assigns it to the cnt variable so
the next loop can use it.
Step 3: run this proc along with the %put 
*/
proc sql; 
select count(name) into :cnt  trimmed 
from dictionary.columns
where memname like '%201905%' and libname = 'ECON' or libname = 'GRIDWORK';
quit; 
 %put &cnt; 
/* 
declare variables for loop,
will loop through col 1 - col n in ECON and GRIDWORK 
Step 4: run this proc
*/
proc sql; 
select distinct name into :var1 - :var&cnt
from dictionary.columns
where memname like '%201905%' and libname = 'ECON' or libname = 'GRIDWORK';  
quit; 
proc datasets library=work kill noprint;
run;
quit;
options compress=yes;
/* 
macro for sums&nulls for sas files: 
using the cnt and var variables, loops through and sums each value found in the columns along 
with count of non-null values found in each file
Step 5: run the macro
*/
%macro sumit (); 

%do i = 1 %to &cnt;        
proc sql;
create table sum_&&var&i. as          
select                                      
"&&var&i" format = $20. as name length = 20,
sum(&&var&i)  as sum_sas, 
count(*)  as non_null_sas                                          
from econ.ECON_201905_M_BASE
where &&var&i is not null
;
quit;
%end;

%do j = 1 %to &cnt;          
proc sql;
create table su_m&&var&j. as          
select                                      
"&&var&j" format = $20. as name length = 20,
sum(&&var&j)  as sum_sas, 
count(*)  as non_null_sas                                            
from econ.ECON_201905_Q_BASE
where &&var&j is not null
; 
quit;
%end;

%do z = 1 %to &cnt;           
proc sql;
create table s_um&&var&z. as          
select                                      
"&&var&z" format = $20. as name length = 20,
sum(&&var&z)  as sum_sas, 
count(*)  as non_null_sas                                          
from econ.ECON_201905_M_OPT
where &&var&z is not null
; 
quit;
%end;

%do k = 1 %to &cnt;           
proc sql;
create table sum_s&&var&k. as          
select                                      
"&&var&k" format = $20. as name length = 20,
sum(&&var&k)  as sum_sas, 
count(*)  as non_null_sas                                          
from econ.ECON_201905_Q_OPT
where &&var&k is not null; 
quit;
%end;

%do r = 1 %to &cnt;      
proc sql;
create table sum4&&var&r. as          
select                                      
"&&var&r" format = $20. as name length = 20,
sum(&&var&r)  as sum_sas, 
count(*)  as non_null_sas                                          
from econ.ECON_201905_M_PESS
where &&var&r is not null; 
quit;
%end;

%do m = 1 %to &cnt;            
proc sql;
create table sum5&&var&m. as          
select                                      
"&&var&m" format = $20. as name length = 20,
sum(&&var&m)  as sum_sas, 
count(*)  as non_null_sas                                          
from econ.ECON_201905_Q_PESS
where &&var&m is not null; 
quit;
%end;

%MEND sumit; 
/*
%macro() function calls macros
Step 6: call the macro and run the compress option. This will take a few minutes 
*/
%sumit();
options compress=yes;
/* 
Series of data steps takes the results calculated from the macro and outputs it to
table all_2 and so on. Run these data steps to get outputs for sas files
Step 7: run all the data steps at once
*/ 
data all_sumn_MBASE;
set sum_:;
run;
data all_sumn_QBASE;
set su_m:;
run;
data all_sumn_MOPT;
set s_um:;
run;
data all_sumn_QOPT;
set sum_s:;
run;
data all_sumn_MPESS;
set sum4:;
run;
data all_sumn_QPESS;
set sum5:;
run;

options compress=yes; 

proc datasets library=work kill noprint;
run;
quit;
options compress=yes;
/* 
macro for sums&nulls for excel files, same as macro for sas files 
Repeat steps 5-7 for this macro
*/
%macro sumex (); 

%do a = 1 %to &cnt;        
proc sql;
create table sum_b&&var&a. as          
select                                      
"&&var&a" format = $20. as name length = 20,
sum(&&var&a)  as sum_excel, 
count(*)  as non_null_ex                                          
from GRIDWORK.ECON_201905_M_BASE
where &&var&a is not null;
quit;
%end;

%do b = 1 %to &cnt;          
proc sql;
create table su_m1&&var&b. as          
select                                      
"&&var&b" format = $20. as name length = 20,
sum(&&var&b)  as sum_excel, 
count(*)  as non_null_ex                                          
from gridwork.ECON_201905_Q_BASE
where &&var&b is not null; 
quit;
%end;

%do c = 1 %to &cnt;           
proc sql;
create table sum_c&&var&c. as          
select                                      
"&&var&c" format = $20. as name length = 20,
sum(&&var&c)  as sum_excel, 
count(*)  as non_null_ex                                          
from gridwork.ECON_201905_M_OPT
where &&var&c is not null; 
quit;
%end;

%do d = 1 %to &cnt;           
proc sql;
create table sum_3&&var&d. as          
select                                      
"&&var&d" format = $20. as name length = 20,
sum(&&var&d)  as sum_excel, 
count(*)  as non_null_ex                                          
from gridwork.ECON_201905_Q_OPT
where &&var&d is not null; 
quit;
%end;

%do e = 1 %to &cnt;      
proc sql;
create table sum_4&&var&e. as          
select                                      
"&&var&e" format = $20. as name length = 20,
sum(&&var&e)  as sum_excel, 
count(*)  as non_null_ex                                          
from gridwork.ECON_201905_M_PESS
where &&var&e is not null; 
quit;
%end;

%do f = 1 %to &cnt;            
proc sql;
create table sum_5&&var&f. as          
select                                      
"&&var&f" format = $20. as name length = 20,
sum(&&var&f)  as sum_excel, 
count(*)  as non_null_ex                                          
from gridwork.ECON_201905_Q_PESS
where &&var&f is not null; 
quit;
%end;

%MEND sumex; 

%sumex();
 options compress=yes; 
data ex_sumn_MBASE;
set sum_b:;
run;
data ex_sumn_QBASE;
set su_m1:;
run;
data ex_sumn_MOPT;
set sum_c:;
run;
data ex_sumn_QOPT;
set sum_3:;
run;
data ex_sumn_MPESS;
set sum_4:;
run;
data ex_sumn_QPESS;
set sum_5:;
run;

options compress=yes; 
/*
This proc is optional, but provides better readability, 
run this after running both macros and data steps
*/
proc sql;

select  
ex_sumn_QPESS.sum_excel as sum_exQPESS, all_sumn_QPESS.sum_sas  as sum_sasQPESS, 
ex_sumn_QPESS.non_null_ex as ex_null_count, all_sumn_QPESS.non_null_sas as all_null_count
from ex_sumn_QPESS, all_sumn_QPESS
where
ex_sumn_QPESS.sum_excel = all_sumn_QPESS.sum_sas;

select 
ex_sumn_MPESS.sum_excel as sum_exMPESS, all_sumn_MPESS.sum_sas  as sum_sasMPESS, 
ex_sumn_MPESS.non_null_ex as ex_null_count, all_sumn_MPESS.non_null_sas as all_null_count
from ex_sumn_MPESS, all_sumn_MPESS
where
ex_sumn_MPESS.sum_excel = all_sumn_MPESS.sum_sas; 

select 
ex_sumn_QOPT.sum_excel as sum_exQOPT, all_sumn_QOPT.sum_sas  as sum_sasQOPT, 
ex_sumn_QOPT.non_null_ex as ex_null_count, all_sumn_QOPT.non_null_sas as all_null_count
from ex_sumn_QOPT, all_sumn_QOPT
where
ex_sumn_QOPT.sum_excel = all_sumn_QOPT.sum_sas;

select 
ex_sumn_MOPT.sum_excel as sum_exMOPT, all_sumn_MOPT.sum_sas  as sum_sasMOPT, 
ex_sumn_MOPT.non_null_ex as ex_null_count, all_sumn_MOPT.non_null_sas as all_null_count
from ex_sumn_MOPT, all_sumn_MOPT
where
ex_sumn_MOPT.sum_excel = all_sumn_MOPT.sum_sas;

select 
ex_sumn_QBASE.sum_excel as sum_exQBASE, all_sumn_QBASE.sum_sas  as sum_sasQBASE, 
ex_sumn_QBASE.non_null_ex as ex_null_count, all_sumn_QBASE.non_null_sas as all_null_count
from ex_sumn_QBASE, all_sumn_QBASE
where
ex_sumn_QBASE.sum_excel = all_sumn_QBASE.sum_sas;

select 
ex_sumn_MBASE.sum_excel as sum_exMBASE, all_sumn_MBASE.sum_sas  as sum_sasMBASE, 
ex_sumn_MBASE.non_null_ex as ex_null_count, all_sumn_MBASE.non_null_sas as all_null_count
from ex_sumn_MBASE, all_sumn_MBASE
where
ex_sumn_MBASE.sum_excel = all_sumn_MBASE.sum_sas;

quit; 




/* 
GOAL: compare sums in excel & sas files
gridwork.filename= excel file 

steps: 
change gridwork path
call library
run meta table
run cnt and %cnt 
run loop
run macro
call macro
run data steps
*/ 