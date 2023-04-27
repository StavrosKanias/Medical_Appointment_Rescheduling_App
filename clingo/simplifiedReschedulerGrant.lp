person("21088977182").
person("21088977183").
person("21088977184").
person("21088977185").
person("21088977186").
person("21088977187").
person("21088977188").
firstname("21088977182","Dimitrios").
firstname("21088977183","Nikos").
firstname("21088977184","Giorgos").
firstname("21088977185","Ioannis").
firstname("21088977186","Maria").
firstname("21088977187","Despoina").
firstname("21088977188","Kostas").
lastname("21088977182","Dimitriou").
lastname("21088977183","Nikou").
lastname("21088977184","Georgiou").
lastname("21088977185","Ioannou").
lastname("21088977186","Marietti").
lastname("21088977187","Panagiotopoulou").
lastname("21088977188","Karakostas").
phone("21088977182","6978909873").
phone("21088977183","6978909845").
phone("21088977184","6978909846").
phone("21088977185","6978903456").
phone("21088977186","6945609846").
phone("21088977187","6972349846").
phone("21088977188","6972747848").
email("21088977182","dimitriosdimitriou@gmail.com").
email("21088977183","nikosnikou@gmail.com").
email("21088977184","giogrosgeorgiou@gmail.com").
email("21088977185","ioannisioannou@gmail.com").
email("21088977186","mariamarietti@gmail.com").
email("21088977187","despoinapanagiotopoulou@gmail.com").
email("21088977188","kostaskarakostas@gmail.com").
birth_date("21088977182","1985-05-05").
birth_date("21088977183","1990-05-05").
birth_date("21088977184","1991-05-05").
birth_date("21088977185","1992-05-05").
birth_date("21088977186","1995-05-05").
birth_date("21088977187","1997-05-05").
birth_date("21088977188","1992-05-05").
specialty("Cardiology").
doctor("21088977182").
upin("21088977182","cfnasnd123").
doctor_available("21088977182").
specialty_title("21088977182","Cardiology").
patient("21088977183").
patient("21088977184").
patient("21088977185").
patient("21088977186").
patient("21088977187").
patient("21088977188").
priority("21088977183",85).
priority("21088977184",81).
priority("21088977185",43).
priority("21088977186",36).
priority("21088977187",57).
priority("21088977188",80).
timeslot(1).
timeslot(2).
timeslot(3).
timeslot(4).
date(1,"2023-05-01").
date(2,"2023-05-02").
date(3,"2023-05-04").
date(4,"2023-05-05").
time(1,"09:00:00").
time(2,"13:00:00").
time(3,"11:00:00").
time(4,"15:00:00").
timeslot_available(1).
timeslot_available(2).
timeslot_available(3).
timeslot_available(4).
doctor_id(1,"21088977182").
doctor_id(2,"21088977182").
doctor_id(3,"21088977182").
doctor_id(4,"21088977182").
request(2).
request(3).
request(4).
request(5).
request(6).
request(7).
request(8).
request(9).
request(10).
patient_id(2,"21088977184").
patient_id(3,"21088977188").
patient_id(4,"21088977184").
patient_id(5,"21088977185").
patient_id(6,"21088977186").
patient_id(7,"21088977185").
patient_id(8,"21088977186").
patient_id(9,"21088977188").
patient_id(10,"21088977187").
timeslot_id(2,1).
timeslot_id(3,1).
timeslot_id(4,2).
timeslot_id(5,2).
timeslot_id(6,2).
timeslot_id(7,3).
timeslot_id(8,3).
timeslot_id(9,4).
timeslot_id(10,4).
preference(2,1).
preference(3,1).
preference(4,2).
preference(5,1).
preference(6,1).
preference(7,2).
preference(8,1).
preference(9,2).
preference(10,1).
score(2,87).
score(3,86).
score(4,72).
score(5,60).
score(6,55).
score(7,45).
score(8,40).
score(9,71).
score(10,70).
status(2,0).
status(3,0).
status(4,1).
status(5,0).
status(6,0).
status(7,1).
status(8,0).
status(9,1).
status(10,0).


% RULES
%% GRANTED RULE
granted(R) :- status(R, S), S = 1.
%% WAITING RULE
appointed(P) :- not {request(R)}, patient_id(R, P), granted(R).
%% BETTER RULE
better(R,X) :- score(R, S), score(X, SX), R != X, S > SX. 
% SET CREATION
{grant(R)} 1 :- granted(R).
{grant(R)} 1 :- not granted(R), patient_id(R,P), granted(X), patient_id(X,P), better(R,X).
{grant(R)} 1 :- patient_id(R,P), not appointed(P).
% INTEGRITY CONSTRAINTS
%% A timeslot can be appointed only to one patient
:- grant(R1), timeslot_id(R1, T), grant(R2), timeslot_id(R2, T), R1 != R2.
%% Only one timeslot can be appointed to a patient
:- grant(R1), patient_id(R1, P), grant(R2), patient_id(R2, P), R1 != R2.

% OPTIMIZATION
#maximize{ S, R : grant(R), score(R, S)}.

#show grant/1.