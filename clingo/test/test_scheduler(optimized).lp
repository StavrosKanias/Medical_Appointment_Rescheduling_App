doctor("21088977182").
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
date(1,"2023-04-17").
date(2,"2023-04-18").
date(3,"2023-04-20").
date(4,"2023-04-21").
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
%% OWNERSHIP RULE
owns(P, T, R) :- patient(P), request(R), timeslot(T), timeslot_id(R, T), patient_id(R, P), status(R, S), S = 1.
%% BEST CHOICE RULE
best(P,R) :- patient(P), request(R), patient_id(R,P), score(R,SR), request(X), patient_id(X,P), score(X,SX), SR > SX.

% SET CREATION
0 {assign(P, T, R) : patient(P)} 1 :- timeslot(T), request(R), patient_id(R, P), timeslot_id(R, T), owns(P,_,X), not best(P,X).
0 {assign(P, T, R) : patient(P)} 1 :- timeslot(T), request(R), patient_id(R, P), timeslot_id(R, T), timeslot(X), not owns(P,X,_).

% INTEGRITY CONSTRAINTS
%% The timeslot and the doctor has to be available
:- assign(_, T, _), not timeslot_available(T). 
%% A timeslot can be appointed only to one patient
:- patient(A), patient(B), assign(A,T,_), assign(B,T,_), A != B.
%% Only one timeslot can be appointed to a patient
:- timeslot(A), timeslot(B), assign(P,A,_), assign(P,B,_), A != B.
%% A patient cannot receive a timeslot corresponding to a lower request score than the one he or she already owns
:- owns(P, TA, A), score(A, SA), assign(P, TB, B), score(B, SB), SA > SB.

% OPTIMIZATION
#maximize{ S, R, T, P : assign(P, T ,R), score(R, S)}.

#show assign/3.