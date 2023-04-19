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
granted(R) :- status(R, S), S = 1.
%% WAITING RULE
1 {best(R) : request(R)} 1 :- patient_id(R,P), score(R,S), not request(X), patient_id(X,P), score(X,SX), X != R, S < SX.
% SET CREATION
0 {grant(R)} 1 :- request(R), granted(R), not best(R).
0 {grant(R)} 1 :- request(R), not granted(R).

% INTEGRITY CONSTRAINTS
%% A timeslot can be appointed only to one patient
:- grant(R1), timeslot_id(R1, T), grant(R2), timeslot_id(R2, T), R1 != R2.
%% Only one timeslot can be appointed to a patient
:- grant(R1), patient_id(R1, P), grant(R2), patient_id(R2, P), R1 != R2.
%% A patient cannot receive a timeslot corresponding to a lower request score than the one he or she already owns
:- granted(R1), patient_id(R1, P), score(R1, S1), grant(R2), patient_id(R2,P), score(R2, S2), R1 != R2, S2 < S1.

% OPTIMIZATION
#maximize{ S, R : grant(R), score(R, S)}.

#show grant/1.