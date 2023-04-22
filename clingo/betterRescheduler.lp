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