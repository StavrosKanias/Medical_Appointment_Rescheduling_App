% SET GENERATION
0 {assign(P, T, R) : patient(P)} 1 :- timeslot(T), request(R), patient_id(R, P), timeslot_id(R, T).

% OWNERSHIP RULE
owns(P, T, R) :- patient(P), request(R), timeslot(T), timeslot_id(R, T), patient_id(R, P), status(R, S), S = 1.

% INTEGRITY CONSTRAINTS
%% The timeslot and the doctor has to be available
:- assign(P, T, R), not timeslot_available(T). 
%% A timeslot can be appointed only to one patient
:- patient(A), patient(B), assign(A,T,_), assign(B,T,_), A != B.
%% Only one timeslot can be appointed to a patient
:- timeslot(A), timeslot(B), assign(P,A,_), assign(P,B,_), A != B.
%% A patient cannot receive a timeslot corresponding to a lower request score than the one he or she already owns
:- owns(P, TA, A), score(A, SA), assign(P, TB, B), score(B, SB), SA > SB.

% OPTIMIZATION
#maximize{ S, R, T, P : assign(P, T ,R), score(R, S)}.