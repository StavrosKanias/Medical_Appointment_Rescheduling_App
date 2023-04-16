% SET CREATION
0 {assign(T, P) : patient(P)} 1 :- timeslot(T).
% OWNERSHIP RULE
owns(P, T, R) :- patient(P), request(R), timeslot(T), timeslot_id(R, T), patient_id(R, P), status(R, S), S = 1.

% INTEGRITY CONSTRAINTS
%% The timeslot and the doctor has to be available
:- request(R), timeslot_id(R,T), doctor_id(T,D), not timeslot_available(T), not doctor_available(D). 
%% A timeslot can be appointed only to one patient
:- timeslot(A), timeslot(B), assign(X,A), assign(Y,B), X != Y.
%% A patient cannot receive a timeslot corresponding to a lower request score than the one he or she already owns
:- request(A), score(A,SA), timeslot_id(A,TA), owns(P,A,TA), request(B), score(B,SB), timeslot_id(B,TB), assign(TB,P), SA < SB.

% OPTIMIZATION
#maximize{ S, R : score(R, S) }.