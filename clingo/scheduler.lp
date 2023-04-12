% RULES
{ assign(T, P) : timeslot(T) } :- patient(P).
owns(P, T) :- patient(P), request(R), timeslot(T), timeslot_id(R, T), status(R, S), S = 1.
% OPTIMIZATION
#maximize{S,R,T:score(R,S),owns(_,T)}.
% INTEGRITY CONSTRAINTS
%% The timeslot and the doctor has to be available
:- request(R), timeslot_id(R,T), doctor_id(T,D), not timeslot_available(T), not doctor_available(D). 