% %EXAMPLE

% RULES
%% GRANTED RULE
granted(R) :- status(R, S), S = 1.
%% APPOINTED RULE
appointed(P) :- patient_id(R, P), granted(R).
%% BEST RULE
best(R) :- patient_id(R, P), score(R, S), 0 {patient_id(X, P) : score(X, SX), S < SX, R != X} 0.
%% ONLY OPTION RULE
onlyOption(R) :- patient_id(R, P), 0 {patient_id(X, P) : R != X} 0.
%% SINGLE REQUEST RULES
singleRequest(R) :- timeslot_id(R, T), 0 {timeslot_id(X, T) : R != X} 0.
bestSingleRequest(R) :- patient_id(R,P), score(R,S), timeslot_id(R, T), singleRequest(R), 0 {patient_id(X,P) : score(X,SX), singleRequest(X), SX > S, R != X} 0.

% SET CREATION
0 {grant(R)} 1 :- granted(R), not best(R), not bestSingleRequest(R).
0 {grant(R)} 1 :- patient_id(R, P), score(R, S), not granted(R), patient_id(X,P), score(X, SX), granted(X), S > SX, R != X.
0 {grant(R)} 1 :- patient_id(R,P), not appointed(P).

%% FRAME AXIOM
claimed(R) :- granted(R), best(R).
claimed(R) :- granted(R), not best(R), bestSingleRequest(R).


% INTEGRITY CONSTRAINTS
%% A timeslot can be appointed only to one patient
:- grant(R1), timeslot_id(R1, T), grant(R2), timeslot_id(R2, T), R1 != R2.
%% Only one timeslot can be appointed to a patient
:- grant(R1), patient_id(R1, P), grant(R2), patient_id(R2, P), R1 != R2.
% %% If a request is claimed all the other requests for the corresponding timeslot can't be granted
:- grant(R), timeslot_id(R,T), claimed(X), timeslot_id(X,T), R != X.
% %% If a patient has only one option it can't be ignored for the maximization of the common benefit
:- grant(R), timeslot_id(R,T), score(R,S), not granted(R), not onlyOption(R), timeslot_id(X,T), score(X,SX), onlyOption(X), S < SX, R != X.
% %% If someone had an appointment before they also have to have an appointment after the rescheduling
:- granted(R), patient_id(R, P), not claimed(R), 0 { grant(X) : patient_id(X, P) } 0. 
% %% Only the best single request can be granted
:- grant(R), singleRequest(R), not bestSingleRequest(R).
% %% If a patient wants a timeslot and is the only one to request it, the request will be granted if it is better than the already granted request if it exists.
:- patient_id(R, P), score(R, S), bestSingleRequest(R), patient_id(X, P), score(X, SX), granted(X), R != X, S > SX, patient_id(Y, P), Y != R, grant(Y), not grant(R).
:- patient_id(R, P), bestSingleRequest(R), not appointed(P), patient_id(Y, P), Y != R, grant(Y), not grant(R).

% OPTIMIZATION
#maximize{ S, R : grant(R), score(R, S) }.

% #show grant.
% #show claimed.