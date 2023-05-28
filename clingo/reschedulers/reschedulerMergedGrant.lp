% RULES
%% GRANTED RULE
granted(R) :- request(R,P,T,S,ST), ST = 1.
%% APPOINTED RULE
appointed(P) :- request(R,P,T,S,ST), granted(R).
%% BEST RULE
best(R) :- request(R,P,T,S,ST), 0 {request(X,P,_,SX,_) : S < SX, R != X} 0.
%% ONLY OPTION RULE
onlyOption(R) :- request(R,P,T,S,ST), 0 {request(X,P,_,_,_) :  R != X} 0.
%% SINGLE REQUEST RULES
singleRequest(R) :- request(R,P,T,S,ST), 0 {request(X,_,T,_,_) :  R != X} 0.
bestSingleRequest(R) :- request(R,P,T,S,ST), singleRequest(R), 0 {request(X,P,_,SX,_): singleRequest(X), SX > S, R != X} 0.

% SET CREATION
0 {grant(R,S)} 1 :- request(R,P,T,S,ST), granted(R), not best(R), not bestSingleRequest(R).
0 {grant(R,S)} 1 :- request(R,P,T,S,ST), not granted(R), request(X,P,_,SX,_), granted(X), S > SX, R != X.
0 {grant(R,S)} 1 :- request(R,P,T,S,ST), not appointed(P).

%% FRAME AXIOM
claimed(R) :- granted(R), best(R).
claimed(R) :- granted(R), not best(R), bestSingleRequest(R).


% INTEGRITY CONSTRAINTS
%% A timeslot can be appointed only to one patient
:- request(R,P,T,S,ST), grant(R,_), request(X,_,T,_,_), grant(X,_), R != X.
%% Only one timeslot can be appointed to a patient
:- request(R,P,T,S,ST), grant(R,_), request(X,P,_,_,_), grant(X,_), R != X.
% %% If a request is claimed all the other requests for the corresponding timeslot can't be granted
:- request(R,P,T,S,ST), grant(R,_), request(X,_,T,_,_), claimed(X), R != X.
% %% If a request has a lower score than someone's only option it won't be considered in the optimizatioin process
:- request(R,P,T,S,ST), grant(R,_), not granted(R), not onlyOption(R), request(X,_,T,SX,_), onlyOption(X), S < SX, R != X.
% %% If someone had an appointment before they also have to have an appointment after the rescheduling
:- request(R,P,T,S,ST), granted(R), not claimed(R), 0 {grant(X,_) : request(X,P,_,_,_)} 0.
%% Only the best single request can be granted
:- grant(R,_), singleRequest(R), not bestSingleRequest(R).
% %% If a patient wants a timeslot and is the only one to request it, the request will be granted if it is better than the already granted request if it exists.
:- request(R,P,T,S,ST), bestSingleRequest(R), request(X,P,_,SX,_), granted(X), R != X, S > SX, request(Y,P,_,_,_), Y != R, grant(Y,_), not grant(R,_).
:- request(R,P,T,S,ST), bestSingleRequest(R), not appointed(P), request(Y,P,_,_,_), Y != R, grant(Y,_), not grant(R,_).

% OPTIMIZATION
#maximize{ S, R : grant(R, S)}.

% #show grant.
% #show claimed.