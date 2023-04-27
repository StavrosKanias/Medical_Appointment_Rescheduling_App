% RULES
%% GRANTED RULE
granted(R) :- request(R,_,_,_,ST), ST = 1.
%% WAITING RULE
appointed(P) :- request(R,P,_,_,_), granted(R).
% SET CREATION
0 {grant(R,S)} 1 :- request(R,P,_,S,_), granted(R).
0 {grant(R,S)} 1 :- request(R,P,_,S,_), not granted(R), request(X,P,_,SX,_), granted(X), R != X, S > SX.
0 {grant(R,S)} 1 :- request(R,P,_,S,_), not appointed(P).
% INTEGRITY CONSTRAINTS
%% A timeslot can be appointed only to one patient
:- request(R,_,T,S,_), grant(R,S), request(X,_,T,SX,_), grant(X,SX), R != X.
%% Only one timeslot can be appointed to a patient
:- request(R,P,_,S,_), grant(R,S), request(X,P,_,SX,_), grant(X,SX), R != X.

% OPTIMIZATION
#maximize{ S, R : grant(R,S)}.