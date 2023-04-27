request(2,"21088977184",1,87,0).
request(3,"21088977188",1,86,0).
request(4,"21088977184",2,72,1).
request(5,"21088977185",2,60,0).
request(6,"21088977186",2,55,0).
request(7,"21088977185",3,45,1).
request(8,"21088977186",3,40,0).
request(9,"21088977188",4,71,1).
request(10,"21088977187",4,70,0).


% RULES
%% GRANTED RULE
granted(R) :- request(R,P,T,S,ST), ST = 1.
%% WAITING RULE
appointed(P) :- not {request(R,P,T,S,SR)}, granted(R).
%% BETTER RULE
better(R,X) :- request(R,P,T,S,ST),request(RX,PX,TX,SX,STX), R != X, S > SX. 
% SET CREATION
{grant(R)} 1 :- granted(R).
{grant(R)} 1 :- request(R,P,T,S,ST), not granted(R), request(X,P,TX,SX,STX), granted(X), better(R,X).
{grant(R)} 1 :- request(R,P,T,S,ST), not appointed(P).
% INTEGRITY CONSTRAINTS
%% A timeslot can be appointed only to one patient
:- request(R1,P,T,S,ST), grant(R1), request(RX,PX,TX,SX,STX), grant(R2), timeslot_id(R2, T), R1 != R2.
%% Only one timeslot can be appointed to a patient
:- grant(R1), patient_id(R1, P), grant(R2), patient_id(R2, P), R1 != R2.

% OPTIMIZATION
#maximize{ S, R : grant(R), score(R, S)}.

#show grant/1.