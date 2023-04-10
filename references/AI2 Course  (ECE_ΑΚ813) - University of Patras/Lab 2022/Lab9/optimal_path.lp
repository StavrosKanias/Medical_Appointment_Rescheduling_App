%---------- OPTIMAL PATH ----------%

% NODES
#const start = 1. 
#const end = 5.

% FACTS
edge(1,2,3).
edge(2,3,2).
edge(3,4,3).
edge(4,1,7).
edge(1,4,7).
edge(4,3,3).
edge(3,5,10).
edge(4,5,4).
edge(5,2,6).

% RULES 
{inPath(X,Y) : edge(X,Y,_)}.	
reachable(X,Y) :- inPath(X,Y).	
reachable(X,Y) :- reachable(X,Z),	inPath(Z,Y).	

% OPTIMIZATION
#minimize{Z,X,Y: inPath(X,Y), edge(X,Y,Z)}.	

% Integrity constraints
:- not reachable(start,end).

% OUTPUT
#show inPath/2.