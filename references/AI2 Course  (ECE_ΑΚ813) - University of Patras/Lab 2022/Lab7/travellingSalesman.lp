%---------- TRAVELLING SALESMAN ----------%

% TRAVELLS
#const n = 4.

% FACTS
city(a). city(b). city(c). city(d).
road(a,b,10). road(b,c,20). road(c,d,25). road(d,a,40).
road(b,d,30). road(d,c,25). road(c,a,35).
at(a, 0).

% RULES
1 {go(X, T) : city(X)} 1 :- T = 0..n-2.
visited(C) :- at(C, T), T = 0..n-1.
go(a, n-1).

% MINIMIZATION
#minimize {D, X, Y, T : at(X, T), go(Y, T), road(X, Y, D)}.

% EFFECT AXIOM
at(Y, T+1) :- at(X, T), road(X, Y, _), go(Y, T).

% INTEGRITY CONSTRAINTS
:- city(C), not visited(C).

% OUTPUT
#show go/2.