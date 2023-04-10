%---------- PIE TOWER ----------%

% PIES
#const m = 4.	

% STEPS
#const n = 3.	

% FACTS
on(1, 5, 0). on(4, 1, 0). on(2, 4, 0). on(3, 2, 0).	
pie(1..m).
intermediateState(0..n-1).
state(0..n).

% RULE
1 { flip(X, T) : pie(X) } 1 :- intermediateState(T).	

% GOAL	
goal(X, Y) :- pie(X), pie(Y), Y = X + 1.	

% AUXILARY PREDICATES	
above(X, Y, T) :- on(X, Y, T), state(T).	
above(X, Y, T) :- on(X, Z, T), above(Z, Y, T), state(T).	
top(X, T) :- pie(X), state(T), not on(_, X, T).

% EFFECT AXIOMS	
on(Y, X, T + 1) :- flip(Z, T), above(X, Z, T), on(X, Y, T), intermediateState(T).	
on(X, Y, T + 1) :- on(Z, Y, T), top(X, T), flip(Z, T), intermediateState(T).	

% FRAME AXIOM	
on(X, Y, T + 1) :- on(X, Y, T), flip(Z, T), not above(X, Z, T), intermediateState(T).	

% INTEGRITY CONSTRAINT
:- goal(X, Y), not on(X, Y, n).

% OUTPUT
#show flip/2.	