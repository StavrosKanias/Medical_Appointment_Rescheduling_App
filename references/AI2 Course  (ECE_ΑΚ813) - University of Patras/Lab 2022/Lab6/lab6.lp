%---------- MOVING ANIMALS FROM ONE CAGE TO ANOTHER ----------%

% STEPS
#const n = 3.

% FACTS
in(lion, a, 0).
in(tiger, b, 0).
in(sheep, c, 0).
in(cow, c, 0).
in(horse, d, 0).
carnivorous(lion; tiger).
hernivorous(sheep; cow; horse).
cage(a; b; c; d).

% RULES
animal(X) :- carnivorous(X).
animal(X) :- hernivorous(X).

% GOAL
goal :- in(tiger, c, n).
1 {move(A, C, T) : animal(A), cage(C)} 1 :- T = 0..n-1.

% EFFECT AXIOM
in(A, C, T + 1) :- move(A, C, T), T < n.

% FRAME AXIOM
in(A, C, T + 1) :- in(A, C, T), not move(A, C, T), T < n.

% INTEGRITY CONSTRAINTS
:- not goal.
:- in(A, C, T), in(B, C, T), carnivorous(A), hernivorous(B), T < n. 

% OUTPUT
#show move/3.