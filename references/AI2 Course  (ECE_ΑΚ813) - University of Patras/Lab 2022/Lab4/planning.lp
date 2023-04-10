%---------- THE TOWERS OF HANOI ----------%

% DISKS
#const m = 4.

% STEPS
#const n = 15.

% FACTS
peg(a; b; c).
disk(1..m).
at(1..m, a, 0).
goal(1..m, c).

% RULE
1 {move(D, P, T) : disk(D), peg(P)} 1 :- T = 0..n-1.

% EFFECT AXIOM
at(D, P, T + 1) :- move(D, P, T), T < n.

% FRAME AXIOM
at(D, P, T + 1) :- at(D, P, T), not move(D, _, T), T < n.

% INTEGRITY CONSTRAINTS
:- goal(D, P), not at(D, P, n).
:- at(D2, P2, T), move(D1, P2, T), D1 > D2, T < n.
:- at(D, P, T), move(D, P, T), T < n.
:- at(D1, P, T), at(D2, P, T), move(D1, _, T), D1 > D2, T < n.

% OUTPUT
#show move/3.