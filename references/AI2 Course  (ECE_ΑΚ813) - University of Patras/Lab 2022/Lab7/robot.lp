%---------- ROBOT LABYRINTH ----------%

% GRID
#const rows = 4.
#const cols = 4.
#const n = rows * cols.

% FACTS
blocked(2, 1).
blocked(2, 2).
blocked(3, 3).
direction(up; down; left; right).
robot(1, 1, 0).

% GOAL
goal:-robot(3, 2, n).

% RULES
0 {move(D, T) : direction(D)} 1 :- T = 0..n-1.
action(T) :- move(_, T).
lastAction(T) :- T = #max {X : action(X)}.

% MINIMIZATION
#minimize {T : lastAction(T)}.

% EFFECT AXIOMS
robot(R+1, C, T+1) :- robot(R,C,T), move(down, T), R < rows,  T < n.
robot(R-1, C, T+1) :- robot(R,C,T), move(up, T), R > 1, T < n.
robot(R, C+1, T+1) :- robot(R,C,T), move(right, T), C < cols, T < n.
robot(R, C-1, T+1) :- robot(R,C,T), move(left, T), C > 1, T < n.

% FRAME AXIOM
robot(R, C, T+1) :- robot(R, C, T), not action(T), T < n.

% INTEGRITY CONTSTRAINTS
:- robot(R, C, T), blocked(R, C).
:- not goal.

% OUTPUT
#show move/2.