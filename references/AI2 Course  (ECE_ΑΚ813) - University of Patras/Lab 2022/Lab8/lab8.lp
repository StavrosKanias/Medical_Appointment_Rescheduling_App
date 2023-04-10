%---------- FARMER CROSSES RIVER ----------%
                
% STEPS
#const n=3.

% FACTS
object(wolf; sheep; hay).
position(wolf, left, 0).
position(sheep, left, 0).
position(hay, left, 0).
position(boat, left, 0).

% GOAL
goal :- position(wolf, right, n), position(sheep, right, n), position(hay, right, n), position(boat, right, n).

% RULE
1 { move(X,T) : object(X) } 2 :- T = 0..n-1.

% EFFECT AXIOM
position(X, right, T+1) :- position(X, left, T), move(X, T), position(boat, left, T), T < n.
position(X, left, T+1) :- position(X, right, T), move(X, T), position(boat, right, T), T < n.
position(boat, left, T+1) :- position(boat, right, T), T < n.
position(boat, right, T+1) :- position(boat, left, T), T < n.

% FRAME AXIOM
position(X, Y, T+1) :- position(X, Y, T), not move(X, T), object(X), T < n.

% INTEGRITY CONSTRAINTS
:- not goal.
:- position(wolf, X, T), position(sheep, X, T), position(boat, Z, T), X != Z.
:- position(sheep, X, T), position(hay, X, T), position(boat, Z, T), X != Z.

% OUTPUT
#show move/2.
