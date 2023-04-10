%---------- 4 TOWERS AND 4 BISHOPS ----------%

% RULES
1 {tower(I, X, Y) : X = 1..8, Y = 1..8} 1 :- I = 1..4.
1 {bishop(I, X, Y) : X = 1..8, Y = 1..8} 1 :- I = 1..4.
occupied(Y) :- tower(_, _, Y), Y = 1..8.
occupied(Y) :- bishop(_, _, Y), Y = 1..8.

% INTEGRITY CONSTRAINTS
:- tower(I, X, Y1), tower(J, X, Y2), I != J.
:- tower(I, X1, Y), tower(J, X2, Y), I != J.
:- bishop(I, X1, Y1), bishop(J, X2, Y2), X1 + X2 == Y1 + Y2, I!=J.
:- bishop(I, X1, Y1), bishop(J, X2, Y2), X1 + Y1 == X2 + Y2, I!=J.
:- tower(I, X, Y1), bishop(J, X, Y2).
:- tower(I, X1, Y), bishop(J, X2, Y).
:- tower(I, X1, Y1), bishop(J, X2, Y2), X1 + Y1 == X2 + Y2.
:- tower(I, X1, Y1), bishop(J, X2, Y2), X1 + Y2 == X2 + Y1.
:- not occupied(Y), Y = 1..8.

% OUTPUT
#show bishop/3.
#show tower/3.

