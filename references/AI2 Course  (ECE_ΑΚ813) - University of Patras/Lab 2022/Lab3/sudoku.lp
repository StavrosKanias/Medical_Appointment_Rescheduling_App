%---------- SUDOKU SOLVER ----------%

% BOX
#const n = 3.

% FACTS
% Puzzle description (input) -> fixed(x_pos,  y_pos,  value)
fixed(1, 1, 5). fixed(1, 2, 3). fixed(1, 5, 7).
fixed(2, 1, 6). fixed(2, 4, 1). fixed(2, 5, 9). fixed(2, 6, 5).
fixed(3, 2, 9). fixed(3, 3, 8). fixed(3, 8, 6).
fixed(4, 1, 8). fixed(4, 5, 6). fixed(4, 9, 3).
fixed(5, 1, 4). fixed(5, 4, 8). fixed(5, 6, 3). fixed(5, 9, 1).
fixed(6, 1, 7). fixed(6, 5, 2). fixed(6, 9, 6).
fixed(7, 2, 6). fixed(7, 7, 2). fixed(7, 8, 8).
fixed(8, 4, 4). fixed(8, 5, 1). fixed(8, 6, 9). fixed(8, 9, 5).
fixed(9, 5, 8). fixed(9, 8, 7). fixed(9, 9, 9).

% RULES
1 {value(X, Y, I) : I = 1..n * n}  1 :- X = 1..n * n , Y = 1..n * n . 
samebox(X1, Y1, X2, Y2) :- X1 = 1..n * n, X2 = 1..n * n, Y1 = 1..n * n, Y2 = 1..n * n, (X1 - 1) / n  =  (X2 - 1) / n, (Y1 - 1) / n  =  (Y2 - 1) / n. 

% INTEGRITY CONSTRAINTS
:- fixed(X, Y, I1), value(X, Y, I2), I1 != I2.
:- value(X, Y1, I), value(X, Y2, I), Y1 != Y2.
:- value(X1, Y, I), value(X2, Y, I), X1 != X2.
:- samebox(X1, Y1, X2, Y2), value(X1, Y1, I), value(X2, Y2, I), X1 != X2, Y1 != Y2.

% OUTPUT
#show fixed/3.