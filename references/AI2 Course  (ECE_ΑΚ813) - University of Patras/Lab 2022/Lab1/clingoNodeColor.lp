%---------- NODE COLORING ----------%

% FACTS
node(1..6).
edge(1, 2), edge(2, 4), edge(3, 1).
edge(4, 1), edge(5, 3), edge(6, 2).
edge(1, 3), edge(2, 5), edge(3, 4).
edge(4, 2), edge(5, 4), edge(6, 3).
edge(1, 4), edge(2, 6), edge(3, 5).
edge(5, 6), edge(6, 5).
col(r). col(g). col(b).

% RULE
1 {color(X, Z) : col(Z)} 1 :- node(X).

% INTEGRITY CONSTRAINT 
:- color(X, Z), color(Y, Z), edge(X, Y).

% OUTPUT
#show color/2.
