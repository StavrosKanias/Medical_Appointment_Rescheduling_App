%---------- COURSE SELECION ----------%

% FACTS
student(john). student(mary). student(helen).
course(db). course(ai1). course(ai2). course(thesis). course(java).
credits(db,6). credits(ai1,5). credits(ai2,5). credits(thesis,10). credits(java,4).

% RULE
{select(X, Y) : course(Y)} :- student(X).

% INTEGRITY CONSTRAINTS
:- student(X), #sum {Z, Y : select(X, Y), credits(Y, Z)} < 10.
:- #count {X : select(X, thesis)} > 1.

% OUTUPUT
#show select/2.