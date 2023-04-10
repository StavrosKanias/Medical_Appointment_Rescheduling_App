%---------- ROBOT LABYRINTH ----------%
#const rows=4. 
#const cols=4. 
#const n=rows*cols. 
% ΕΜΠΌΔΙΑ - ΚΑΤΕΥΘΎΝΣΕΙΣ. 
blocked(2,1). blocked(2,2). blocked(3,3). 
direction(up;down;left;right). 
% ΑΡΧΙΚΉ ΚΑΤΆΣΤΑΣΗ
robot(1,1,0). 
% ΣΤΟΧΟΣ
goal :- robot(3,2,n). 
:- not goal. 
% ΕΠΙΛΟΓΉ ΚΙΝΉΣΕΩΝ 
0 { move(D,T): direction(D) } 1 :- T = 0..n-1. 
% ΕΛΑΧΙΣΤΟΠΟΊΗΣΗ ΚΙΝΉΣΕΩΝ 
action(T) :- move(_,T). 
lastAction(T) :- T = #max{ X : action(X) }. 
#minimize { T: lastAction(T) }. 
% EFFECT AXIOMS 
robot(X-1,Y,T+1) :- robot(X,Y,T), X>1, not blocked(X-1,Y), move(up,T). 
robot(X+1,Y,T+1) :- robot(X,Y,T), X<rows, not blocked(X+1,Y), move(down,T). 
robot(X,Y-1,T+1) :- robot(X,Y,T), Y>1, not blocked(X,Y-1), move(left,T). 
robot(X,Y+1,T+1) :- robot(X,Y,T), Y<cols, not blocked(X,Y+1), move(right,T). 
% FRAME AXIOM 
robot(X,Y,T+1) :- robot(X,Y,T), not move(_,T), T=0..n-1. 
#show move/2.