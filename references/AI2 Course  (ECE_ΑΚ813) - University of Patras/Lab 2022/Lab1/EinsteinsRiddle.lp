% -  -  -  -  -  -  -  -  -  -  EINSTEIN'S RIDDLE  -  -  -  -  -  -  -  -  -  - %

%*
Conditions:
There are 5 houses painted five different colors.
In each house lives a person with a different nationality.
These five owners drink a certain type of beverage, smoke a certain brand of cigar, and keep a certain pet.
No owners have the same pet, smoke the same brand of cigar, or drink the same beverage.

Details:
1)The Brit lives in the red house
2)The Swede keeps dogs as pets
3)The Dane drinks tea
4)The green house is on the left of the white house
5)The person who smokes Pall Malls rears birds
6)The owner of the yellow house smokes Dunhill
7)The green house’s owner drinks coffee
8)The man living in the center house drinks milk
9)The Norwegian lives in the first (leftmost) house
10)The man who smokes Blends lives next to the one who keeps cats
11)The man who keeps horses lives next to the man who smokes Dunhill
12)The owner who smokes BlueMaster drinks beer
13)The German smokes Princes
14)The Norwegian lives next to the blue house
15)The man who smokes Blends has a neighbor who drinks water

Question:
Now to solve, tell me who owns the fish?
*%

% FACTS
% There are 5 houses.
house(1..5).
%* The houses are painted with one of the five colors: 
red, green, blue, white, yellow *%
col(r; g; b; w; y).
%*In each house lives a person with one of the following nationalities:
Brit, Swede, German, Norwegian, Dane*%
man(b; s; g; n; d).
%* These five owners drink one of the following beverages:
coffee, water, milk, beer, tea*% 
drink(c; w; m; b; t).
%* These five owners smoke one of the following brands:
Princes, BlueMaster, Pall Malls, Dunhill, Blends*% 
cigarretes(p; bm; pm; d; b).
%* These five owners has one of the following pets:
cats, dogs, birds, fish, horses*% 
pet(c; d; b; f; h).

% RULES
%* Formulate the relations:
lives, hasColor, smokes, drinks and hasPet *% 
1 {lives(X, Y) : house(Y)} 1 :-  man(X).
1 {hasColor(X, Y) : col(Y)} 1 :-  house(X).
1 {smokes(X, Y) : cigarretes(Y)} 1 :-  man(X).
1 {drinks(X, Y) : drink(Y)} 1 :-  man(X).
1 {hasPet(X, Y) : pet(Y)} 1 :-  man(X).

% INTEGRITY CONSTRAINTS
%* Formulate that no two owners live on the same house, smoke the same brand of cigar, drink the same beverage 
or have the same pet and also that each house has a unique color.*%
:-  lives(X, Y), lives(Z, Y), X != Z.
:-  smokes(X, Y), smokes(Z, Y), X != Z.
:-  drinks(X, Y), drinks(Z, Y), X != Z.
:-  hasPet(X, Y), hasPet(Z, Y), X != Z.
:-  hasColor(X, Y), hasColor(Z, Y), X != Z.
%* The Brit lives in the red house *%
:-  lives(b, Y), hasColor(Y, Z), Z != r.
%* The Swede keeps dogs as pets *%
:-  hasPet(s, Y), Y != d.
%* The Dane drinks tea *%
:-  drinks(d, Y), Y != t.
%* The green house is on the left of the white house *%
:-  hasColor(X, g), hasColor(Y, w), X != Y - 1.
%* The green house’s owner drinks coffee *%
:-  lives(X, Y), hasColor(Y, g), drinks(X, Z), Z != c.
%* The person who smokes Pall Malls rears birds *%
:-  smokes(X, pm), hasPet(X, Y), Y != b.
%* The owner of the yellow house smokes Dunhill *%
:-  lives(X, Y), hasColor(Y, y), smokes(X, Z), Z != d.
%* The man living in the center house drinks milk *%
:-  drinks(X, m), lives(X, Y), Y != 3.
%* The Norwegian lives in the first (leftmost) house *%
:-  lives(n, Y), Y != 1.
%* The man who smokes Blends lives next to the one who keeps cats *%
:-  smokes(X, b), lives(X, Y), hasPet(Z, c), lives(Z, W), |Y - W| != 1.
%* The man who keeps horses lives next to the man who smokes Dunhill *%
:-  smokes(X, d), lives(X, Y), hasPet(Z, h), lives(Z, W), |Y - W| != 1.
%* The owner who smokes BlueMaster drinks beer *%
:-  smokes(X, bm), drinks(X, Y), Y != b.
%* The German smokes Princes *%
:-  smokes(g, Y), Y != p.
%* The Norwegian lives next to the blue house *%
:-  lives(n, Y), hasColor(Z, b), |Y - Z| != 1.
%* The man who smokes Blends has a neighbor who drinks water *%
:-  smokes(X, b), lives(X, Y), drinks(Z, w), lives(Z, W), |Y - W| != 1.

% OUTPUT
#show lives/2.
#show hasColor/2.
#show smokes/2.
#show drinks/2.
#show hasPet/2.

%* 
Results:

  Brit     Swede     German     Norwegian    Dane      
lives(b, 3)  |lives(s, 5)  | lives(g, 4)  | lives(n, 1)  | lives(d, 2)
hasColor(3, r) |hasColor(5, w) | hasColor(4, g) | hasColor(1, y) | hasColor(2, b)
smokes(b, pm) |smokes(s, bm) | smokes(g, p)  | smokes(n, d)  | smokes(d, b)
drinks(b, m)  |drinks(s, b)  | drinks(g, c)  | drinks(n, w)  | drinks(d, t)
hasPet(b, b)  |hasPet(s, d)  | hasPet(g, f)  | hasPet(n, c)  | hasPet(d, h)

Answer: The Norwegian owns the fish
*%