SELECT title FROM Book;
SELECT title, authorId FROM Book;
SELECT * FROM Book;
SELECT Book.title FROM Book;
SELECT Book.title AS BNAME FROM Book;
SELECT Book.title AS BNAME, b.authorId FROM Book AS b;
SELECT Book.title AS BNAME, b.authorId AS BID FROM Book AS b;
SELECT Book.title AS BNAME, Author.authorId AS AID FROM Book, Author;
SELECT Book.title AS BNAME, Author.authorId AS AID FROM Book, Author WHERE Book.authorId = Author.authorId;
SELECT * FROM Book, Author WHERE Book.authorId = Author.authorId;
SELECT * FROM Book AS B WHERE Book.authorId;
SELECT * FROM Book AS B WHERE Book.authorId < 5;
SELECT * FROM Book AS B WHERE Book.authorId <> 5;
SELECT * FROM Book AS B WHERE Book.authorId > 5;
SELECT * FROM Book AS B WHERE Book.authorId < 5 AND Book.authorId;
SELECT * FROM Book AS B WHERE Book.title < 'Hello';
SELECT * FROM Book AS B WHERE Book.title > 'Hello';
SELECT * FROM Book AS B WHERE Book.title = 'Hello';
SELECT * FROM Book AS B WHERE Book.title = 'Bible';
SELECT Book.* FROM Book, Author WHERE Book.authorId = Author.authorId;