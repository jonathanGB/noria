DROP TABLE IF EXISTS Messages;
DROP TABLE IF EXISTS ListMembers;
DROP TABLE IF EXISTS Lists;
DROP TABLE IF EXISTS Tweets;
DROP TABLE IF EXISTS Follows;
DROP TABLE IF EXISTS Users;

CREATE TABLE Users (
  id iNT PRIMARY KEY AUTO_INCREMENT,
  handle VARCHAR(15) NOT NULL UNIQUE,
  name VARCHAR(50) NOT NULL,
  email VARCHAR(320) NOT NULL UNIQUE,
  password VARCHAR(100) NOT NULL,
  is_private BOOLEAN NOT NULL DEFAULT FALSE,
  is_open_dms BOOLEAN NOT NULL DEFAULT FALSE,
  is_displaying_sensitive_content BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  bio VARCHAR(160),
  location VARCHAR(30),
  website VARCHAR(100),
  month_birthday VARCHAR(9),
  day_birthday INT,
  year_birthday INT,
  restriction_birth_daymonth INT NOT NULL DEFAULT 0,
  restriction_birth_year INT NOT NULL DEFAULT 0,
  CONSTRAINT month CHECK (month_birthday IN 
    ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')),
  CONSTRAINT day CHECK(day_birthday >= 1 AND day_birthday <= 31),
  CONSTRAINT year CHECK(year_birthday >= 1900 AND year_birthday <= 2020),
  CONSTRAINT restriction_birth CHECK 
    (restriction_birth_daymonth >= 0 AND restriction_birth_daymonth < 5 AND
     restriction_birth_year >= 0 AND restriction_birth_year < 5)
);

-- Regarding restrictions_birth
-- 0: private
-- 1: you follow each other
-- 2: you follow them
-- 3: they follow you
-- 4: public

-- Populate `Users`
INSERT INTO Users (handle, name, email, bio, location, website, day_birthday, month_birthday, year_birthday, is_open_dms, password) VALUES ('user1', 'Jonathan', 'j@me.com', '[insert bio here]', 'Cambridge, MA', 'jonathangb.com', 1, 'January', 1900, true, '12345');
INSERT INTO Users (handle, name, email, bio, password) VALUES ('user2', 'François', 'f@me.com', "This is Francois' account.", 'abc');
INSERT INTO Users (handle, name, email, password) VALUES ('user3', 'Gaëtan', 'g@me.com', 'strong password');
INSERT INTO Users (handle, name, email, is_private, password) VALUES ('private_1', 'George', 'whodis@me.com', true, 'password');


CREATE TABLE Follows (
  user_id INT NOT NULL,
  followed_id iNT NOT NULL,
  PRIMARY KEY(user_id, followed_id),
  FOREIGN KEY (user_id) REFERENCES Users(id),
  FOREIGN KEY (followed_id) REFERENCES Users(id),
  CONSTRAINT no_follow_oneself CHECK (user_id <> followed_id)
);

-- Populate `Follows`
INSERT INTO Follows (user_id, followed_id) VALUES (1, 2);
INSERT INTO Follows (user_id, followed_id) VALUES (1, 3);
INSERT INTO Follows (user_id, followed_id) VALUES (1, 4);
INSERT INTO Follows (user_id, followed_id) VALUES (2, 3);
INSERT INTO Follows (user_id, followed_id) VALUES (3, 2);

CREATE TABLE Tweets (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT NOT NULL,
  content VARCHAR(280),
  retweet_id INT,
  reply_id INT,
  is_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
  timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES Users(id),
  CONSTRAINT no_retweet_and_reply
    CHECK (retweet_id IS NULL OR reply_id IS NULL),
  CONSTRAINT no_empty_content_if_not_retweet
    CHECK (content IS NOT NULL OR retweet_id IS NOT NULL)
);

-- Populate `Tweets`
INSERT INTO Tweets (user_id, content) VALUES (1, "This is my very original tweet.");
INSERT INTO Tweets (user_id, retweet_id) VALUES (2, 1);
INSERT INTO Tweets (user_id, reply_id, content) VALUES (2, 2, "reply to my own retweet");
INSERT INTO Tweets (user_id, content) VALUES (3, "#3 also tweets...");
INSERT INTO Tweets (user_id, content, is_sensitive) VALUES (4, "PRIVATE TWEET", true);


CREATE TABLE Messages (
  sender_id INT NOT NULL,
  sendee_id INT NOT NULL,
  content VARCHAR(500) NOT NULL,
  timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (sender_id) REFERENCES Users(id),
  FOREIGN KEY (sendee_id) REFERENCES Users(id),
  CONSTRAINT no_message_oneself CHECK (sender_id <> sendee_id)
  -- Should also check that message is to an allowed sendee,
  -- i.e. sendee has `is_open_dms` or sendee follows sender.
);

-- Populate `Messages`
INSERT INTO Messages (sender_id, sendee_id, content) VALUES (1, 2, "Hello, this is #1");
INSERT INTO Messages (sender_id, sendee_id, content) VALUES (2, 1, "Oh hi #1, I am #2");
INSERT INTO Messages (sender_id, sendee_id, content) VALUES (3, 2, "First message from #3 to #2");
INSERT INTO Messages (sender_id, sendee_id, content) VALUES (3, 2, "Second message from #3 to #2");
INSERT INTO Messages (sender_id, sendee_id, content) VALUES (1, 4, "First #1 to #4");

CREATE TABLE Lists (
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(25) NOT NULL,
  description VARCHAR(100),
  creator_id INT NOT NULL,
  is_private BOOLEAN NOT NULL,
  FOREIGN KEY (creator_id) REFERENCES Users(id)
);

-- Populate Lists
INSERT INTO Lists (name, description, creator_id, is_private) VALUES ("#1 List", "No description", 1, false);
INSERT INTO Lists (name, description, creator_id, is_private) VALUES ("#1 Private List", "No one knows about this list", 1, true);
INSERT INTO Lists (name, description, creator_id, is_private) VALUES ("#4 List", "I also have a list", 4, false);

CREATE TABLE ListMembers (
  list_id INT NOT NULL,
  user_id INT NOT NULL,
  PRIMARY KEY(list_id, user_id),
  FOREIGN KEY (list_id) REFERENCES Lists(id),
  FOREIGN KEY (user_id) REFERENCES Users(id)
  -- Should check that `user_id` is not a private account.
);

INSERT INTO ListMembers (list_id, user_id) VALUES (1, 2);
INSERT INTO ListMembers (list_id, user_id) VALUES (1, 3);
INSERT INTO ListMembers (list_id, user_id) VALUES (2, 4);
INSERT INTO ListMembers (list_id, user_id) VALUES (3, 1);
INSERT INTO ListMembers (list_id, user_id) VALUES (3, 2);