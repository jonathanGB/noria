-- Main Timeline
SELECT t.id t_id, t.timestamp t_created, t.content t_content, u.id u_id, u.handle u_handle, u.name u_name, t.reply_id t_r_id, t.retweet_id t_rt_id, retweets.*
FROM Tweets t 
  INNER JOIN Users u ON t.user_id = u.id -- Fetches user information about a tweet.
  LEFT JOIN ( -- Fetches retweet contents and its user information.
    SELECT t.id rt_id, t.timestamp rt_created, t.content rt_content, u.id rt_u_id, u.handle rt_u_handle, u.name rt_u_name
    FROM Tweets t
      INNER JOIN Users u ON t.user_id = u.id 
  ) retweets ON t.retweet_id = retweets.rt_id
WHERE t.user_id IN ( -- Limits to tweets/retweets/replies by user followed.
  SELECT followed_id FROM Follows WHERE user_id = 1)
ORDER BY t.timestamp DESC;

SELECT t.id t_id, t.timestamp t_created, t.content t_content, u.id u_id, u.handle u_handle, u.name u_name, t.reply_id t_r_id, t.retweet_id t_rt_id, retweets.*
FROM Tweets t 
  INNER JOIN Users u ON t.user_id = u.id -- Fetches user information about a tweet.
  LEFT JOIN ( -- Fetches retweet contents and its user information.
    SELECT t.id rt_id, t.timestamp rt_created, t.content rt_content, u.id rt_u_id, u.handle rt_u_handle, u.name rt_u_name
    FROM Tweets t
      INNER JOIN Users u ON t.user_id = u.id 
  ) retweets ON t.retweet_id = retweets.rt_id
  INNER JOIN (
    SELECT followed_id FROM Follows WHERE user_id = 1
  ) followed ON t.user_id = followed.followed_id
ORDER BY t.timestamp DESC;

-- User Timeline Tweets
SELECT t.id t_id, t.timestamp t_created, t.content t_content, u.id u_id, u.handle u_handle, u.name u_name, t.reply_id t_r_id, retweets.*
FROM Tweets t 
  INNER JOIN Users u ON t.user_id = u.id -- Fetches user information about a tweet.
  LEFT JOIN ( -- Fetches retweet contents and its user information.
    SELECT t.id rt_id, t.timestamp rt_created, t.content rt_content, u.id rt_u_id, u.handle rt_u_handle, u.name rt_u_name
    FROM Tweets t
      INNER JOIN Users u ON t.user_id = u.id 
  ) retweets ON t.retweet_id = retweets.rt_id
WHERE u.id = 1
ORDER BY t.timestamp DESC;

-- User Timeline Info
SELECT *
FROM Users
WHERE id = 1;

-- Direct Messages (DMs) - Conversation (w/out mdb)
-- User 1 wants to see messages w/ user 2
SELECT *
FROM Messages
WHERE (sender_id = 2 AND sendee_id = 1) OR (sendee_id = 2 AND sender_id = 1)
ORDER BY timestamp DESC;

-- Direct Messages (DMs) - Conversation (w/ mdb)
-- User 1 wants to see messages w/ user 2
SELECT *
FROM Messages
WHERE sender_id = 2 OR sendee_id = 2
ORDER BY timestamp DESC;

-- Enumerate all lists by a user
SELECT *
FROM Lists
WHERE creator_id = 1;

-- Get metadata (header information) of a list
SELECT *
FROM Lists
WHERE id = 1;

-- Get Members of a List
SELECT t.id t_id, t.timestamp t_created, t.content t_content, u.id u_id, u.handle u_handle, u.name u_name, t.reply_id t_r_id, retweets.*
FROM Tweets t 
  INNER JOIN Users u ON t.user_id = u.id -- Fetches user information about a tweet.
  LEFT JOIN ( -- Fetches retweet contents and its user information.
    SELECT t.id rt_id, t.timestamp rt_created, t.content rt_content, u.id rt_u_id, u.handle rt_u_handle, u.name rt_u_name
    FROM Tweets t
      INNER JOIN Users u ON t.user_id = u.id 
  ) retweets ON t.retweet_id = retweets.rt_id
WHERE t.user_id IN ( -- Limits to tweets/retweets/replies by members of the list.
  SELECT user_id FROM ListMembers WHERE list_id = 2)
ORDER BY t.timestamp DESC;