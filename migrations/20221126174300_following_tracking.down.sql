ALTER TABLE
    twitter_login DROP COLUMN track_followers;

ALTER TABLE
    twitter_login DROP COLUMN track_following;

ALTER TABLE
    twitter_event DROP COLUMN tracking;

ALTER TABLE
    twitter_state RENAME COLUMN account_ids TO follower_ids;

ALTER TABLE
    twitter_state RENAME COLUMN account_count TO follower_count;

ALTER TABLE
    twitter_state DROP COLUMN tracking;
