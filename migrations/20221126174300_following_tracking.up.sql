ALTER TABLE
    twitter_login
ADD
    COLUMN track_followers BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE
    twitter_login
ADD
    COLUMN track_following BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE
    twitter_event
ADD
    COLUMN tracking TEXT;

UPDATE
    twitter_event
SET
    tracking = 'follower';

ALTER TABLE
    twitter_event
ALTER COLUMN
    tracking
SET
    NOT NULL;

ALTER TABLE
    twitter_state
RENAME COLUMN
    follower_ids TO account_ids;

ALTER TABLE
    twitter_state
RENAME COLUMN
    follower_count TO account_count;

ALTER TABLE
    twitter_state
ADD
    COLUMN tracking TEXT;

UPDATE
    twitter_state
SET
    tracking = 'follower';

ALTER TABLE
    twitter_state
ALTER COLUMN
    tracking
SET
    NOT NULL;
