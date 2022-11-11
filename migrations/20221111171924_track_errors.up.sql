ALTER TABLE
    twitter_login
ADD
    COLUMN error_count INTEGER NOT NULL DEFAULT 0;
