CREATE TABLE twitter_account (
    id BIGINT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    screen_name TEXT NOT NULL,
    display_name TEXT NOT NULL
);

CREATE TABLE twitter_login (
    twitter_account_id BIGINT NOT NULL PRIMARY KEY REFERENCES twitter_account (id),
    last_updated TIMESTAMP WITH TIME ZONE,
    consumer_key TEXT NOT NULL,
    consumer_secret TEXT NOT NULL
);

CREATE TABLE twitter_event (
    id INTEGER NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    login_twitter_account_id BIGINT NOT NULL REFERENCES twitter_login (twitter_account_id) ON DELETE CASCADE,
    related_twitter_account_id BIGINT NOT NULL REFERENCES twitter_account (id),
    event_name TEXT NOT NULL
);

CREATE INDEX twitter_event_idx ON twitter_event (login_twitter_account_id, created_at);

CREATE TABLE twitter_state (
    id INTEGER NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
    login_twitter_account_id BIGINT NOT NULL REFERENCES twitter_login (twitter_account_id) ON DELETE CASCADE,
    follower_ids BIGINT[] NOT NULL,
    follower_count INTEGER NOT NULL GENERATED ALWAYS AS (cardinality(follower_ids)) STORED
);

CREATE INDEX twitter_state_idx ON twitter_state (login_twitter_account_id, created_at);
