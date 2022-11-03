use std::collections::HashSet;

use actix_session::{storage::CookieSessionStore, Session, SessionMiddleware};
use actix_web::{get, post, web, App, HttpResponse, HttpServer};
use askama::Template;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;

#[derive(Clone, Parser)]
struct Config {
    #[clap(long, env)]
    database_url: String,
    #[clap(long, env)]
    host_url: String,
    #[clap(long, env)]
    session_secret: String,

    #[clap(long, env)]
    twitter_consumer_key: String,
    #[clap(long, env)]
    twitter_consumer_secret: String,
}

struct Context {
    pool: sqlx::PgPool,
    kp: egg_mode::KeyPair,
    config: Config,
}

#[actix_web::main]
async fn main() {
    let _ = dotenvy::dotenv();
    let config = Config::parse();

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn,unflwrs=info");
    }
    tracing_subscriber::fmt::init();

    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("must be able to connect to database");

    sqlx::migrate!()
        .run(&pool)
        .await
        .expect("could not run migrations");

    let kp = egg_mode::KeyPair::new(
        config.twitter_consumer_key.clone(),
        config.twitter_consumer_secret.clone(),
    );

    refresh_stale_accounts(pool.clone(), kp.clone()).await;

    tracing::info!("starting server");

    HttpServer::new(move || {
        let cx = Context {
            pool: pool.clone(),
            kp: kp.clone(),
            config: config.clone(),
        };

        let key = actix_web::cookie::Key::from(config.session_secret.as_bytes());
        let session = SessionMiddleware::new(CookieSessionStore::default(), key);

        App::new()
            .wrap(session)
            .service(home)
            .service(twitter_login)
            .service(twitter_callback)
            .service(feed)
            .service(feed_graph)
            .service(actix_files::Files::new("/static", "./static"))
            .app_data(web::Data::new(cx))
    })
    .bind(("127.0.0.1", 8080))
    .unwrap()
    .run()
    .await
    .unwrap()
}

#[derive(Debug)]
struct AppError(eyre::Report);

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl actix_web::error::ResponseError for AppError {}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for AppError {
    fn from(err: E) -> Self {
        AppError(eyre::Report::new(err))
    }
}

#[tracing::instrument(skip(pool, token))]
async fn refresh_account(
    pool: sqlx::PgPool,
    user_id: u64,
    token: egg_mode::Token,
) -> Result<i64, AppError> {
    let mut tx = pool.begin().await?;

    let existing_follower_ids = sqlx::query!(
        "SELECT follower_ids FROM twitter_state WHERE login_twitter_account_id = $1 ORDER BY created_at DESC LIMIT 1",
        i64::try_from(user_id).unwrap()
    )
    .map(|row| row.follower_ids.into_iter().filter_map(|id| u64::try_from(id).ok()).collect::<Vec<_>>())
    .fetch_optional(&mut tx).await?.unwrap_or_default();
    let existing_follower_ids: HashSet<u64> = HashSet::from_iter(existing_follower_ids);
    tracing::debug!(
        "found {} existing follower ids",
        existing_follower_ids.len()
    );

    let current_follower_ids = egg_mode::user::followers_ids(user_id, &token)
        .with_page_size(5000)
        .map_ok(|r| r.response)
        .try_collect::<Vec<_>>()
        .await?;
    tracing::debug!("found {} new follower ids", current_follower_ids.len());
    let current_follower_i64s = current_follower_ids
        .iter()
        .filter_map(|id| i64::try_from(*id).ok())
        .collect::<Vec<_>>();
    sqlx::query!(
        "INSERT INTO twitter_state (login_twitter_account_id, follower_ids) VALUES ($1, $2)",
        i64::try_from(user_id).unwrap(),
        &current_follower_i64s
    )
    .execute(&mut tx)
    .await?;

    let unknown_ids = find_unknown_ids(&pool, &current_follower_i64s).await?;
    for chunk in unknown_ids.chunks(100) {
        tracing::debug!("looking up batch of {} users", chunk.len());

        let ids = chunk
            .iter()
            .map(|id| egg_mode::user::UserID::ID(u64::try_from(*id).unwrap()));
        let accounts = egg_mode::user::lookup(ids, &token).await?;

        let mut tx = pool.begin().await?;
        for resp in accounts {
            let account = resp.response;
            sqlx::query!("INSERT INTO twitter_account (id, screen_name, display_name) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET last_updated = current_timestamp, screen_name = EXCLUDED.screen_name, display_name = EXCLUDED.display_name", i64::try_from(account.id).unwrap(), account.screen_name, account.name).execute(&mut tx).await?;
        }
        tx.commit().await?;
    }

    let current_follower_ids: HashSet<u64> = HashSet::from_iter(current_follower_ids);

    let new_unfollower_ids = existing_follower_ids.difference(&current_follower_ids);
    let new_follower_ids = current_follower_ids.difference(&existing_follower_ids);

    for unfollower_id in new_unfollower_ids {
        sqlx::query!("INSERT INTO twitter_event (login_twitter_account_id, related_twitter_account_id, event_name) VALUES ($1, $2, 'unfollow')", i64::try_from(user_id).unwrap(), i64::try_from(*unfollower_id).unwrap()).execute(&mut tx).await?;
    }

    for follower_id in new_follower_ids {
        sqlx::query!("INSERT INTO twitter_event (login_twitter_account_id, related_twitter_account_id, event_name) VALUES ($1, $2, 'follow')", i64::try_from(user_id).unwrap(), i64::try_from(*follower_id).unwrap()).execute(&mut tx).await?;
    }

    sqlx::query!(
        "UPDATE twitter_login SET last_updated = current_timestamp WHERE twitter_account_id = $1",
        i64::try_from(user_id).unwrap()
    )
    .execute(&mut tx)
    .await?;

    tx.commit().await?;

    Ok(i64::try_from(user_id).unwrap())
}

async fn find_unknown_ids(pool: &sqlx::PgPool, given_ids: &[i64]) -> Result<Vec<i64>, AppError> {
    let existing_ids = sqlx::query_scalar!("SELECT id FROM twitter_account WHERE id = ANY($1) AND last_updated > now() - interval '7 day'", given_ids).fetch_all(pool).await?;
    let existing_ids: HashSet<i64> = HashSet::from_iter(existing_ids);

    let new_ids = given_ids
        .iter()
        .copied()
        .filter(|given_id| !existing_ids.contains(given_id))
        .collect();
    Ok(new_ids)
}

async fn refresh_stale_accounts(pool: sqlx::PgPool, kp: egg_mode::KeyPair) {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5 * 60));

        loop {
            if let Err(err) = refresh_accounts(&pool, &kp).await {
                tracing::error!("could not refresh accounts: {err}");
            }

            interval.tick().await;
        }
    });
}

async fn refresh_accounts(pool: &sqlx::PgPool, kp: &egg_mode::KeyPair) -> Result<(), AppError> {
    let old_accounts = sqlx::query!("SELECT twitter_account_id, consumer_key, consumer_secret FROM twitter_login WHERE last_updated IS NULL OR last_updated < now() - interval '1 day' LIMIT 100").fetch_all(pool).await?;
    tracing::info!("found {} accounts needing update", old_accounts.len());

    let tokens = old_accounts.into_iter().map(|row| {
        (row.twitter_account_id, {
            let access = egg_mode::KeyPair::new(row.consumer_key, row.consumer_secret);
            egg_mode::Token::Access {
                consumer: kp.clone(),
                access,
            }
        })
    });

    let mut futs = futures::stream::iter(tokens.map(|(twitter_account_id, token)| {
        refresh_account(
            pool.clone(),
            u64::try_from(twitter_account_id).unwrap(),
            token,
        )
    }))
    .buffer_unordered(4);

    while let Some(res) = futs.next().await {
        match res {
            Ok(twitter_account_id) => tracing::info!(twitter_account_id, "updated account"),
            Err(err) => tracing::error!("could not update account: {err}"),
        }
    }

    Ok(())
}

#[derive(Template)]
#[template(path = "home.html")]
struct HomeTemplate;

#[get("/")]
async fn home() -> Result<actix_web::HttpResponse, AppError> {
    let body = HomeTemplate.render()?;
    Ok(actix_web::HttpResponse::Ok()
        .content_type("text/html")
        .body(body))
}

#[post("/twitter/login")]
async fn twitter_login(
    cx: web::Data<Context>,
    sess: Session,
) -> Result<actix_web::HttpResponse, AppError> {
    let request_token =
        egg_mode::auth::request_token(&cx.kp, format!("{}/twitter/callback", cx.config.host_url))
            .await?;
    let auth_url = egg_mode::auth::authorize_url(&request_token);

    sess.insert("twitter-request-token", request_token)?;

    Ok(actix_web::HttpResponse::Found()
        .insert_header(("location", auth_url))
        .finish())
}

#[derive(Deserialize)]
struct TwitterCallbackQuery {
    oauth_verifier: String,
}

#[get("/twitter/callback")]
async fn twitter_callback(
    cx: web::Data<Context>,
    sess: Session,
    query: web::Query<TwitterCallbackQuery>,
) -> Result<actix_web::HttpResponse, actix_web::Error> {
    let request_token: egg_mode::KeyPair = sess
        .get("twitter-request-token")?
        .ok_or_else(|| actix_web::error::ErrorBadRequest("missing session data"))?;

    let (token, user_id, _screen_name) =
        egg_mode::auth::access_token(cx.kp.clone(), &request_token, &query.oauth_verifier)
            .await
            .map_err(actix_web::error::ErrorBadRequest)?;

    let user_kp = match &token {
        egg_mode::Token::Access {
            consumer: _,
            access,
        } => access,
        _ => unreachable!("access token should always be access token"),
    };

    let resp = egg_mode::user::show(user_id, &token)
        .await
        .map_err(actix_web::error::ErrorBadRequest)?;
    let account = resp.response;

    sqlx::query!(
        "INSERT INTO twitter_account (id, screen_name, display_name) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET last_updated = current_timestamp, screen_name = EXCLUDED.screen_name, display_name = EXCLUDED.display_name",
        i64::try_from(account.id).unwrap(),
        account.screen_name,
        account.name
    ).execute(&cx.pool).await.map_err(actix_web::error::ErrorInternalServerError)?;

    sqlx::query!(
        "INSERT INTO twitter_login (twitter_account_id, consumer_key, consumer_secret) VALUES ($1, $2, $3) ON CONFLICT (twitter_account_id) DO UPDATE SET consumer_key = EXCLUDED.consumer_key, consumer_secret = EXCLUDED.consumer_secret",
        i64::try_from(account.id).unwrap(),
        user_kp.key.as_ref(),
        user_kp.secret.as_ref()
    ).execute(&cx.pool).await.map_err(actix_web::error::ErrorInternalServerError)?;

    sess.insert("twitter-user-id", user_id)?;

    tokio::task::spawn(async move {
        if let Err(err) = refresh_account(cx.pool.clone(), user_id, token).await {
            tracing::error!("could not refresh account: {err}");
        }
    });

    Ok(HttpResponse::Found()
        .insert_header(("location", "/feed"))
        .finish())
}

struct TwitterUser {
    id: i64,
    screen_name: String,
    display_name: String,
}

enum TwitterEvent {
    Follow,
    Unfollow,
}

impl std::fmt::Display for TwitterEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Follow => write!(f, "follow"),
            Self::Unfollow => write!(f, "unfollow"),
        }
    }
}

struct TwitterEventEntry {
    user: TwitterUser,
    event: TwitterEvent,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Template)]
#[template(path = "feed.html")]
struct FeedTemplate {
    user: TwitterUser,
    events: Vec<TwitterEventEntry>,
    last_updated: Option<chrono::DateTime<chrono::Utc>>,
}

#[get("/feed")]
async fn feed(cx: web::Data<Context>, sess: Session) -> Result<HttpResponse, actix_web::Error> {
    let user_id: i64 = sess
        .get("twitter-user-id")?
        .ok_or_else(|| actix_web::error::ErrorUnauthorized("missing id"))?;

    let user = sqlx::query_as!(
        TwitterUser,
        "SELECT id, screen_name, display_name FROM twitter_account WHERE id = $1",
        user_id
    )
    .fetch_one(&cx.pool)
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;

    let events = sqlx::query!("SELECT twitter_event.created_at, twitter_event.event_name, twitter_account.id, twitter_account.screen_name, twitter_account.display_name FROM twitter_event JOIN twitter_account ON twitter_account.id = twitter_event.related_twitter_account_id WHERE login_twitter_account_id = $1 ORDER BY created_at DESC LIMIT 5000", user_id).map(|row| TwitterEventEntry {
        user: TwitterUser {
            id: row.id,
            screen_name: row.screen_name,
            display_name: row.display_name,
        },
        event: match row.event_name.as_ref() {
            "follow" => TwitterEvent::Follow,
            "unfollow" => TwitterEvent::Unfollow,
            _ => unreachable!(),
        },
        created_at: row.created_at,
    }).fetch_all(&cx.pool).await.map_err(actix_web::error::ErrorInternalServerError)?;

    let last_updated = sqlx::query_scalar!(
        "SELECT max(created_at) FROM twitter_state WHERE login_twitter_account_id = $1",
        user_id
    )
    .fetch_optional(&cx.pool)
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?
    .flatten();

    let body = FeedTemplate {
        user,
        events,
        last_updated,
    }
    .render()
    .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok().content_type("text/html").body(body))
}

#[get("/feed/graph")]
async fn feed_graph(
    cx: web::Data<Context>,
    sess: Session,
) -> Result<web::Json<Vec<(i64, i32)>>, actix_web::Error> {
    let user_id: i64 = sess
        .get("twitter-user-id")?
        .ok_or_else(|| actix_web::error::ErrorUnauthorized("missing id"))?;

    let entries = sqlx::query!(
        "SELECT created_at, follower_count FROM twitter_state WHERE login_twitter_account_id = $1 ORDER BY created_at LIMIT 100",
        user_id
    )
    .map(|row| (row.created_at.timestamp(), row.follower_count))
    .fetch_all(&cx.pool)
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(web::Json(entries))
}
