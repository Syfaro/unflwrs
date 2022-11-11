use std::collections::{HashMap, HashSet};

use actix_session::{storage::CookieSessionStore, Session, SessionMiddleware};
use actix_web::{get, post, web, App, HttpResponse, HttpServer};
use askama::Template;
use bytes::Bytes;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

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
            .service(export_csv)
            .service(account_delete)
            .service(actix_files::Files::new("/static", "./static"))
            .app_data(web::Data::new(cx))
    })
    .bind(("0.0.0.0", 8080))
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
            interval.tick().await;

            if let Err(err) = refresh_accounts(&pool, &kp).await {
                tracing::error!("could not refresh accounts: {err}");
            }
        }
    });
}

async fn refresh_accounts(pool: &sqlx::PgPool, kp: &egg_mode::KeyPair) -> Result<(), AppError> {
    let old_accounts = sqlx::query!("SELECT twitter_account_id, consumer_key, consumer_secret FROM twitter_login WHERE last_updated IS NULL OR last_updated < now() - interval '6 hours' LIMIT 100").fetch_all(pool).await?;
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
async fn home(sess: Session) -> Result<actix_web::HttpResponse, AppError> {
    if matches!(sess.get::<i64>("twitter-user-id"), Ok(Some(_))) {
        return Ok(HttpResponse::Found()
            .insert_header(("location", "/feed"))
            .finish());
    }

    let body = HomeTemplate.render()?;
    Ok(actix_web::HttpResponse::Ok()
        .content_type("text/html")
        .body(body))
}

#[derive(Deserialize)]
struct LoginForm {
    oneoff: Option<String>,
}

#[post("/twitter/login")]
async fn twitter_login(
    cx: web::Data<Context>,
    sess: Session,
    form: web::Form<LoginForm>,
) -> Result<actix_web::HttpResponse, AppError> {
    let request_token =
        egg_mode::auth::request_token(&cx.kp, format!("{}/twitter/callback", cx.config.host_url))
            .await?;
    let auth_url = egg_mode::auth::authorize_url(&request_token);

    sess.insert("twitter-request-token", request_token)?;
    if form.oneoff.as_deref().unwrap_or_default() == "yes" {
        sess.insert("oneoff", true)?;
    }

    Ok(actix_web::HttpResponse::Found()
        .insert_header(("location", auth_url))
        .finish())
}

#[derive(Deserialize)]
struct TwitterCallbackQuery {
    oauth_verifier: String,
}

fn generate_token(n: usize) -> String {
    use rand::Rng;

    rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
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

    if sess
        .get::<bool>("oneoff")
        .unwrap_or_default()
        .unwrap_or_default()
    {
        return oneoff(user_id, token, sess).await;
    }

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
    sess.insert("csrf-token", generate_token(48))?;

    tokio::task::spawn(async move {
        if let Err(err) = refresh_account(cx.pool.clone(), user_id, token).await {
            tracing::error!("could not refresh account: {err}");
        }
    });

    Ok(HttpResponse::Found()
        .insert_header(("location", "/feed"))
        .finish())
}

async fn oneoff(
    user_id: u64,
    token: egg_mode::Token,
    sess: Session,
) -> actix_web::Result<actix_web::HttpResponse> {
    tracing::info!("{user_id} requested oneoff export");
    sess.clear();

    let follower_ids = egg_mode::user::followers_ids(user_id, &token)
        .with_page_size(5000)
        .map_ok(|r| r.response)
        .try_collect::<Vec<_>>()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    tracing::debug!("found {} followers", follower_ids.len());

    let friend_ids = egg_mode::user::friends_ids(user_id, &token)
        .with_page_size(5000)
        .map_ok(|r| r.response)
        .try_collect::<Vec<_>>()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    tracing::debug!("found {} friends", friend_ids.len());

    let mut users = HashMap::with_capacity(follower_ids.len() + friend_ids.len());

    let chunks = follower_ids
        .iter()
        .chain(friend_ids.iter())
        .unique()
        .chunks(100);
    for (idx, chunk) in chunks.into_iter().enumerate() {
        tracing::debug!("loading chunk {idx}");

        let ids = chunk.map(|id| egg_mode::user::UserID::ID(*id));
        let resp = egg_mode::user::lookup(ids, &token)
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?;

        for user in resp.response {
            users.insert(user.id, user);
        }
    }
    tracing::debug!("fetched information for {} users", users.len());

    let (mut wtr, rdr) = tokio::io::duplex(1024);

    tokio::task::spawn(async move {
        let mut wtr = async_zip::write::ZipFileWriter::new(&mut wtr);

        create_user_entry(&mut wtr, "followers.csv".to_string(), &users, &follower_ids).await;
        create_user_entry(&mut wtr, "friends.csv".to_string(), &users, &friend_ids).await;

        wtr.close().await.unwrap();
    });

    let stream = tokio_util::codec::FramedRead::new(rdr, tokio_util::codec::BytesCodec::new())
        .map_ok(|b| b.freeze());

    Ok(HttpResponse::Ok()
        .insert_header((
            "content-disposition",
            r#"attachment; filename="twitter-users.zip""#,
        ))
        .content_type("application/zip")
        .streaming(stream))
}

#[derive(Default, Serialize)]
struct TwitterEntry<'a> {
    id: u64,
    screen_name: Option<&'a str>,
    website: Option<&'a str>,
    location: Option<&'a str>,
    bio: Option<&'a str>,
}

async fn create_user_entry<W: tokio::io::AsyncWrite + Unpin>(
    wtr: &mut async_zip::write::ZipFileWriter<W>,
    name: String,
    users: &HashMap<u64, egg_mode::user::TwitterUser>,
    ids: &[u64],
) {
    let opts = async_zip::ZipEntryBuilder::new(name, async_zip::Compression::Deflate);
    let entry_writer = wtr.write_entry_stream(opts).await.unwrap();
    let mut csv = csv_async::AsyncSerializer::from_writer(entry_writer);

    for id in ids.iter().copied() {
        let row = match users.get(&id) {
            Some(user) => TwitterEntry {
                id: user.id,
                screen_name: Some(&user.screen_name),
                website: user.url.as_deref(),
                location: user.location.as_deref(),
                bio: user.description.as_deref(),
            },
            None => {
                tracing::warn!("user was not loaded: {id}");

                TwitterEntry {
                    id,
                    ..Default::default()
                }
            }
        };

        csv.serialize(row).await.unwrap();
    }

    let entry_writer = csv.into_inner().await.unwrap();
    entry_writer.close().await.unwrap();
}

#[allow(dead_code)]
#[derive(Debug)]
struct TwitterUser {
    id: i64,
    screen_name: String,
    display_name: String,
}

#[derive(Debug)]
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

#[derive(Debug)]
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
    csrf_token: String,
}

#[get("/feed")]
async fn feed(cx: web::Data<Context>, sess: Session) -> Result<HttpResponse, actix_web::Error> {
    let Some(user_id) = sess
        .get::<i64>("twitter-user-id")? else {
            return Ok(HttpResponse::Found().insert_header(("location", "/")).finish())
        };

    let csrf_token: String = sess.get("csrf-token")?.ok_or_else(|| {
        sess.clear();
        actix_web::error::ErrorUnauthorized("missing token")
    })?;

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
        csrf_token,
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

#[get("/export/csv")]
async fn export_csv(
    cx: web::Data<Context>,
    sess: Session,
) -> Result<HttpResponse, actix_web::Error> {
    let user_id: i64 = sess
        .get("twitter-user-id")?
        .ok_or_else(|| actix_web::error::ErrorUnauthorized("missing id"))?;

    tracing::info!("starting csv export");

    let (tx, rx) = tokio::sync::mpsc::channel(4);

    tokio::task::spawn(async move {
        let mut events = sqlx::query!("SELECT twitter_event.created_at, twitter_event.event_name, twitter_account.id, twitter_account.screen_name, twitter_account.display_name FROM twitter_event JOIN twitter_account ON twitter_account.id = twitter_event.related_twitter_account_id WHERE login_twitter_account_id = $1 ORDER BY created_at", user_id).map(|row| TwitterEventEntry {
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
        }).fetch(&cx.pool);

        while let Some(Ok(event)) = events.next().await {
            tx.send(event).await.unwrap();
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::from(rx).map(|event| {
        Ok::<_, std::convert::Infallible>(Bytes::from(format!(
            "{},{},{},{}\n",
            event.created_at.to_rfc3339(),
            event.event,
            event.user.id,
            event.user.screen_name
        )))
    });

    Ok(HttpResponse::Ok()
        .content_type("text/csv")
        .streaming(stream))
}

#[derive(Deserialize)]
struct AccountDeleteForm {
    csrf: String,
}

#[post("/account/delete")]
async fn account_delete(
    cx: web::Data<Context>,
    sess: Session,
    form: web::Form<AccountDeleteForm>,
) -> Result<HttpResponse, actix_web::Error> {
    let user_id: i64 = sess
        .get("twitter-user-id")?
        .ok_or_else(|| actix_web::error::ErrorUnauthorized("missing id"))?;

    let csrf_token: String = sess
        .get("csrf-token")?
        .ok_or_else(|| actix_web::error::ErrorUnauthorized("missing token"))?;

    if form.csrf != csrf_token {
        return Err(actix_web::error::ErrorUnauthorized("bad csrf token"));
    }

    sqlx::query!(
        "DELETE FROM twitter_login WHERE twitter_account_id = $1",
        user_id
    )
    .execute(&cx.pool)
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;

    sess.clear();

    Ok(HttpResponse::Found()
        .insert_header(("location", "/"))
        .finish())
}
