use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use actix_session::{storage::CookieSessionStore, Session, SessionMiddleware};
use actix_web::{
    dev::{self, ServiceResponse},
    get,
    http::{header::ContentType, StatusCode},
    middleware::{ErrorHandlerResponse, ErrorHandlers},
    post, web, App, HttpResponse, HttpResponseBuilder, HttpServer,
};
use askama::Template;
use bytes::Bytes;
use clap::Parser;
use futures::{Future, StreamExt, TryFutureExt, TryStreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::Connection;
use tokio::io::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::Instrument;
use twitter_v2::{
    authorization::{Oauth2Client, Oauth2Token, Scope},
    oauth2::{AuthorizationCode, CsrfToken, PkceCodeChallenge, PkceCodeVerifier},
    prelude::PaginableApiResponse,
    query::{ListField, MediaField, TweetExpansion, TweetField, UserExpansion, UserField},
    TwitterApi,
};

#[derive(Clone, Parser)]
struct Config {
    #[clap(long, env)]
    database_url: String,
    #[clap(long, env)]
    redis_url: String,

    #[clap(long, env)]
    host_url: String,
    #[clap(long, env)]
    session_secret: String,

    #[clap(long, env)]
    twitter_consumer_key: String,
    #[clap(long, env)]
    twitter_consumer_secret: String,

    #[clap(long, env)]
    twitter_client_id: String,
    #[clap(long, env)]
    twitter_client_secret: String,
}

struct Context {
    pool: sqlx::PgPool,
    kp: egg_mode::KeyPair,
    config: Config,
    oauth2_client: Oauth2Client,
    redis: redis::aio::ConnectionManager,
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

    let oauth_client = Oauth2Client::new(
        config.twitter_client_id.clone(),
        config.twitter_client_secret.clone(),
        format!("{}/twitter/callback/v2", config.host_url)
            .parse()
            .unwrap(),
    );

    let redis_client =
        redis::aio::ConnectionManager::new(redis::Client::open(config.redis_url.clone()).unwrap())
            .await
            .unwrap();

    refresh_stale_accounts(pool.clone(), kp.clone()).await;

    tracing::info!("starting server");

    HttpServer::new(move || {
        let cx = Context {
            pool: pool.clone(),
            kp: kp.clone(),
            config: config.clone(),
            oauth2_client: oauth_client.clone(),
            redis: redis_client.clone(),
        };

        let key = actix_web::cookie::Key::from(config.session_secret.as_bytes());
        let session = SessionMiddleware::new(CookieSessionStore::default(), key);

        App::new()
            .wrap(session)
            .service(home)
            .service(twitter_login)
            .service(twitter_callback)
            .service(twitter_callback_v2)
            .service(export_v2)
            .service(export_events)
            .service(feed)
            .service(feed_graph)
            .service(export_csv)
            .service(export_ff)
            .service(signout)
            .service(account_delete)
            .service(actix_files::Files::new("/static", "./static"))
            .app_data(web::Data::new(cx))
            .wrap(tracing_actix_web::TracingLogger::default())
            .wrap(
                ErrorHandlers::new()
                    .handler(StatusCode::NOT_FOUND, error_page)
                    .handler(StatusCode::BAD_REQUEST, error_page)
                    .handler(StatusCode::INTERNAL_SERVER_ERROR, error_page)
                    .handler(StatusCode::UNAUTHORIZED, error_page),
            )
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

#[derive(Serialize)]
struct TwitterDbEvent {
    login_twitter_account_id: i64,
    related_twitter_account_id: i64,
    event: &'static str,
}

#[tracing::instrument(skip(pool, token))]
async fn refresh_account(
    pool: sqlx::PgPool,
    user_id: u64,
    token: egg_mode::Token,
    track_opts: TrackOptions,
) -> Result<i64, AppError> {
    let mut tx = pool.begin().await?;

    if track_opts.followers {
        tracing::info!("updating followers");

        let current_follower_ids = egg_mode::user::followers_ids(user_id, &token)
            .with_page_size(5000)
            .map_ok(|r| r.response)
            .try_collect::<Vec<_>>()
            .await?;

        let tx = tx.begin().await?;
        let tx =
            update_account_tracking(&pool, tx, user_id, current_follower_ids, &token, "follower")
                .await?;
        tx.commit().await?;
    }

    if track_opts.following {
        tracing::info!("updating following");

        let current_following_ids = egg_mode::user::friends_ids(user_id, &token)
            .with_page_size(5000)
            .map_ok(|r| r.response)
            .try_collect::<Vec<_>>()
            .await?;

        let tx = tx.begin().await?;
        let tx = update_account_tracking(
            &pool,
            tx,
            user_id,
            current_following_ids,
            &token,
            "following",
        )
        .await?;
        tx.commit().await?;
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

async fn update_account_tracking<'a>(
    pool: &sqlx::PgPool,
    mut tx: sqlx::Transaction<'a, sqlx::Postgres>,
    user_id: u64,
    current_ids: Vec<u64>,
    token: &egg_mode::Token,
    tracking: &str,
) -> Result<sqlx::Transaction<'a, sqlx::Postgres>, AppError> {
    let existing_ids = sqlx::query!(
        "SELECT account_ids FROM twitter_state WHERE login_twitter_account_id = $1 AND tracking = $2 ORDER BY created_at DESC LIMIT 1",
        i64::try_from(user_id).unwrap(),
        tracking,
    )
    .map(|row| {
        row
            .account_ids
            .into_iter()
            .filter_map(|id| u64::try_from(id).ok())
            .collect::<Vec<_>>()
    })
    .fetch_optional(&mut tx)
    .await?
    .unwrap_or_default();
    let existing_ids: HashSet<u64> = HashSet::from_iter(existing_ids);
    tracing::debug!("found {} existing ids", existing_ids.len());

    tracing::debug!("found {} new ids", current_ids.len());
    let current_i64s = current_ids
        .iter()
        .filter_map(|id| i64::try_from(*id).ok())
        .collect::<Vec<_>>();
    sqlx::query!(
        "INSERT INTO twitter_state (login_twitter_account_id, account_ids, tracking) VALUES ($1, $2, $3)",
        i64::try_from(user_id).unwrap(),
        &current_i64s,
        tracking,
    )
    .execute(&mut tx)
    .await?;

    let unknown_ids = find_unknown_ids(pool, &current_i64s).await?;
    for chunk in unknown_ids.chunks(100) {
        tracing::debug!("looking up batch of {} users", chunk.len());

        let ids = chunk
            .iter()
            .map(|id| egg_mode::user::UserID::ID(u64::try_from(*id).unwrap()));
        let accounts = egg_mode::user::lookup(ids, token).await?;

        let mut tx = pool.begin().await?;
        for resp in accounts {
            let account = resp.response;
            sqlx::query!(
                "INSERT INTO twitter_account (id, screen_name, display_name) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET last_updated = current_timestamp, screen_name = EXCLUDED.screen_name, display_name = EXCLUDED.display_name",
                i64::try_from(account.id).unwrap(),
                account.screen_name,
                account.name
            )
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
    }

    let current_ids: HashSet<u64> = HashSet::from_iter(current_ids);

    let new_unfollower_ids = existing_ids.difference(&current_ids);
    let new_follower_ids = current_ids.difference(&existing_ids);

    let events: Vec<_> = new_unfollower_ids
        .map(|id| (*id, "unfollow"))
        .chain(new_follower_ids.map(|id| (*id, "follow")))
        .map(|(related_id, event)| TwitterDbEvent {
            login_twitter_account_id: i64::try_from(user_id).unwrap(),
            related_twitter_account_id: i64::try_from(related_id).unwrap(),
            event,
        })
        .collect();

    for (idx, chunk) in events.chunks(100).enumerate() {
        tracing::debug!("inserting chunk {idx} of {} events", chunk.len());
        let data = serde_json::to_value(chunk)?;

        sqlx::query!(
            "INSERT INTO twitter_event
                (login_twitter_account_id, related_twitter_account_id, event_name, tracking)
            SELECT
                event.login_twitter_account_id,
                event.related_twitter_account_id,
                event.event,
                $2
            FROM jsonb_to_recordset($1) AS
                event(login_twitter_account_id bigint, related_twitter_account_id bigint, event text)
            JOIN twitter_account
                ON twitter_account.id = event.related_twitter_account_id",
            data,
            tracking,
        )
        .execute(&mut tx)
        .await?;
    }

    Ok(tx)
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
    let old_accounts = sqlx::query!("SELECT twitter_account_id, consumer_key, consumer_secret, track_followers, track_following FROM twitter_login WHERE last_updated IS NULL OR last_updated < now() - interval '6 hours' AND error_count < 10 LIMIT 100").fetch_all(pool).await?;
    tracing::info!("found {} accounts needing update", old_accounts.len());

    let tokens = old_accounts.into_iter().map(|row| {
        (
            row.twitter_account_id,
            TrackOptions {
                followers: row.track_followers,
                following: row.track_following,
            },
            {
                let access = egg_mode::KeyPair::new(row.consumer_key, row.consumer_secret);
                egg_mode::Token::Access {
                    consumer: kp.clone(),
                    access,
                }
            },
        )
    });

    let mut futs = futures::stream::iter(tokens.map(|(twitter_account_id, opts, token)| {
        refresh_account(
            pool.clone(),
            u64::try_from(twitter_account_id).unwrap(),
            token,
            opts,
        )
        .map_err(move |err| (err, twitter_account_id))
    }))
    .buffer_unordered(4);

    while let Some(res) = futs.next().await {
        match res {
            Ok(twitter_account_id) => tracing::info!(twitter_account_id, "updated account"),
            Err((err, twitter_account_id)) => {
                tracing::error!(twitter_account_id, "could not update account: {err}");
                sqlx::query!("UPDATE twitter_login SET error_count = error_count + 1 WHERE twitter_account_id = $1", twitter_account_id).execute(pool).await?;
            }
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

    sess.clear();

    let body = HomeTemplate.render()?;
    Ok(actix_web::HttpResponse::Ok()
        .content_type("text/html")
        .body(body))
}

#[derive(Template)]
#[template(path = "error.html")]
struct ErrorTemplate {
    message: Cow<'static, str>,
}

fn error_page<B>(res: dev::ServiceResponse<B>) -> actix_web::Result<ErrorHandlerResponse<B>> {
    let status = res.status();
    let request = res.into_parts().0;

    let body = ErrorTemplate {
        message: status.to_string().into(),
    }
    .render()
    .unwrap();
    let new_response = HttpResponseBuilder::new(status)
        .insert_header(ContentType::html())
        .body(body);

    Ok(ErrorHandlerResponse::Response(
        ServiceResponse::new(request, new_response).map_into_right_body(),
    ))
}

#[derive(Deserialize)]
struct LoginForm {
    oneoff: Option<String>,
    use_json: Option<String>,
    export_bookmarks: Option<String>,
    download_profile_pictures: Option<String>,
    track_followers: Option<String>,
    track_following: Option<String>,
}

#[derive(Default, Serialize, Deserialize)]
struct ExportOptions {
    json: bool,
    bookmarks: bool,
    profile_pictures: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TrackOptions {
    following: bool,
    followers: bool,
}

#[post("/twitter/login")]
async fn twitter_login(
    cx: web::Data<Context>,
    sess: Session,
    form: web::Form<LoginForm>,
) -> Result<actix_web::HttpResponse, AppError> {
    let is_oneoff = form.oneoff.as_deref().unwrap_or_default() == "yes";

    let location = if is_oneoff {
        let opts = ExportOptions {
            json: matches!(&form.use_json, Some(val) if val == "on"),
            bookmarks: matches!(&form.export_bookmarks, Some(val) if val == "on"),
            profile_pictures: matches!(&form.download_profile_pictures, Some(val) if val == "on"),
        };

        let mut scopes = vec![
            Scope::TweetRead,
            Scope::UsersRead,
            Scope::FollowsRead,
            Scope::ListRead,
        ];

        if opts.bookmarks {
            scopes.push(Scope::BookmarkRead);
        }

        sess.insert("export-options", opts)?;

        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let (url, state) = cx.oauth2_client.auth_url(challenge, scopes);

        sess.insert("twitter-verifier", verifier)?;
        sess.insert("twitter-state", state)?;

        url.as_str().to_string()
    } else {
        let track_options = TrackOptions {
            followers: matches!(&form.track_followers, Some(val) if val == "on"),
            following: matches!(&form.track_following, Some(val) if val == "on"),
        };

        sess.insert("track-options", track_options)?;

        let request_token = egg_mode::auth::request_token(
            &cx.kp,
            format!("{}/twitter/callback", cx.config.host_url),
        )
        .await?;
        let auth_url = egg_mode::auth::authorize_url(&request_token);

        sess.insert("twitter-request-token", request_token)?;

        auth_url
    };

    Ok(actix_web::HttpResponse::Found()
        .insert_header(("location", location))
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

    let opts: TrackOptions = sess.get("track-options")?.unwrap_or_default();

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
        "INSERT INTO twitter_login (twitter_account_id, consumer_key, consumer_secret, track_followers, track_following) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (twitter_account_id) DO UPDATE SET consumer_key = EXCLUDED.consumer_key, consumer_secret = EXCLUDED.consumer_secret, error_count = 0",
        i64::try_from(account.id).unwrap(),
        user_kp.key.as_ref(),
        user_kp.secret.as_ref(),
        opts.followers,
        opts.following,
    ).execute(&cx.pool).await.map_err(actix_web::error::ErrorInternalServerError)?;

    sess.insert("twitter-user-id", user_id)?;
    sess.insert("csrf-token", generate_token(48))?;

    tokio::task::spawn(async move {
        if let Err(err) = refresh_account(cx.pool.clone(), user_id, token, opts).await {
            tracing::error!("could not refresh account: {err}");
        }
    });

    Ok(HttpResponse::Found()
        .insert_header(("location", "/feed"))
        .finish())
}

#[derive(Deserialize)]
struct TwitterCallbackV2Query {
    code: AuthorizationCode,
    state: CsrfToken,
}

#[derive(Template)]
#[template(path = "export.html")]
struct ExportTemplate;

#[get("/twitter/callback/v2")]
async fn twitter_callback_v2(
    cx: web::Data<Context>,
    sess: Session,
    query: web::Query<TwitterCallbackV2Query>,
) -> actix_web::Result<actix_web::HttpResponse> {
    let verifier: PkceCodeVerifier = sess
        .get("twitter-verifier")?
        .ok_or_else(|| actix_web::error::ErrorBadRequest("missing verifier"))?;

    let twitter_state: CsrfToken = sess
        .get("twitter-state")?
        .ok_or_else(|| actix_web::error::ErrorBadRequest("missing state"))?;
    if twitter_state.secret() != query.state.secret() {
        return Err(actix_web::error::ErrorBadRequest("wrong state"));
    }

    let token = cx
        .oauth2_client
        .request_token(query.code.clone(), verifier)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    sess.insert("twitter-v2-token", token)?;
    sess.insert("csrf-token", generate_token(48))?;

    let body = ExportTemplate
        .render()
        .map_err(actix_web::error::ErrorInternalServerError)?;
    Ok(HttpResponse::Ok()
        .insert_header(("content-type", "text/html"))
        .body(body))
}

#[get("/export/v2")]
async fn export_v2(
    cx: web::Data<Context>,
    sess: Session,
) -> actix_web::Result<actix_web::HttpResponse> {
    let session_name = sess
        .get("csrf-token")?
        .ok_or_else(|| actix_web::error::ErrorBadRequest("missing session name"))?;

    let token = sess
        .get("twitter-v2-token")?
        .ok_or_else(|| actix_web::error::ErrorUnauthorized("missing token"))?;

    let opts = sess.get("export-options")?.unwrap_or_default();

    oneoff_v2(cx.redis.clone(), session_name, token, opts).await
}

#[tracing::instrument(skip(redis, session_name, token, opts), fields(user_id))]
async fn oneoff_v2(
    mut redis: redis::aio::ConnectionManager,
    session_name: String,
    token: Oauth2Token,
    opts: ExportOptions,
) -> actix_web::Result<actix_web::HttpResponse> {
    let channel = format!("unflwrs:export:{session_name}");

    tracing::info!("starting oneoff v2");
    let api = TwitterApi::new(token);

    publish_event(&mut redis, &channel, ExportEvent::Started).await;

    let user = api
        .get_users_me()
        .user_fields([UserField::PublicMetrics])
        .send()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?
        .into_data()
        .unwrap();
    tracing::Span::current().record("user_id", user.id.as_u64());
    tracing::debug!("found user");

    let username = user.username.clone();

    let (wtr, rdr) = tokio::io::duplex(1024);

    tokio::task::spawn(
        async move {
            if let Err(err) = write_zips(&mut redis, wtr, api, user, &channel, opts).await {
                tracing::error!("could not finish zip: {err}");
                publish_event(
                    &mut redis,
                    &channel,
                    ExportEvent::Error {
                        message: err.into(),
                    },
                )
                .await;
            }
        }
        .in_current_span(),
    );

    let stream = tokio_util::codec::FramedRead::new(rdr, tokio_util::codec::BytesCodec::new())
        .map_ok(|b| b.freeze());

    Ok(HttpResponse::Ok()
        .insert_header((
            "content-disposition",
            format!(r#"attachment; filename="twitter-users-{username}.zip""#),
        ))
        .content_type("application/zip")
        .streaming(stream))
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "event", content = "data")]
enum ExportEvent {
    Started,
    Message { text: Cow<'static, str> },
    Error { message: Cow<'static, str> },
    Completed,
}

impl ExportEvent {
    fn message<S>(text: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        Self::Message { text: text.into() }
    }
}

#[get("/export/events")]
async fn export_events(
    cx: web::Data<Context>,
    sess: Session,
) -> actix_web::Result<impl actix_web::Responder> {
    let session_name: String = sess
        .get("csrf-token")?
        .ok_or_else(|| actix_web::error::ErrorBadRequest("missing session name"))?;
    let channel = format!("unflwrs:export:{session_name}");

    let redis = redis::Client::open(cx.config.redis_url.clone())
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let (tx, rx) = actix_web_lab::sse::channel(1);

    tokio::task::spawn(
        async move {
            tracing::debug!("starting pubsub");
            let mut sub = redis.get_async_connection().await.unwrap().into_pubsub();
            sub.subscribe(channel).await.unwrap();

            while let Some(message) = sub.on_message().next().await {
                let payload = message.get_payload::<Vec<u8>>().unwrap();
                let event: ExportEvent = serde_json::from_slice(&payload).unwrap();

                tx.send(actix_web_lab::sse::Event::Data(
                    actix_web_lab::sse::Data::new_json(&event)
                        .unwrap()
                        .event("progress"),
                ))
                .await
                .unwrap();

                if event == ExportEvent::Completed {
                    tracing::info!("got completed event, ending");
                    drop(tx);
                    return;
                }
            }
        }
        .in_current_span(),
    );

    Ok(rx)
}

#[tracing::instrument(skip(f))]
async fn fetch_all<T, A, M, F, Fut>(
    max_requests: usize,
    f: F,
) -> twitter_v2::Result<(Vec<T>, twitter_v2::data::Expansions)>
where
    T: serde::de::DeserializeOwned + Clone + Send + Sync,
    A: twitter_v2::Authorization + Send + Sync,
    M: serde::de::DeserializeOwned + twitter_v2::meta::PaginationMeta + Send + Sync,
    F: FnOnce() -> Fut,
    Fut: Future<Output = twitter_v2::Result<twitter_v2::ApiResponse<A, Vec<T>, M>>>,
{
    tracing::debug!("fetching first page");
    let mut resp = f().await?;
    let mut made_requests = 1;

    let mut items = Vec::new();
    let mut expansions = twitter_v2::data::Expansions {
        users: None,
        tweets: None,
        spaces: None,
        media: None,
        polls: None,
        places: None,
    };

    if let Some(new_items) = resp.data() {
        items.extend_from_slice(new_items);
    }

    if let Some(new_expansions) = resp.includes() {
        merge_expansions(&mut expansions, new_expansions);
    }

    while let Some(next_page) = resp.next_page().await? {
        tracing::debug!("fetched next page");
        resp = next_page;
        made_requests += 1;

        if let Some(new_items) = resp.data() {
            items.extend_from_slice(new_items);
        }

        if let Some(new_expansions) = resp.includes() {
            merge_expansions(&mut expansions, new_expansions);
        }

        if made_requests >= max_requests {
            tracing::warn!(made_requests, max_requests, "hit max requests");
            break;
        }
    }

    tracing::info!("completed fetch, discovered {} items", items.len());
    Ok((items, expansions))
}

macro_rules! merge_expansion_field {
    ($expansions:expr, $new_expansions:expr, $field:ident) => {
        if let Some(field) = $new_expansions.$field.as_ref() {
            let expansion_field = if let Some(existing_field) = $expansions.$field.as_mut() {
                existing_field
            } else {
                $expansions.$field = Some(Vec::with_capacity(field.len()));
                $expansions.$field.as_mut().unwrap()
            };
            expansion_field.extend_from_slice(field);
        }
    };
}

fn merge_expansions(
    expansions: &mut twitter_v2::data::Expansions,
    new_expansions: &twitter_v2::data::Expansions,
) {
    merge_expansion_field!(expansions, new_expansions, users);
    merge_expansion_field!(expansions, new_expansions, tweets);
    merge_expansion_field!(expansions, new_expansions, spaces);
    merge_expansion_field!(expansions, new_expansions, media);
    merge_expansion_field!(expansions, new_expansions, polls);
    merge_expansion_field!(expansions, new_expansions, places);
}

async fn publish_event(
    redis: &mut redis::aio::ConnectionManager,
    channel: &str,
    event: ExportEvent,
) {
    let event = serde_json::to_vec(&event).unwrap();
    let _ = redis.publish::<_, _, ()>(channel, event).await;
}

#[derive(Template)]
#[template(path = "export.txt")]
struct ExportReadmeTemplate<'a> {
    username: &'a str,
}

async fn write_zips<W, A>(
    client: &mut redis::aio::ConnectionManager,
    wtr: W,
    api: twitter_v2::TwitterApi<A>,
    user: twitter_v2::User,
    channel: &str,
    opts: ExportOptions,
) -> Result<(), String>
where
    W: tokio::io::AsyncWrite + Unpin,
    A: Send + Sync + twitter_v2::Authorization,
{
    tracing::debug!("starting zip writer");
    let mut wtr = async_zip::write::ZipFileWriter::new(wtr);

    let readme = ExportReadmeTemplate {
        username: &user.username,
    }
    .render()
    .unwrap();
    let entry =
        async_zip::ZipEntryBuilder::new("readme.txt".to_string(), async_zip::Compression::Deflate)
            .unix_permissions(777);
    let mut entry_writer = wtr.write_entry_stream(entry).await.unwrap();
    entry_writer.write_all(readme.as_bytes()).await.unwrap();
    entry_writer.close().await.unwrap();

    let following_count = user
        .public_metrics
        .as_ref()
        .map(|metrics| metrics.following_count)
        .unwrap_or_default();
    let follower_count = user
        .public_metrics
        .as_ref()
        .map(|metrics| metrics.followers_count)
        .unwrap_or_default();

    let mut follower_ids = HashSet::with_capacity(follower_count);

    let mut pictures = HashMap::new();

    if follower_count > 15_000 {
        tracing::warn!("user followers more than 15,000");
        publish_event(
            client,
            channel,
            ExportEvent::message("skipping followers, greater than 15,000"),
        )
        .await;
    } else {
        tracing::debug!("loading followers");
        publish_event(client, channel, ExportEvent::message("loading followers")).await;
        let mut follower_req = api.get_user_followers(user.id);
        follower_req
            .max_results(1000)
            .expansions([UserExpansion::PinnedTweetId])
            .user_fields([
                UserField::CreatedAt,
                UserField::Description,
                UserField::Entities,
                UserField::Id,
                UserField::Location,
                UserField::Name,
                UserField::PinnedTweetId,
                UserField::ProfileImageUrl,
                UserField::Protected,
                UserField::Url,
                UserField::Username,
            ])
            .tweet_fields([TweetField::Entities, TweetField::Text]);

        let (followers, expansions) = fetch_all(15, || follower_req.send())
            .await
            .map_err(|err| err.to_string())?;

        follower_ids.extend(followers.iter().map(|user| user.id));

        if opts.profile_pictures {
            pictures.extend(followers.iter().flat_map(|user| {
                user.profile_image_url
                    .as_ref()
                    .map(|url| (user.username.clone(), url.as_str().to_owned()))
            }));
        }

        if opts.json {
            create_user_entry_json(
                &mut wtr,
                "followers.json".to_string(),
                followers.iter(),
                &expansions.tweets.unwrap_or_default(),
            )
            .await;
        } else if !followers.is_empty() {
            create_user_entry_v2(
                &mut wtr,
                "followers.csv".to_string(),
                followers.iter(),
                &expansions.tweets.unwrap_or_default(),
            )
            .await;
        }
    }

    if following_count > 15_000 {
        tracing::warn!("user following more than 15,000");
        publish_event(
            client,
            channel,
            ExportEvent::message("skipping following, greater than 15,000"),
        )
        .await;
    } else {
        tracing::debug!("loading following");
        publish_event(client, channel, ExportEvent::message("loading following")).await;
        let mut following_req = api.get_user_following(user.id);
        following_req
            .max_results(1000)
            .expansions([UserExpansion::PinnedTweetId])
            .user_fields([
                UserField::CreatedAt,
                UserField::Description,
                UserField::Entities,
                UserField::Id,
                UserField::Location,
                UserField::Name,
                UserField::PinnedTweetId,
                UserField::ProfileImageUrl,
                UserField::Protected,
                UserField::Url,
                UserField::Username,
            ])
            .tweet_fields([TweetField::Entities, TweetField::Text]);

        let (following, expansions) = fetch_all(15, || following_req.send())
            .await
            .map_err(|err| err.to_string())?;

        let pinned_tweets = expansions.tweets.unwrap_or_default();

        if opts.profile_pictures {
            pictures.extend(following.iter().flat_map(|user| {
                user.profile_image_url
                    .as_ref()
                    .map(|url| (user.username.clone(), url.as_str().to_owned()))
            }));
        }

        if opts.json {
            create_user_entry_json(
                &mut wtr,
                "following.json".to_string(),
                following.iter(),
                &pinned_tweets,
            )
            .await;
        } else if !following.is_empty() {
            create_user_entry_v2(
                &mut wtr,
                "following.csv".to_string(),
                following.iter(),
                &pinned_tweets,
            )
            .await;

            if !follower_ids.is_empty()
                && following.iter().any(|user| follower_ids.contains(&user.id))
            {
                create_user_entry_v2(
                    &mut wtr,
                    "mutuals.csv".to_string(),
                    following
                        .iter()
                        .filter(|following| follower_ids.contains(&following.id)),
                    &pinned_tweets,
                )
                .await;
            }
        }
    }

    tracing::debug!("loading lists");
    let mut list_request = api.get_user_owned_lists(user.id);
    list_request
        .max_results(100)
        .list_fields([ListField::MemberCount]);

    let (lists, _expansions) = fetch_all(15, || list_request.send())
        .await
        .map_err(|err| err.to_string())?;

    for list in lists {
        tracing::debug!(list_id = %list.id, "loading list");
        publish_event(
            client,
            channel,
            ExportEvent::message(format!("loading list: {}", list.name)),
        )
        .await;

        let mut member_request = api.get_list_members(list.id);
        member_request
            .max_results(100)
            .expansions([UserExpansion::PinnedTweetId])
            .user_fields([
                UserField::CreatedAt,
                UserField::Description,
                UserField::Entities,
                UserField::Id,
                UserField::Location,
                UserField::Name,
                UserField::PinnedTweetId,
                UserField::ProfileImageUrl,
                UserField::Protected,
                UserField::Url,
                UserField::Username,
            ])
            .tweet_fields([TweetField::Entities, TweetField::Text]);

        let (members, expansions) = fetch_all(900, || member_request.send())
            .await
            .map_err(|err| err.to_string())?;

        if opts.profile_pictures {
            pictures.extend(members.iter().flat_map(|user| {
                user.profile_image_url
                    .as_ref()
                    .map(|url| (user.username.clone(), url.as_str().to_owned()))
            }));
        }

        if opts.json {
            create_user_entry_json(
                &mut wtr,
                format!("list-{}-{}.json", list.id, slug::slugify(list.name)),
                members.iter(),
                &expansions.tweets.unwrap_or_default(),
            )
            .await;
        } else if !members.is_empty() {
            create_user_entry_v2(
                &mut wtr,
                format!("list-{}-{}.csv", list.id, slug::slugify(list.name)),
                members.iter(),
                &expansions.tweets.unwrap_or_default(),
            )
            .await;
        }
    }

    if opts.bookmarks {
        // Videos can take a long time to download, set a higher timeout
        let req = reqwest::ClientBuilder::default()
            .timeout(std::time::Duration::from_secs(60 * 5))
            .build()
            .unwrap();

        let mut bookmarks_request = api.get_user_bookmarks(user.id);
        bookmarks_request
            .max_results(100)
            .expansions([
                TweetExpansion::AttachmentsMediaKeys,
                TweetExpansion::AuthorId,
            ])
            .media_fields([MediaField::MediaKey, MediaField::Url, MediaField::Variants])
            .tweet_fields([
                TweetField::Attachments,
                TweetField::AuthorId,
                TweetField::Entities,
                TweetField::Text,
            ])
            .user_fields([UserField::Id, UserField::Name, UserField::Username]);

        publish_event(client, channel, ExportEvent::message("loading bookmarks")).await;

        let (bookmarks, expansions) = fetch_all(180, || bookmarks_request.send()).await.unwrap();
        let expanded_users = expansions.users.unwrap_or_default();

        let opts = async_zip::ZipEntryBuilder::new(
            "bookmarks.csv".to_string(),
            async_zip::Compression::Deflate,
        )
        .unix_permissions(777);
        let entry_writer = wtr.write_entry_stream(opts).await.unwrap();
        let mut csv = csv_async::AsyncSerializer::from_writer(entry_writer);

        let mut used_media: HashMap<_, Vec<_>> = HashMap::new();

        for bookmark in bookmarks {
            let media_names = if let Some(twitter_v2::data::Attachments {
                media_keys: Some(media_keys),
                ..
            }) = bookmark.attachments
            {
                let ids = media_keys
                    .iter()
                    .map(|key| format!("media/{}_{}", bookmark.id, key))
                    .collect();

                for media_key in media_keys {
                    used_media.entry(media_key).or_default().push(bookmark.id);
                }

                ids
            } else {
                vec![]
            };

            let text = if let Some(url_entities) = bookmark
                .entities
                .as_ref()
                .and_then(|entities| entities.urls.as_ref())
            {
                url_entities.iter().fold(bookmark.text, |text, entity| {
                    if let Some(expanded_url) = entity.expanded_url.as_deref() {
                        text.replace(&entity.url, expanded_url)
                    } else {
                        text
                    }
                })
            } else {
                bookmark.text
            };

            let author = bookmark
                .author_id
                .and_then(|author_id| expanded_users.iter().find(|user| user.id == author_id));

            csv.serialize(BookmarkEntry {
                id: bookmark.id.as_u64(),
                author_username: author.map(|author| author.username.as_ref()),
                author_name: author.map(|author| author.name.as_ref()),
                text,
                media: media_names.join(" "),
            })
            .await
            .unwrap();
        }

        let entry_writer = csv.into_inner().await.unwrap();
        entry_writer.close().await.unwrap();

        for media in expansions.media.unwrap_or_default() {
            for tweet_id in used_media.remove(&media.media_key).unwrap_or_default() {
                tracing::trace!(media_id = %media.media_key, %tweet_id, "downloading media");

                publish_event(
                    client,
                    channel,
                    ExportEvent::message(format!("downloading media for bookmark {tweet_id}")),
                )
                .await;

                let media_info = if let Some(variants) = media.variants.as_ref() {
                    let best_url = variants
                        .iter()
                        .filter(|variant| variant.url.is_some())
                        .max_by(|a, b| {
                            a.bit_rate
                                .unwrap_or_default()
                                .cmp(&b.bit_rate.unwrap_or_default())
                        })
                        .and_then(|variant| variant.url.as_ref());

                    best_url.map(|url| (url, format!("media/{}_{}.mp4", tweet_id, media.media_key)))
                } else if let Some(url) = media.url.as_ref() {
                    Some((url, format!("media/{}_{}.jpg", tweet_id, media.media_key)))
                } else {
                    None
                };

                if let Some((url, filename)) = media_info {
                    if let Err(err) = save_media(&mut wtr, &req, filename, url.as_str()).await {
                        tracing::warn!(media_id = %media.media_key, %tweet_id, "could not download media: {err}");
                        publish_event(
                            client,
                            channel,
                            ExportEvent::message(format!("error downloading media: {tweet_id}")),
                        )
                        .await;
                    }
                }
            }
        }
    }

    let req = reqwest::ClientBuilder::default()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    for (username, url) in pictures {
        tracing::trace!(username, "downloading profile picture");
        publish_event(
            client,
            channel,
            ExportEvent::message(format!("profile picture: {username}")),
        )
        .await;

        if let Err(err) = save_media(
            &mut wtr,
            &req,
            format!("profile_picture/{username}.jpg"),
            &url,
        )
        .await
        {
            tracing::warn!(username, "could not download profile picture: {err}");
            publish_event(
                client,
                channel,
                ExportEvent::message(format!("error downloading profile picture: {username}")),
            )
            .await;
        }
    }

    wtr.close().await.unwrap();
    tracing::info!("finished writing zip");

    publish_event(client, channel, ExportEvent::Completed).await;

    Ok(())
}

#[derive(Serialize)]
struct BookmarkEntry<'a> {
    id: u64,
    author_username: Option<&'a str>,
    author_name: Option<&'a str>,
    text: String,
    media: String,
}

async fn save_media<W: tokio::io::AsyncWrite + Unpin, S: ToString>(
    wtr: &mut async_zip::write::ZipFileWriter<W>,
    client: &reqwest::Client,
    path: S,
    url: &str,
) -> Result<(), eyre::Report> {
    let opts = async_zip::ZipEntryBuilder::new(path.to_string(), async_zip::Compression::Deflate)
        .unix_permissions(777);
    let mut entry_writer = wtr.write_entry_stream(opts).await?;

    let body = client.get(url).send().await?;
    let mut stream = body
        .bytes_stream()
        .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
        .into_async_read()
        .compat();
    tokio::io::copy(&mut stream, &mut entry_writer).await?;

    entry_writer.close().await?;

    Ok(())
}

#[derive(Template)]
#[template(path = "ff_message.html")]
struct FFMessageTemplate;

#[get("/export/ff")]
async fn export_ff() -> Result<actix_web::HttpResponse, actix_web::Error> {
    let body = FFMessageTemplate
        .render()
        .map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok()
        .insert_header(("content-type", "text/html"))
        .body(body))
}

#[post("/signout")]
async fn signout(sess: Session) -> Result<actix_web::HttpResponse, actix_web::Error> {
    sess.clear();

    Ok(HttpResponse::Found()
        .insert_header(("location", "/"))
        .finish())
}

#[derive(Default, Serialize)]
struct TwitterEntry<'a> {
    id: u64,
    username: Option<&'a str>,
    name: Option<&'a str>,
    website: Option<String>,
    location: Option<&'a str>,
    bio: Option<String>,
    pinned_tweet: Option<String>,
    profile_picture_url: Option<&'a str>,
}

async fn create_user_entry_json<W: tokio::io::AsyncWrite + Unpin>(
    wtr: &mut async_zip::write::ZipFileWriter<W>,
    name: String,
    users: impl Iterator<Item = &twitter_v2::User>,
    pinned_tweets: &[twitter_v2::Tweet],
) {
    let opts = async_zip::ZipEntryBuilder::new(name, async_zip::Compression::Deflate)
        .unix_permissions(777);
    let mut entry_writer = wtr.write_entry_stream(opts).await.unwrap();

    let pinned_tweets: HashMap<_, _> = pinned_tweets
        .iter()
        .map(|tweet| (tweet.id, tweet))
        .collect();

    let users: Vec<_> = users
        .map(|user| {
            (
                user,
                user.pinned_tweet_id.and_then(|id| pinned_tweets.get(&id)),
            )
        })
        .collect();

    let data = serde_json::to_vec(&users).unwrap();

    entry_writer.write_all(&data).await.unwrap();
    entry_writer.close().await.unwrap();
}

#[tracing::instrument(skip(wtr, users, pinned_tweets))]
async fn create_user_entry_v2<W: tokio::io::AsyncWrite + Unpin>(
    wtr: &mut async_zip::write::ZipFileWriter<W>,
    name: String,
    users: impl Iterator<Item = &twitter_v2::User>,
    pinned_tweets: &[twitter_v2::Tweet],
) {
    tracing::debug!("starting entry");
    let opts = async_zip::ZipEntryBuilder::new(name, async_zip::Compression::Deflate)
        .unix_permissions(777);
    let entry_writer = wtr.write_entry_stream(opts).await.unwrap();
    let mut csv = csv_async::AsyncSerializer::from_writer(entry_writer);

    let pinned_tweets: HashMap<_, _> = pinned_tweets
        .iter()
        .map(|tweet| (tweet.id, tweet))
        .collect();

    for user in users {
        let url = user.url.as_deref().map(|url| {
            let url_entities = user
                .entities
                .as_ref()
                .and_then(|entities| entities.url.as_ref())
                .and_then(|url_entities| url_entities.urls.as_ref());

            if let Some(url_entities) = url_entities {
                url_entities.iter().fold(url.to_string(), |url, entity| {
                    if let Some(expanded_url) = entity.expanded_url.as_deref() {
                        url.replace(&entity.url, expanded_url)
                    } else {
                        url
                    }
                })
            } else {
                url.to_string()
            }
        });

        let bio = user.description.as_deref().map(|bio| {
            let url_entities = user
                .entities
                .as_ref()
                .and_then(|entities| entities.description.as_ref())
                .and_then(|description_entities| description_entities.urls.as_ref());

            if let Some(url_entities) = url_entities {
                url_entities.iter().fold(bio.to_string(), |bio, entity| {
                    if let Some(expanded_url) = entity.expanded_url.as_deref() {
                        bio.replace(&entity.url, expanded_url)
                    } else {
                        bio
                    }
                })
            } else {
                bio.to_string()
            }
        });

        let pinned_tweet = if let Some(pinned_tweet_id) = user.pinned_tweet_id {
            match pinned_tweets.get(&pinned_tweet_id) {
                Some(pinned_tweet) => {
                    let url_entities = pinned_tweet
                        .entities
                        .as_ref()
                        .and_then(|entities| entities.urls.as_ref());

                    if let Some(url_entities) = url_entities {
                        Some(url_entities.iter().fold(
                            pinned_tweet.text.to_string(),
                            |text, entity| {
                                if let Some(expanded_url) = entity.expanded_url.as_deref() {
                                    text.replace(&entity.url, expanded_url)
                                } else {
                                    text
                                }
                            },
                        ))
                    } else {
                        Some(pinned_tweet.text.to_string())
                    }
                }
                _ => None,
            }
        } else {
            None
        };

        let row = TwitterEntry {
            id: user.id.as_u64(),
            username: Some(&user.username),
            name: Some(&user.name),
            website: url,
            location: user.location.as_deref(),
            bio,
            pinned_tweet,
            profile_picture_url: user.profile_image_url.as_ref().map(|url| url.as_str()),
        };

        csv.serialize(row).await.unwrap();
    }

    let entry_writer = csv.into_inner().await.unwrap();
    entry_writer.close().await.unwrap();
    tracing::debug!("finished entry");
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
    tracking: String,
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
    track_opts: TrackOptions,
    page: FeedPage,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum FeedPage {
    Followers,
    Following,
}

impl FeedPage {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Followers => "follower",
            Self::Following => "following",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FeedQuery {
    page: Option<FeedPage>,
}

#[get("/feed")]
async fn feed(
    cx: web::Data<Context>,
    query: web::Query<FeedQuery>,
    sess: Session,
) -> Result<HttpResponse, actix_web::Error> {
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

    let track_opts = sqlx::query!(
        "SELECT track_followers, track_following FROM twitter_login WHERE twitter_account_id = $1",
        user_id
    )
    .map(|row| TrackOptions {
        followers: row.track_followers,
        following: row.track_following,
    })
    .fetch_one(&cx.pool)
    .await
    .map_err(actix_web::error::ErrorInternalServerError)?;

    let page = match query.page {
        Some(page) => page,
        None if track_opts.followers => FeedPage::Followers,
        None if track_opts.following => FeedPage::Following,
        None => FeedPage::Followers,
    };

    let events = sqlx::query!(
        "SELECT twitter_event.created_at, twitter_event.event_name, twitter_event.tracking, twitter_account.id, twitter_account.screen_name, twitter_account.display_name FROM twitter_event JOIN twitter_account ON twitter_account.id = twitter_event.related_twitter_account_id WHERE login_twitter_account_id = $1 AND tracking = $2 ORDER BY created_at DESC LIMIT 5000",
        user_id,
        page.as_str(),
    )
    .map(|row| TwitterEventEntry {
        tracking: row.tracking,
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
        track_opts,
        page,
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
        "SELECT created_at, account_count FROM twitter_state WHERE login_twitter_account_id = $1 ORDER BY created_at LIMIT 100",
        user_id
    )
    .map(|row| (row.created_at.timestamp(), row.account_count))
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
        let mut events = sqlx::query!("SELECT twitter_event.created_at, twitter_event.event_name, twitter_event.tracking, twitter_account.id, twitter_account.screen_name, twitter_account.display_name FROM twitter_event JOIN twitter_account ON twitter_account.id = twitter_event.related_twitter_account_id WHERE login_twitter_account_id = $1 ORDER BY created_at", user_id).map(|row| TwitterEventEntry {
            tracking: row.tracking,
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
            "{},{},{},{},{}\n",
            event.created_at.to_rfc3339(),
            event.tracking,
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
