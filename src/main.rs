use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use actix_session::{storage::CookieSessionStore, Session, SessionMiddleware};
use actix_web::{get, post, web, App, HttpResponse, HttpServer};
use askama::Template;
use async_zip::ZipEntryBuilderExt;
use bytes::Bytes;
use clap::Parser;
use futures::{Future, StreamExt, TryFutureExt, TryStreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tracing::Instrument;
use twitter_v2::{
    authorization::{Oauth2Client, Oauth2Token, Scope},
    oauth2::{AuthorizationCode, CsrfToken, PkceCodeChallenge, PkceCodeVerifier},
    prelude::PaginableApiResponse,
    query::{ListField, TweetField, UserExpansion, UserField},
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
                (login_twitter_account_id, related_twitter_account_id, event_name)
            SELECT
                event.login_twitter_account_id,
                event.related_twitter_account_id,
                event.event
            FROM jsonb_to_recordset($1) AS
                event(login_twitter_account_id bigint, related_twitter_account_id bigint, event text)
            JOIN twitter_account
                ON twitter_account.id = event.login_twitter_account_id",
            data
        )
        .execute(&mut tx)
        .await?;
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
    let old_accounts = sqlx::query!("SELECT twitter_account_id, consumer_key, consumer_secret FROM twitter_login WHERE last_updated IS NULL OR last_updated < now() - interval '6 hours' AND error_count < 10 LIMIT 100").fetch_all(pool).await?;
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

#[derive(Deserialize)]
struct LoginForm {
    oneoff: Option<String>,
    use_json: Option<String>,
}

#[post("/twitter/login")]
async fn twitter_login(
    cx: web::Data<Context>,
    sess: Session,
    form: web::Form<LoginForm>,
) -> Result<actix_web::HttpResponse, AppError> {
    let is_oneoff = form.oneoff.as_deref().unwrap_or_default() == "yes";

    if matches!(&form.use_json, Some(val) if val == "on") {
        sess.insert("use-json", true)?;
    }

    let location = if is_oneoff {
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let (url, state) = cx.oauth2_client.auth_url(
            challenge,
            [
                Scope::TweetRead,
                Scope::UsersRead,
                Scope::FollowsRead,
                Scope::ListRead,
            ],
        );

        sess.insert("twitter-verifier", verifier)?;
        sess.insert("twitter-state", state)?;

        url.as_str().to_string()
    } else {
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
        "INSERT INTO twitter_login (twitter_account_id, consumer_key, consumer_secret) VALUES ($1, $2, $3) ON CONFLICT (twitter_account_id) DO UPDATE SET consumer_key = EXCLUDED.consumer_key, consumer_secret = EXCLUDED.consumer_secret, error_count = 0",
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

    sess.insert("twitter-v2-token", &token)?;
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

    let use_json = sess.get("use-json")?.unwrap_or(false);

    oneoff_v2(cx.redis.clone(), session_name, token, use_json).await
}

#[tracing::instrument(skip(redis, session_name, token), fields(user_id))]
async fn oneoff_v2(
    mut redis: redis::aio::ConnectionManager,
    session_name: String,
    token: Oauth2Token,
    use_json: bool,
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
            if let Err(err) = write_zips(&mut redis, wtr, api, user, &channel, use_json).await {
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

        if let Some(new_items) = resp.data() {
            items.extend_from_slice(new_items);
        }

        if let Some(new_expansions) = resp.includes() {
            merge_expansions(&mut expansions, new_expansions);
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

async fn write_zips<W, A>(
    client: &mut redis::aio::ConnectionManager,
    wtr: W,
    api: twitter_v2::TwitterApi<A>,
    user: twitter_v2::User,
    channel: &str,
    use_json: bool,
) -> Result<(), String>
where
    W: tokio::io::AsyncWrite + Unpin,
    A: Send + Sync + twitter_v2::Authorization,
{
    tracing::debug!("starting zip writer");
    let mut wtr = async_zip::write::ZipFileWriter::new(wtr);

    let follower_count = user
        .public_metrics
        .as_ref()
        .map(|metrics| metrics.followers_count)
        .unwrap_or_default();
    let mut follower_ids = HashSet::with_capacity(follower_count);

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
                UserField::Protected,
                UserField::Url,
                UserField::Username,
            ])
            .tweet_fields([TweetField::Entities, TweetField::Text]);

        let (followers, expansions) = fetch_all(|| follower_req.send())
            .await
            .map_err(|err| err.to_string())?;

        follower_ids.extend(followers.iter().map(|user| user.id));

        if use_json {
            create_user_entry_json(
                &mut wtr,
                "followers.json".to_string(),
                followers.iter(),
                &expansions.tweets.unwrap_or_default(),
            )
            .await;
        } else {
            create_user_entry_v2(
                &mut wtr,
                "followers.csv".to_string(),
                followers.iter(),
                &expansions.tweets.unwrap_or_default(),
            )
            .await;
        }
    }

    if user
        .public_metrics
        .as_ref()
        .map(|metrics| metrics.following_count)
        .unwrap_or_default()
        > 15_000
    {
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
                UserField::Protected,
                UserField::Url,
                UserField::Username,
            ])
            .tweet_fields([TweetField::Entities, TweetField::Text]);

        let (following, expansions) = fetch_all(|| following_req.send())
            .await
            .map_err(|err| err.to_string())?;

        let pinned_tweets = expansions.tweets.unwrap_or_default();

        if use_json {
            create_user_entry_json(
                &mut wtr,
                "following.json".to_string(),
                following.iter(),
                &pinned_tweets,
            )
            .await;
        } else {
            create_user_entry_v2(
                &mut wtr,
                "following.csv".to_string(),
                following.iter(),
                &pinned_tweets,
            )
            .await;

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

    tracing::debug!("loading lists");
    let mut list_request = api.get_user_owned_lists(user.id);
    list_request
        .max_results(100)
        .list_fields([ListField::MemberCount]);

    let (lists, _expansions) = fetch_all(|| list_request.send())
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
                UserField::Protected,
                UserField::Url,
                UserField::Username,
            ])
            .tweet_fields([TweetField::Entities, TweetField::Text]);

        let (members, expansions) = fetch_all(|| member_request.send())
            .await
            .map_err(|err| err.to_string())?;

        if use_json {
            create_user_entry_json(
                &mut wtr,
                format!("list-{}-{}.json", list.id, slug::slugify(list.name)),
                members.iter(),
                &expansions.tweets.unwrap_or_default(),
            )
            .await;
        } else {
            create_user_entry_v2(
                &mut wtr,
                format!("list-{}-{}.csv", list.id, slug::slugify(list.name)),
                members.iter(),
                &expansions.tweets.unwrap_or_default(),
            )
            .await;
        }
    }

    wtr.close().await.unwrap();
    tracing::info!("finished writing zip");

    publish_event(client, channel, ExportEvent::Completed).await;

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
        .into_iter()
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
        .into_iter()
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
