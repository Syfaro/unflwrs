use std::collections::{HashMap, HashSet};

use actix_session::{storage::CookieSessionStore, Session, SessionMiddleware};
use actix_web::{get, post, web, App, HttpResponse, HttpServer};
use askama::Template;
use bytes::Bytes;
use clap::Parser;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use twitter_v2::{
    authorization::{Oauth2Client, Oauth2Token, Scope},
    id::NumericId,
    oauth2::{AuthorizationCode, CsrfToken, PkceCodeChallenge, PkceCodeVerifier},
    prelude::{PaginableApiResponse, PaginationMeta},
    query::{ListField, TweetField, UserExpansion, UserField},
    TwitterApi,
};

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

    refresh_stale_accounts(pool.clone(), kp.clone()).await;

    tracing::info!("starting server");

    HttpServer::new(move || {
        let cx = Context {
            pool: pool.clone(),
            kp: kp.clone(),
            config: config.clone(),
            oauth2_client: oauth_client.clone(),
        };

        let key = actix_web::cookie::Key::from(config.session_secret.as_bytes());
        let session = SessionMiddleware::new(CookieSessionStore::default(), key);

        App::new()
            .wrap(session)
            .service(home)
            .service(twitter_login)
            .service(twitter_callback)
            .service(twitter_callback_v2)
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
    let is_oneoff = form.oneoff.as_deref().unwrap_or_default() == "yes";

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

    oneoff_v2(&cx.oauth2_client, token).await
}

#[tracing::instrument(skip(client, token))]
async fn oneoff_v2(
    client: &Oauth2Client,
    mut token: Oauth2Token,
) -> actix_web::Result<actix_web::HttpResponse> {
    tracing::info!("starting oneoff v2");

    client
        .refresh_token_if_expired(&mut token)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    let api = TwitterApi::new(token);

    let user_api = api
        .with_user_ctx()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut list_request = user_api.get_my_owned_lists();
    list_request
        .max_results(100)
        .list_fields([ListField::MemberCount]);

    let mut resp = list_request
        .send()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    let mut lists = Vec::new();
    if let Some(new_lists) = resp.data() {
        lists.extend_from_slice(new_lists);
    }

    while matches!(resp.meta(), Some(meta) if meta.next_token().is_some()) {
        resp = resp
            .next_page()
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?
            .ok_or_else(|| actix_web::error::ErrorInternalServerError("next page did not exist"))?;

        if let Some(new_lists) = resp.data() {
            lists.extend_from_slice(new_lists);
        }
    }

    let mut users: HashMap<NumericId, twitter_v2::User> = HashMap::new();

    let mut list_members: HashMap<NumericId, (String, Vec<NumericId>)> =
        HashMap::with_capacity(lists.len());
    let mut pinned_tweets: HashMap<NumericId, twitter_v2::Tweet> = HashMap::new();

    for list in lists {
        tracing::debug!(list_id = %list.id, "getting members in list");

        let mut member_request = api.get_list_members(list.id);
        member_request
            .max_results(100)
            .expansions([UserExpansion::PinnedTweetId])
            .user_fields([
                UserField::Id,
                UserField::Description,
                UserField::Entities,
                UserField::Location,
                UserField::Name,
                UserField::Url,
                UserField::Username,
                UserField::PinnedTweetId,
            ])
            .tweet_fields([TweetField::Entities, TweetField::Text]);

        let mut resp = member_request
            .send()
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?;

        if let Some(includes) = resp.includes() {
            if let Some(tweets) = &includes.tweets {
                pinned_tweets.extend(tweets.iter().map(|tweet| (tweet.id, tweet.clone())));
            }
        }

        let mut members = Vec::with_capacity(list.member_count.unwrap_or_default());

        if let Some(new_members) = resp.data() {
            members.extend(new_members.iter().map(|member| member.id));
            users.extend(new_members.iter().map(|member| (member.id, member.clone())));
        }

        while matches!(resp.meta(), Some(meta) if meta.next_token().is_some()) {
            resp = resp
                .next_page()
                .await
                .map_err(actix_web::error::ErrorInternalServerError)?
                .ok_or_else(|| {
                    actix_web::error::ErrorInternalServerError("next page did not exist")
                })?;

            if let Some(includes) = resp.includes() {
                if let Some(tweets) = &includes.tweets {
                    pinned_tweets.extend(tweets.iter().map(|tweet| (tweet.id, tweet.clone())));
                }
            }

            if let Some(new_members) = resp.data() {
                members.extend(new_members.iter().map(|member| member.id));
                users.extend(new_members.iter().map(|member| (member.id, member.clone())));
            }
        }

        tracing::debug!(list_id = %list.id, "found {} members in list", members.len());

        list_members.insert(list.id, (slug::slugify(list.name), members));
    }

    tracing::trace!("found {} users in lists", users.len());

    let mut following_req = user_api.get_my_following();
    following_req
        .max_results(1000)
        .expansions([UserExpansion::PinnedTweetId])
        .user_fields([
            UserField::Id,
            UserField::Description,
            UserField::Entities,
            UserField::Location,
            UserField::Name,
            UserField::Url,
            UserField::Username,
            UserField::PinnedTweetId,
        ])
        .tweet_fields([TweetField::Entities, TweetField::Text]);

    let mut resp = following_req
        .send()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    if let Some(includes) = resp.includes() {
        if let Some(tweets) = &includes.tweets {
            pinned_tweets.extend(tweets.iter().map(|tweet| (tweet.id, tweet.clone())));
        }
    }

    let mut following_ids = Vec::new();
    if let Some(new_following) = resp.data() {
        following_ids.extend(new_following.iter().map(|following| following.id));
        users.extend(
            new_following
                .iter()
                .map(|member| (member.id, member.clone())),
        );
    }

    while matches!(resp.meta(), Some(meta) if meta.next_token().is_some()) {
        resp = resp
            .next_page()
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?
            .ok_or_else(|| actix_web::error::ErrorInternalServerError("next page did not exist"))?;

        if let Some(includes) = resp.includes() {
            if let Some(tweets) = &includes.tweets {
                pinned_tweets.extend(tweets.iter().map(|tweet| (tweet.id, tweet.clone())));
            }
        }

        if let Some(new_following) = resp.data() {
            following_ids.extend(new_following.iter().map(|following| following.id));
            users.extend(
                new_following
                    .iter()
                    .map(|member| (member.id, member.clone())),
            );
        }
    }

    tracing::debug!("found {} following", following_ids.len());

    let mut follower_req = user_api.get_my_followers();
    follower_req
        .max_results(1000)
        .expansions([UserExpansion::PinnedTweetId])
        .user_fields([
            UserField::Id,
            UserField::Description,
            UserField::Entities,
            UserField::Location,
            UserField::Name,
            UserField::Url,
            UserField::Username,
            UserField::PinnedTweetId,
        ])
        .tweet_fields([TweetField::Entities, TweetField::Text]);

    let mut resp = follower_req
        .send()
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    if let Some(includes) = resp.includes() {
        if let Some(tweets) = &includes.tweets {
            pinned_tweets.extend(tweets.iter().map(|tweet| (tweet.id, tweet.clone())));
        }
    }

    let mut follower_ids = Vec::new();
    if let Some(new_follower) = resp.data() {
        follower_ids.extend(new_follower.iter().map(|follower| follower.id));
        users.extend(
            new_follower
                .iter()
                .map(|member| (member.id, member.clone())),
        );
    }

    while matches!(resp.meta(), Some(meta) if meta.next_token().is_some()) {
        resp = resp
            .next_page()
            .await
            .map_err(actix_web::error::ErrorInternalServerError)?
            .ok_or_else(|| actix_web::error::ErrorInternalServerError("next page did not exist"))?;

        if let Some(includes) = resp.includes() {
            if let Some(tweets) = &includes.tweets {
                pinned_tweets.extend(tweets.iter().map(|tweet| (tweet.id, tweet.clone())));
            }
        }

        if let Some(new_follower) = resp.data() {
            follower_ids.extend(new_follower.iter().map(|follower| follower.id));
            users.extend(
                new_follower
                    .iter()
                    .map(|member| (member.id, member.clone())),
            );
        }
    }

    tracing::debug!("found {} followers", follower_ids.len());

    tracing::debug!("discovered {} pinned tweets", pinned_tweets.len());

    let (mut wtr, rdr) = tokio::io::duplex(1024);

    tokio::task::spawn(async move {
        let mut wtr = async_zip::write::ZipFileWriter::new(&mut wtr);

        create_user_entry_v2(
            &mut wtr,
            "followers.csv".to_string(),
            &users,
            &pinned_tweets,
            &follower_ids,
        )
        .await;
        create_user_entry_v2(
            &mut wtr,
            "following.csv".to_string(),
            &users,
            &pinned_tweets,
            &following_ids,
        )
        .await;

        for (id, (slug, member_ids)) in list_members {
            create_user_entry_v2(
                &mut wtr,
                format!("list-{id}-{slug}.csv"),
                &users,
                &pinned_tweets,
                &member_ids,
            )
            .await;
        }

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
    screen_name: Option<&'a str>,
    website: Option<String>,
    location: Option<&'a str>,
    bio: Option<String>,
    pinned_tweet: Option<String>,
}

#[tracing::instrument(skip(wtr, users, ids))]
async fn create_user_entry_v2<W: tokio::io::AsyncWrite + Unpin>(
    wtr: &mut async_zip::write::ZipFileWriter<W>,
    name: String,
    users: &HashMap<NumericId, twitter_v2::User>,
    pinned_tweets: &HashMap<NumericId, twitter_v2::Tweet>,
    ids: &[NumericId],
) {
    let opts = async_zip::ZipEntryBuilder::new(name, async_zip::Compression::Deflate);
    let entry_writer = wtr.write_entry_stream(opts).await.unwrap();
    let mut csv = csv_async::AsyncSerializer::from_writer(entry_writer);

    for id in ids.iter().copied() {
        let row = match users.get(&id) {
            Some(user) => {
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

                TwitterEntry {
                    id: user.id.as_u64(),
                    screen_name: Some(&user.username),
                    website: url,
                    location: user.location.as_deref(),
                    bio,
                    pinned_tweet,
                }
            }
            None => {
                tracing::warn!("user was not loaded: {id}");

                TwitterEntry {
                    id: id.as_u64(),
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
