use std::{io, sync::Arc};

use actix_web::{App, HttpServer, middleware::Logger, web};
use cognets_broker::{
    api, app::state::AppState, config::AppConfig, federation::queue::RedisEventQueue,
    persistence::mongo::MongoRepositories, services::federation,
};
use mongodb::Client;

#[actix_web::main]
/// Boots repositories, workers, and HTTP server.
async fn main() -> io::Result<()> {
    env_logger::init();

    let config = AppConfig::from_env();
    let mongo_client = Client::with_uri_str(&config.mongo_url)
        .await
        .map_err(io::Error::other)?;
    let db = mongo_client.database(&config.mongo_database);
    let repositories = MongoRepositories::new(&db)
        .await
        .map_err(io::Error::other)?;
    let queue =
        RedisEventQueue::new(&config.redis_url, &config.redis_stream).map_err(io::Error::other)?;

    queue
        .start_worker(repositories.clone(), config.clone())
        .await
        .map_err(io::Error::other)?;

    let queue = Arc::new(queue);
    let state = web::Data::new(
        AppState::new(config.clone(), db, repositories, queue).map_err(io::Error::other)?,
    );
    tokio::spawn(federation::start_swarm_sync_worker(state.get_ref().clone()));
    let bind_address = format!("{}:{}", config.host, config.port);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .configure(api::configure)
    })
    .bind(&bind_address)?
    .run()
    .await
}
