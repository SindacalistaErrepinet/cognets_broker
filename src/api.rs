use actix_web::{
    HttpRequest, HttpResponse,
    http::{StatusCode, header},
    web,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    app::state::AppState,
    context::headers::{HEADER_RESULTS_COUNT, HEADER_TENANT, RequestContext},
    domain::{
        batch::{BatchOperationResult, UpdateResult},
        discovery::{AttributeInfo, AttributeList, EntityTypeInfo, EntityTypeList},
        temporal::TemporalQueryResult,
        types::{
            ContextSourceIdentity, StoredDocument, SubscriptionDocument, SwarmMutationDocument,
            TemporalEntityDocument,
        },
    },
    error::{BrokerError, ProblemDetails},
    federation::p2p::SwimEventEnvelope,
    query::types::{
        AppendAttrsQuery, AttributeNamePath, BatchUpdateQuery, DeleteAttrQuery, DiscoveryQuery,
        EntityAttrInstancePath, EntityAttrPath, EntityIdPath, EntityQuery, EntityTypeQuery,
        LocalOnlyQuery, SubscriptionIdPath, SubscriptionQuery, TemporalEntityQuery,
        TemporalEntityQueryParams, TypeNamePath, UpsertBatchQuery,
    },
    services::{
        common::{representation_from_request, resource_location, response_content_type},
        discovery, entities, federation as federation_service, info, subscriptions, temporal,
    },
};

const NGSI_LD_CORE_CONTEXT_V1_8: &str =
    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.8.jsonld";

#[derive(Debug, Deserialize, Serialize, ToSchema)]
struct JsonBody {
    #[schema(value_type = Value)]
    value: Value,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
struct JsonArrayBody {
    #[schema(value_type = Vec<Value>)]
    value: Vec<Value>,
}

#[derive(Debug, Serialize, ToSchema)]
struct JsonResponseBody {
    #[schema(value_type = Value)]
    value: Value,
}

#[derive(Debug, Serialize, ToSchema)]
struct StringListResponse {
    value: Vec<String>,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
struct SwarmMutationListQuery {
    #[serde(rename = "sinceNanos")]
    since_nanos: Option<i64>,
    limit: Option<usize>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        create_entity,
        query_entities,
        post_query_entities,
        retrieve_entity,
        delete_entity,
        merge_entity,
        replace_entity,
        append_attributes,
        update_attributes,
        patch_attribute,
        delete_attribute,
        replace_attribute,
        create_batch,
        upsert_batch,
        update_batch,
        merge_batch,
        delete_batch,
        create_subscription,
        query_subscriptions,
        retrieve_subscription,
        update_subscription,
        delete_subscription,
        post_internal_swim,
        list_internal_swim_mutations,
        upsert_temporal_entity,
        query_temporal_entities,
        post_query_temporal_entities,
        retrieve_temporal_entity,
        delete_temporal_entity,
        append_temporal_attrs,
        retrieve_temporal_attrs,
        retrieve_temporal_attr,
        retrieve_temporal_instance,
        delete_temporal_attr,
        patch_temporal_instance,
        delete_temporal_instance,
        list_entity_types,
        get_entity_type,
        list_attributes,
        get_attribute,
        get_source_identity,
    ),
    components(schemas(
        ProblemDetails,
        BatchOperationResult,
        UpdateResult,
        TemporalQueryResult,
        StoredDocument,
        TemporalEntityDocument,
        SubscriptionDocument,
        SwarmMutationDocument,
        SwimEventEnvelope,
        EntityTypeList,
        EntityTypeInfo,
        AttributeList,
        AttributeInfo,
        ContextSourceIdentity,
        EntityQuery,
        EntityTypeQuery,
        LocalOnlyQuery,
        AppendAttrsQuery,
        DeleteAttrQuery,
        UpsertBatchQuery,
        BatchUpdateQuery,
        SubscriptionQuery,
        DiscoveryQuery,
        SwarmMutationListQuery,
        TemporalEntityQuery,
        TemporalEntityQueryParams,
        JsonBody,
        JsonArrayBody,
        JsonResponseBody,
        StringListResponse,
    )),
    tags(
        (name = "entities", description = "Entity CRUD and batch endpoints"),
        (name = "subscriptions", description = "Subscription CRUD endpoints"),
        (name = "temporal", description = "Temporal entity endpoints"),
        (name = "discovery", description = "Entity type and attribute discovery endpoints"),
        (name = "info", description = "Context source identity endpoints"),
        (name = "internal", description = "Internal SWIM and swarm sync endpoints")
    )
)]
pub struct ApiDoc;

/// Registers public NGSI-LD routes and root-level internal routes.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-docs/openapi.json", ApiDoc::openapi()),
    )
    .service(
        web::scope("/ngsi-ld/v1")
            .route("/entities", web::post().to(create_entity))
            .route("/entities", web::get().to(query_entities))
            .route("/entities/{entity_id}", web::get().to(retrieve_entity))
            .route("/entities/{entity_id}", web::delete().to(delete_entity))
            .route("/entities/{entity_id}", web::patch().to(merge_entity))
            .route("/entities/{entity_id}", web::put().to(replace_entity))
            .route(
                "/entities/{entity_id}/attrs",
                web::post().to(append_attributes),
            )
            .route(
                "/entities/{entity_id}/attrs",
                web::patch().to(update_attributes),
            )
            .route(
                "/entities/{entity_id}/attrs/{attr_id}",
                web::patch().to(patch_attribute),
            )
            .route(
                "/entities/{entity_id}/attrs/{attr_id}",
                web::delete().to(delete_attribute),
            )
            .route(
                "/entities/{entity_id}/attrs/{attr_id}",
                web::put().to(replace_attribute),
            )
            .route("/entityOperations/create", web::post().to(create_batch))
            .route("/entityOperations/upsert", web::post().to(upsert_batch))
            .route("/entityOperations/update", web::post().to(update_batch))
            .route("/entityOperations/merge", web::post().to(merge_batch))
            .route("/entityOperations/delete", web::post().to(delete_batch))
            .route(
                "/entityOperations/query",
                web::post().to(post_query_entities),
            )
            .route("/subscriptions", web::post().to(create_subscription))
            .route("/subscriptions", web::get().to(query_subscriptions))
            .route(
                "/subscriptions/{subscription_id}",
                web::get().to(retrieve_subscription),
            )
            .route(
                "/subscriptions/{subscription_id}",
                web::patch().to(update_subscription),
            )
            .route(
                "/subscriptions/{subscription_id}",
                web::delete().to(delete_subscription),
            )
            .route("/temporal/entities", web::post().to(upsert_temporal_entity))
            .route("/temporal/entities", web::get().to(query_temporal_entities))
            .route(
                "/temporal/entities/{entity_id}",
                web::get().to(retrieve_temporal_entity),
            )
            .route(
                "/temporal/entities/{entity_id}",
                web::delete().to(delete_temporal_entity),
            )
            .route(
                "/temporal/entities/{entity_id}/attrs",
                web::get().to(retrieve_temporal_attrs),
            )
            .route(
                "/temporal/entities/{entity_id}/attrs",
                web::post().to(append_temporal_attrs),
            )
            .route(
                "/temporal/entities/{entity_id}/attrs/{attr_id}",
                web::get().to(retrieve_temporal_attr),
            )
            .route(
                "/temporal/entities/{entity_id}/attrs/{attr_id}",
                web::delete().to(delete_temporal_attr),
            )
            .route(
                "/temporal/entities/{entity_id}/attrs/{attr_id}/{instance_id}",
                web::get().to(retrieve_temporal_instance),
            )
            .route(
                "/temporal/entities/{entity_id}/attrs/{attr_id}/{instance_id}",
                web::patch().to(patch_temporal_instance),
            )
            .route(
                "/temporal/entities/{entity_id}/attrs/{attr_id}/{instance_id}",
                web::delete().to(delete_temporal_instance),
            )
            .route(
                "/temporal/entityOperations/query",
                web::post().to(post_query_temporal_entities),
            )
            .route("/types", web::get().to(list_entity_types))
            .route("/types/{type}", web::get().to(get_entity_type))
            .route("/attributes", web::get().to(list_attributes))
            .route("/attributes/{attrId}", web::get().to(get_attribute))
            .route("/info/sourceIdentity", web::get().to(get_source_identity)),
    )
    .route("/internal/swim", web::post().to(post_internal_swim))
    .route(
        "/internal/swim/mutations",
        web::get().to(list_internal_swim_mutations),
    );
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/entities",
    tag = "entities",
    params(EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 201, description = "Entity created"),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 409, description = "Entity already exists", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles entity creation requests.
async fn create_entity(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let entity_id = entities::create(
        state.get_ref(),
        &context,
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    Ok(HttpResponse::Created()
        .insert_header((HEADER_TENANT, context.tenant))
        .insert_header((header::LOCATION, resource_location("entities", &entity_id)))
        .finish())
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/entities",
    tag = "entities",
    params(EntityQuery),
    responses(
        (status = 200, description = "Entities returned", body = Value),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles entity list queries.
async fn query_entities(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<EntityQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let representation =
        representation_from_request(&request, query.format.as_deref(), query.options.as_deref());
    let result = entities::query(state.get_ref(), &request, &context, &query).await?;
    let mut response = HttpResponse::Ok();
    response.insert_header((HEADER_TENANT, context.tenant));
    response.insert_header((
        header::CONTENT_TYPE,
        response_content_type(&request, representation),
    ));
    if query.count.unwrap_or(false) {
        response.insert_header((HEADER_RESULTS_COUNT, result.total_count.to_string()));
    }
    Ok(response.json(result.body))
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/entityOperations/query",
    tag = "entities",
    params(SubscriptionQuery),
    request_body = EntityQuery,
    responses(
        (status = 200, description = "Entities returned", body = Value),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles POST-based entity queries.
async fn post_query_entities(
    state: web::Data<AppState>,
    request: HttpRequest,
    body: web::Json<EntityQuery>,
    count: web::Query<SubscriptionQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = body.into_inner();
    let representation =
        representation_from_request(&request, query.format.as_deref(), query.options.as_deref());
    let result = entities::query(state.get_ref(), &request, &context, &query).await?;
    let mut response = HttpResponse::Ok();
    response.insert_header((HEADER_TENANT, context.tenant));
    response.insert_header((
        header::CONTENT_TYPE,
        response_content_type(&request, representation),
    ));
    if count.count.unwrap_or(false) || query.count.unwrap_or(false) {
        response.insert_header((HEADER_RESULTS_COUNT, result.total_count.to_string()));
    }
    Ok(response.json(result.body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/entities/{entity_id}",
    tag = "entities",
    params(EntityIdPath, EntityQuery),
    responses(
        (status = 200, description = "Entity returned", body = Value),
        (status = 404, description = "Entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single entity retrieval requests.
async fn retrieve_entity(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<EntityQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let representation =
        representation_from_request(&request, query.format.as_deref(), query.options.as_deref());
    let entity = entities::get(
        state.get_ref(),
        &request,
        &context,
        &path.into_inner(),
        &query,
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .insert_header((
            header::CONTENT_TYPE,
            response_content_type(&request, representation),
        ))
        .json(entity))
}

#[utoipa::path(
    delete,
    path = "/ngsi-ld/v1/entities/{entity_id}",
    tag = "entities",
    params(EntityIdPath, EntityTypeQuery),
    responses(
        (status = 204, description = "Entity deleted"),
        (status = 404, description = "Entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles entity deletion requests.
async fn delete_entity(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<EntityTypeQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    entities::delete(
        state.get_ref(),
        &context,
        &path.into_inner(),
        query.entity_type.as_deref(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    patch,
    path = "/ngsi-ld/v1/entities/{entity_id}",
    tag = "entities",
    params(EntityIdPath, EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 204, description = "Entity merged"),
        (status = 404, description = "Entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles entity merge-patch requests.
async fn merge_entity(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    entities::merge(
        state.get_ref(),
        &context,
        &path.into_inner(),
        query.entity_type.as_deref(),
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    put,
    path = "/ngsi-ld/v1/entities/{entity_id}",
    tag = "entities",
    params(EntityIdPath, EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 204, description = "Entity replaced"),
        (status = 404, description = "Entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles full entity replacement requests.
async fn replace_entity(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    entities::replace(
        state.get_ref(),
        &context,
        &path.into_inner(),
        query.entity_type.as_deref(),
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/entities/{entity_id}/attrs",
    tag = "entities",
    params(EntityIdPath, AppendAttrsQuery),
    request_body = Value,
    responses(
        (status = 204, description = "Attributes appended"),
        (status = 207, description = "Partial append result", body = UpdateResult),
        (status = 404, description = "Entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles attribute append requests.
async fn append_attributes(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<AppendAttrsQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let result = entities::append_attrs(
        state.get_ref(),
        &context,
        &path.into_inner(),
        query.entity_type.as_deref(),
        body.into_inner(),
        query
            .options
            .as_deref()
            .map(|options| options.contains("noOverwrite"))
            .unwrap_or(false),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    if result.not_updated.is_empty() {
        Ok(HttpResponse::NoContent()
            .insert_header((HEADER_TENANT, context.tenant))
            .finish())
    } else {
        Ok(HttpResponse::build(StatusCode::MULTI_STATUS)
            .insert_header((HEADER_TENANT, context.tenant))
            .json(result))
    }
}

#[utoipa::path(
    patch,
    path = "/ngsi-ld/v1/entities/{entity_id}/attrs",
    tag = "entities",
    params(EntityIdPath, EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 204, description = "Attributes updated"),
        (status = 207, description = "Partial update result", body = UpdateResult),
        (status = 404, description = "Entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles multi-attribute update requests.
async fn update_attributes(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let result = entities::update_attrs(
        state.get_ref(),
        &context,
        &path.into_inner(),
        query.entity_type.as_deref(),
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    if result.not_updated.is_empty() {
        Ok(HttpResponse::NoContent()
            .insert_header((HEADER_TENANT, context.tenant))
            .finish())
    } else {
        Ok(HttpResponse::build(StatusCode::MULTI_STATUS)
            .insert_header((HEADER_TENANT, context.tenant))
            .json(result))
    }
}

#[utoipa::path(
    patch,
    path = "/ngsi-ld/v1/entities/{entity_id}/attrs/{attr_id}",
    tag = "entities",
    params(EntityAttrPath, EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 204, description = "Attribute patched"),
        (status = 404, description = "Entity or attribute not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single attribute patch requests.
async fn patch_attribute(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<(String, String)>,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let (entity_id, attr_id) = path.into_inner();
    entities::patch_attr(
        state.get_ref(),
        &context,
        &entity_id,
        &attr_id,
        query.entity_type.as_deref(),
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    delete,
    path = "/ngsi-ld/v1/entities/{entity_id}/attrs/{attr_id}",
    tag = "entities",
    params(EntityAttrPath, DeleteAttrQuery),
    responses(
        (status = 204, description = "Attribute deleted"),
        (status = 404, description = "Entity or attribute not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single attribute deletion requests.
async fn delete_attribute(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<(String, String)>,
    query: web::Query<DeleteAttrQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let (entity_id, attr_id) = path.into_inner();
    let _ = (&query.delete_all, &query.dataset_id);
    entities::delete_attr(
        state.get_ref(),
        &context,
        &entity_id,
        &attr_id,
        query.entity_type.as_deref(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    put,
    path = "/ngsi-ld/v1/entities/{entity_id}/attrs/{attr_id}",
    tag = "entities",
    params(EntityAttrPath, EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 204, description = "Attribute replaced"),
        (status = 404, description = "Entity or attribute not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single attribute replacement requests.
async fn replace_attribute(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<(String, String)>,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let (entity_id, attr_id) = path.into_inner();
    entities::replace_attr(
        state.get_ref(),
        &context,
        &entity_id,
        &attr_id,
        query.entity_type.as_deref(),
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/entityOperations/create",
    tag = "entities",
    params(EntityTypeQuery),
    request_body = Vec<Value>,
    responses(
        (status = 201, description = "Entities created", body = Vec<String>),
        (status = 207, description = "Partial batch result", body = BatchOperationResult),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles batch entity create requests.
async fn create_batch(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    match entities::batch_create(
        state.get_ref(),
        &context,
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?
    {
        Ok(ids) => Ok(HttpResponse::Created()
            .insert_header((HEADER_TENANT, context.tenant))
            .json(ids)),
        Err(result) => Ok(HttpResponse::build(StatusCode::MULTI_STATUS)
            .insert_header((HEADER_TENANT, context.tenant))
            .json(result)),
    }
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/entityOperations/upsert",
    tag = "entities",
    params(UpsertBatchQuery),
    request_body = Vec<Value>,
    responses(
        (status = 201, description = "Entities upserted with creations", body = Vec<String>),
        (status = 204, description = "Entities updated without creations"),
        (status = 207, description = "Partial batch result", body = BatchOperationResult),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles batch entity upsert requests.
async fn upsert_batch(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<UpsertBatchQuery>,
    body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    match entities::batch_upsert(
        state.get_ref(),
        &context,
        body.into_inner(),
        query.options.as_deref() == Some("update"),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?
    {
        Ok(outcome) if outcome.created_ids.is_empty() && outcome.had_updates => {
            Ok(HttpResponse::NoContent()
                .insert_header((HEADER_TENANT, context.tenant))
                .finish())
        }
        Ok(outcome) => Ok(HttpResponse::Created()
            .insert_header((HEADER_TENANT, context.tenant))
            .json(outcome.created_ids)),
        Err(result) => Ok(HttpResponse::build(StatusCode::MULTI_STATUS)
            .insert_header((HEADER_TENANT, context.tenant))
            .json(result)),
    }
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/entityOperations/update",
    tag = "entities",
    params(BatchUpdateQuery),
    request_body = Vec<Value>,
    responses(
        (status = 204, description = "Batch updated"),
        (status = 207, description = "Partial batch result", body = BatchOperationResult),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles batch entity update requests.
async fn update_batch(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<BatchUpdateQuery>,
    body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    match entities::batch_update(
        state.get_ref(),
        &context,
        body.into_inner(),
        query
            .options
            .as_deref()
            .map(|options| options.contains("noOverwrite"))
            .unwrap_or(false),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?
    {
        Ok(()) => Ok(HttpResponse::NoContent()
            .insert_header((HEADER_TENANT, context.tenant))
            .finish()),
        Err(result) => Ok(HttpResponse::build(StatusCode::MULTI_STATUS)
            .insert_header((HEADER_TENANT, context.tenant))
            .json(result)),
    }
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/entityOperations/merge",
    tag = "entities",
    params(LocalOnlyQuery),
    request_body = Vec<Value>,
    responses(
        (status = 204, description = "Batch merged"),
        (status = 207, description = "Partial batch result", body = BatchOperationResult),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles batch entity merge requests.
async fn merge_batch(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<LocalOnlyQuery>,
    body: web::Json<Vec<Value>>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    match entities::batch_merge(
        state.get_ref(),
        &context,
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?
    {
        Ok(()) => Ok(HttpResponse::NoContent()
            .insert_header((HEADER_TENANT, context.tenant))
            .finish()),
        Err(result) => Ok(HttpResponse::build(StatusCode::MULTI_STATUS)
            .insert_header((HEADER_TENANT, context.tenant))
            .json(result)),
    }
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/entityOperations/delete",
    tag = "entities",
    params(EntityTypeQuery),
    request_body = Vec<String>,
    responses(
        (status = 204, description = "Batch deleted"),
        (status = 207, description = "Partial batch result", body = BatchOperationResult),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles batch entity delete requests.
async fn delete_batch(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Vec<String>>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    match entities::batch_delete(
        state.get_ref(),
        &context,
        body.into_inner(),
        query.local.unwrap_or(false),
        normalize_query_string(request.query_string()),
    )
    .await?
    {
        Ok(()) => Ok(HttpResponse::NoContent()
            .insert_header((HEADER_TENANT, context.tenant))
            .finish()),
        Err(result) => Ok(HttpResponse::build(StatusCode::MULTI_STATUS)
            .insert_header((HEADER_TENANT, context.tenant))
            .json(result)),
    }
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/subscriptions",
    tag = "subscriptions",
    request_body = Value,
    responses(
        (status = 201, description = "Subscription created"),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles subscription creation requests.
async fn create_subscription(
    state: web::Data<AppState>,
    request: HttpRequest,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let subscription_id =
        subscriptions::create(state.get_ref(), &context, body.into_inner()).await?;
    Ok(HttpResponse::Created()
        .insert_header((HEADER_TENANT, context.tenant))
        .insert_header((
            header::LOCATION,
            resource_location("subscriptions", &subscription_id),
        ))
        .finish())
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/subscriptions",
    tag = "subscriptions",
    params(SubscriptionQuery),
    responses(
        (status = 200, description = "Subscriptions returned", body = Value),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles subscription list requests.
async fn query_subscriptions(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<SubscriptionQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let result = subscriptions::list(state.get_ref(), &context, &query).await?;
    let mut response = HttpResponse::Ok();
    response.insert_header((HEADER_TENANT, context.tenant));
    if query.count.unwrap_or(false) {
        response.insert_header((HEADER_RESULTS_COUNT, result.total_count.to_string()));
    }
    Ok(response.json(result.body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/subscriptions/{subscription_id}",
    tag = "subscriptions",
    params(SubscriptionIdPath),
    responses(
        (status = 200, description = "Subscription returned", body = Value),
        (status = 404, description = "Subscription not found", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single subscription retrieval requests.
async fn retrieve_subscription(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let body = subscriptions::get(state.get_ref(), &context, &path.into_inner()).await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(body))
}

#[utoipa::path(
    patch,
    path = "/ngsi-ld/v1/subscriptions/{subscription_id}",
    tag = "subscriptions",
    params(SubscriptionIdPath),
    request_body = Value,
    responses(
        (status = 204, description = "Subscription updated"),
        (status = 404, description = "Subscription not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles subscription patch requests.
async fn update_subscription(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    subscriptions::patch(
        state.get_ref(),
        &context,
        &path.into_inner(),
        body.into_inner(),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    delete,
    path = "/ngsi-ld/v1/subscriptions/{subscription_id}",
    tag = "subscriptions",
    params(SubscriptionIdPath),
    responses(
        (status = 204, description = "Subscription deleted"),
        (status = 404, description = "Subscription not found", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles subscription deletion requests.
async fn delete_subscription(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    subscriptions::delete(state.get_ref(), &context, &path.into_inner()).await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    get,
    path = "/internal/swim/mutations",
    tag = "internal",
    params(SwarmMutationListQuery),
    responses(
        (status = 200, description = "Swarm mutations returned", body = Vec<SwarmMutationDocument>),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles root-level internal mutation log reads.
async fn list_internal_swim_mutations(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<SwarmMutationListQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let mutations = federation_service::list_swarm_mutations_since(
        state.get_ref(),
        &context.tenant,
        query.since_nanos,
        query.limit.unwrap_or(200).min(1_000),
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(mutations))
}

#[utoipa::path(
    post,
    path = "/internal/swim",
    tag = "internal",
    request_body = SwimEventEnvelope,
    responses(
        (status = 204, description = "SWIM event accepted"),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles root-level inbound SWIM events from peers.
async fn post_internal_swim(
    state: web::Data<AppState>,
    request: HttpRequest,
    body: web::Json<SwimEventEnvelope>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    federation_service::accept_swim_event(state.get_ref(), &context.tenant, &body.into_inner())
        .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/temporal/entities",
    tag = "temporal",
    params(EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 201, description = "Temporal entity created"),
        (status = 204, description = "Temporal entity updated"),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal entity upsert requests.
async fn upsert_temporal_entity(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let (entity_id, created) = temporal::upsert(
        state.get_ref(),
        &context,
        body.into_inner(),
        query.local.unwrap_or(false),
    )
    .await?;
    let mut response = if created {
        HttpResponse::Created()
    } else {
        HttpResponse::NoContent()
    };
    response.insert_header((HEADER_TENANT, context.tenant));
    if created {
        response.insert_header((
            header::LOCATION,
            resource_location("temporal/entities", &entity_id),
        ));
    }
    Ok(response.finish())
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/temporal/entities",
    tag = "temporal",
    params(TemporalEntityQueryParams),
    responses(
        (status = 200, description = "Temporal entities returned", body = Value),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal entity list queries.
async fn query_temporal_entities(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<TemporalEntityQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let representation = representation_from_request(
        &request,
        query.entity.format.as_deref(),
        query.entity.options.as_deref(),
    );
    let result = temporal::query(state.get_ref(), &context, &request, &query).await?;
    let mut response = HttpResponse::Ok();
    response.insert_header((HEADER_TENANT, context.tenant));
    response.insert_header((
        header::CONTENT_TYPE,
        response_content_type(&request, representation),
    ));
    if query.entity.count.unwrap_or(false) {
        response.insert_header((HEADER_RESULTS_COUNT, result.total_count.to_string()));
    }
    Ok(response.json(result.body))
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/temporal/entityOperations/query",
    tag = "temporal",
    request_body = TemporalEntityQuery,
    responses(
        (status = 200, description = "Temporal entities returned", body = Value),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles POST-based temporal queries.
async fn post_query_temporal_entities(
    state: web::Data<AppState>,
    request: HttpRequest,
    body: web::Json<TemporalEntityQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = body.into_inner();
    let representation = representation_from_request(
        &request,
        query.entity.format.as_deref(),
        query.entity.options.as_deref(),
    );
    let result = temporal::query(state.get_ref(), &context, &request, &query).await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .insert_header((
            header::CONTENT_TYPE,
            response_content_type(&request, representation),
        ))
        .json(result.body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}",
    tag = "temporal",
    params(EntityIdPath, TemporalEntityQueryParams),
    responses(
        (status = 200, description = "Temporal entity returned", body = Value),
        (status = 404, description = "Temporal entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single temporal entity retrieval requests.
async fn retrieve_temporal_entity(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<TemporalEntityQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let representation = representation_from_request(
        &request,
        query.entity.format.as_deref(),
        query.entity.options.as_deref(),
    );
    let body = temporal::get(
        state.get_ref(),
        &context,
        &request,
        &path.into_inner(),
        &query,
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .insert_header((
            header::CONTENT_TYPE,
            response_content_type(&request, representation),
        ))
        .json(body))
}

#[utoipa::path(
    delete,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}",
    tag = "temporal",
    params(EntityIdPath, EntityTypeQuery),
    responses(
        (status = 204, description = "Temporal entity deleted"),
        (status = 404, description = "Temporal entity not found", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal entity deletion requests.
async fn delete_temporal_entity(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<EntityTypeQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    temporal::delete_entity(
        state.get_ref(),
        &context,
        &path.into_inner(),
        query.local.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    post,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}/attrs",
    tag = "temporal",
    params(EntityIdPath, EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 204, description = "Temporal attributes appended"),
        (status = 404, description = "Temporal entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal attribute append requests.
async fn append_temporal_attrs(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    temporal::append_attrs(
        state.get_ref(),
        &context,
        &path.into_inner(),
        body.into_inner(),
        query.local.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}/attrs",
    tag = "temporal",
    params(EntityIdPath, TemporalEntityQueryParams),
    responses(
        (status = 200, description = "Temporal attributes returned", body = Value),
        (status = 404, description = "Temporal entity not found", body = ProblemDetails),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal attribute collection retrieval requests.
async fn retrieve_temporal_attrs(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<TemporalEntityQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let body = temporal::get_attrs(
        state.get_ref(),
        &context,
        &request,
        &path.into_inner(),
        &query,
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}/attrs/{attr_id}",
    tag = "temporal",
    params(EntityAttrPath, TemporalEntityQueryParams),
    responses(
        (status = 200, description = "Temporal attribute returned", body = Value),
        (status = 404, description = "Temporal entity or attribute not found", body = ProblemDetails),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single temporal attribute retrieval requests.
async fn retrieve_temporal_attr(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<(String, String)>,
    query: web::Query<TemporalEntityQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let (entity_id, attr_id) = path.into_inner();
    let body = temporal::get_attr(
        state.get_ref(),
        &context,
        &request,
        &entity_id,
        &attr_id,
        &query,
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}/attrs/{attr_id}/{instance_id}",
    tag = "temporal",
    params(EntityAttrInstancePath, TemporalEntityQueryParams),
    responses(
        (status = 200, description = "Temporal instance returned", body = Value),
        (status = 404, description = "Temporal instance not found", body = ProblemDetails),
        (status = 400, description = "Invalid query", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal attribute instance retrieval requests.
async fn retrieve_temporal_instance(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<(String, String, String)>,
    query: web::Query<TemporalEntityQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let (entity_id, attr_id, instance_id) = path.into_inner();
    let body = temporal::get_attr_instance(
        state.get_ref(),
        &context,
        &entity_id,
        &attr_id,
        &instance_id,
        &query,
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(body))
}

#[utoipa::path(
    delete,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}/attrs/{attr_id}",
    tag = "temporal",
    params(EntityAttrPath, EntityTypeQuery),
    responses(
        (status = 204, description = "Temporal attribute deleted"),
        (status = 404, description = "Temporal entity or attribute not found", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal attribute deletion requests.
async fn delete_temporal_attr(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<(String, String)>,
    query: web::Query<EntityTypeQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let (entity_id, attr_id) = path.into_inner();
    temporal::delete_attr(
        state.get_ref(),
        &context,
        &entity_id,
        &attr_id,
        query.local.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    patch,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}/attrs/{attr_id}/{instance_id}",
    tag = "temporal",
    params(EntityAttrInstancePath, EntityTypeQuery),
    request_body = Value,
    responses(
        (status = 204, description = "Temporal instance patched"),
        (status = 404, description = "Temporal instance not found", body = ProblemDetails),
        (status = 400, description = "Invalid request", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal attribute instance patch requests.
async fn patch_temporal_instance(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<(String, String, String)>,
    query: web::Query<EntityTypeQuery>,
    body: web::Json<Value>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let (entity_id, attr_id, instance_id) = path.into_inner();
    temporal::patch_instance(
        state.get_ref(),
        &context,
        &entity_id,
        &attr_id,
        &instance_id,
        body.into_inner(),
        query.local.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    delete,
    path = "/ngsi-ld/v1/temporal/entities/{entity_id}/attrs/{attr_id}/{instance_id}",
    tag = "temporal",
    params(EntityAttrInstancePath, EntityTypeQuery),
    responses(
        (status = 204, description = "Temporal instance deleted"),
        (status = 404, description = "Temporal instance not found", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles temporal attribute instance deletion requests.
async fn delete_temporal_instance(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<(String, String, String)>,
    query: web::Query<EntityTypeQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let (entity_id, attr_id, instance_id) = path.into_inner();
    temporal::delete_instance(
        state.get_ref(),
        &context,
        &entity_id,
        &attr_id,
        &instance_id,
        query.local.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::NoContent()
        .insert_header((HEADER_TENANT, context.tenant))
        .finish())
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/types",
    tag = "discovery",
    params(DiscoveryQuery),
    responses(
        (status = 200, description = "Entity type list or details returned", body = Value),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles entity type discovery requests.
async fn list_entity_types(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<DiscoveryQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let body = discovery::list_types(
        state.get_ref(),
        &request,
        &context,
        query.local.unwrap_or(false),
        query.details.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/types/{type}",
    tag = "discovery",
    params(TypeNamePath, DiscoveryQuery),
    responses(
        (status = 200, description = "Entity type details returned", body = EntityTypeInfo),
        (status = 404, description = "Entity type not found", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single entity type discovery requests.
async fn get_entity_type(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<DiscoveryQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let body = discovery::get_type(
        state.get_ref(),
        &request,
        &context,
        &path.into_inner(),
        query.local.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/attributes",
    tag = "discovery",
    params(DiscoveryQuery),
    responses(
        (status = 200, description = "Attribute list or details returned", body = Value),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles attribute discovery requests.
async fn list_attributes(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<DiscoveryQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let query = query.into_inner();
    let body = discovery::list_attributes(
        state.get_ref(),
        &request,
        &context,
        query.local.unwrap_or(false),
        query.details.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/attributes/{attrId}",
    tag = "discovery",
    params(AttributeNamePath, DiscoveryQuery),
    responses(
        (status = 200, description = "Attribute details returned", body = AttributeInfo),
        (status = 404, description = "Attribute not found", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles single attribute discovery requests.
async fn get_attribute(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    query: web::Query<DiscoveryQuery>,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let body = discovery::get_attribute(
        state.get_ref(),
        &request,
        &context,
        &path.into_inner(),
        query.local.unwrap_or(false),
    )
    .await?;
    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .json(body))
}

#[utoipa::path(
    get,
    path = "/ngsi-ld/v1/info/sourceIdentity",
    tag = "info",
    responses(
        (status = 200, description = "Context source identity returned", body = ContextSourceIdentity),
        (status = 501, description = "Operation not implemented", body = ProblemDetails),
        (status = 500, description = "Internal error", body = ProblemDetails)
    )
)]
/// Handles context source identity retrieval requests.
async fn get_source_identity(
    state: web::Data<AppState>,
    request: HttpRequest,
) -> Result<HttpResponse, BrokerError> {
    let context = RequestContext::from_request(&request);
    let content_type =
        response_content_type(&request, crate::query::types::Representation::Normalized);
    let identity = info::source_identity(state.get_ref(), &context)?;
    let mut body = serde_json::to_value(identity)?;

    if content_type != "application/json" {
        apply_source_identity_context(&mut body);
    }

    Ok(HttpResponse::Ok()
        .insert_header((HEADER_TENANT, context.tenant))
        .insert_header((header::CONTENT_TYPE, content_type))
        .json(body))
}

/// Converts empty raw query strings into `None`.
fn normalize_query_string(raw: &str) -> Option<String> {
    if raw.is_empty() {
        None
    } else {
        Some(raw.to_string())
    }
}

fn apply_source_identity_context(body: &mut Value) {
    let Some(object) = body.as_object_mut() else {
        return;
    };

    object.insert(
        "@context".to_string(),
        Value::Array(vec![
            Value::String(NGSI_LD_CORE_CONTEXT_V1_8.to_string()),
            json!({
                "contextSourceUpTime": "ngsi-ld:contextSourceUptime"
            }),
        ]),
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actix_web::{App, http::StatusCode, test};
    use mongodb::options::{ClientOptions, ServerAddress};
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        config::AppConfig,
        domain::types::{
            PeerStatus, SwarmEventKind, SwarmMutationDocument, SwarmOperation, SwarmResourceKind,
        },
        federation::p2p::{SwimEventEnvelope, SwimEventKind, SwimPeer},
        federation::queue::{InMemoryEventQueue, NotificationTargetKind, QueueMessage},
        persistence::{mongo::MongoRepositories, repository::EntityRepository},
    };

    fn test_state_with_queue() -> (web::Data<AppState>, Arc<InMemoryEventQueue>) {
        let mut config = AppConfig::for_tests();
        config.p2p_enabled = false;

        let mut options = ClientOptions::default();
        options.hosts = vec![ServerAddress::Tcp {
            host: "127.0.0.1".to_string(),
            port: Some(27017),
        }];
        let client = mongodb::Client::with_options(options).unwrap();
        let db = client.database(&config.mongo_database);
        let repositories = MongoRepositories::new_without_indexes(&db);
        let queue = Arc::new(InMemoryEventQueue::default());

        (
            web::Data::new(AppState::new(config, db, repositories, queue.clone()).unwrap()),
            queue,
        )
    }

    fn test_state() -> web::Data<AppState> {
        test_state_with_queue().0
    }

    async fn response_body(response: actix_web::dev::ServiceResponse) -> Value {
        serde_json::from_slice(&test::read_body(response).await).unwrap()
    }

    #[actix_web::test]
    async fn create_entity_returns_problem_details_for_invalid_id() {
        let app = test::init_service(App::new().app_data(test_state()).configure(configure)).await;

        let request = test::TestRequest::post()
            .uri("/ngsi-ld/v1/entities")
            .set_json(json!({
                "id": "vehicle-1",
                "type": "Vehicle"
            }))
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_body(response).await;
        assert_eq!(
            body.get("title").and_then(Value::as_str),
            Some("InvalidRequest")
        );
        assert!(
            body.get("detail")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("entity id must be a URI")
        );
    }

    #[actix_web::test]
    async fn create_subscription_returns_problem_details_for_invalid_endpoint() {
        let app = test::init_service(App::new().app_data(test_state()).configure(configure)).await;

        let request = test::TestRequest::post()
            .uri("/ngsi-ld/v1/subscriptions")
            .set_json(json!({
                "id": "urn:ngsi-ld:Subscription:test",
                "type": "Subscription",
                "notification": {
                    "endpoint": {
                        "uri": "not-a-url"
                    }
                }
            }))
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_body(response).await;
        assert!(
            body.get("detail")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("notification endpoint must be a valid URL")
        );
    }

    #[actix_web::test]
    async fn temporal_query_requires_end_time_for_between() {
        let app = test::init_service(App::new().app_data(test_state()).configure(configure)).await;

        let request = test::TestRequest::get()
            .uri("/ngsi-ld/v1/temporal/entities?timerel=between&timeAt=2024-01-01T00:00:00Z")
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_body(response).await;
        assert!(
            body.get("detail")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("endTimeAt is required when timerel=between")
        );
    }

    #[actix_web::test]
    async fn normalizes_query_string_to_none_when_empty() {
        assert_eq!(normalize_query_string(""), None);
        assert_eq!(
            normalize_query_string("limit=10"),
            Some("limit=10".to_string())
        );
    }

    #[actix_web::test]
    async fn serves_openapi_spec() {
        let app = test::init_service(App::new().app_data(test_state()).configure(configure)).await;

        let request = test::TestRequest::get()
            .uri("/api-docs/openapi.json")
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        assert_eq!(body.get("openapi").and_then(Value::as_str), Some("3.1.0"));
        assert!(
            body.get("paths")
                .and_then(|paths| paths.get("/ngsi-ld/v1/entities"))
                .is_some()
        );
        assert!(
            body.get("paths")
                .and_then(|paths| paths.get("/ngsi-ld/v1/entityOperations/merge"))
                .is_some()
        );
        assert!(
            body.get("paths")
                .and_then(|paths| paths.get("/ngsi-ld/v1/info/sourceIdentity"))
                .is_some()
        );
        assert!(
            body.get("paths")
                .and_then(|paths| paths.get("/ngsi-ld/v1/types"))
                .is_some()
        );
        assert!(
            body.get("paths")
                .and_then(|paths| paths.get("/internal/swim"))
                .is_some()
        );
        assert!(
            body.get("paths")
                .and_then(|paths| paths.get("/internal/swim/mutations"))
                .is_some()
        );
    }

    #[actix_web::test]
    async fn internal_swim_applies_mutation_enqueues_notification_and_lists_mutation() {
        let (state, queue) = test_state_with_queue();
        let app = test::init_service(App::new().app_data(state.clone()).configure(configure)).await;

        let subscription_request = test::TestRequest::post()
            .uri("/ngsi-ld/v1/subscriptions")
            .set_json(json!({
                "id": "urn:ngsi-ld:Subscription:swim-notify",
                "type": "Subscription",
                "entities": [{"type": "Vehicle"}],
                "watchedAttributes": ["speed"],
                "notification": {
                    "endpoint": {
                        "uri": "http://listener.example/notify"
                    }
                }
            }))
            .to_request();
        let subscription_response = test::call_service(&app, subscription_request).await;
        assert_eq!(subscription_response.status(), StatusCode::CREATED);

        let mutation = SwarmMutationDocument {
            tenant: "default".to_string(),
            mutation_id: "urn:ngsi-ld:SwarmMutation:peer-b:1".to_string(),
            resource_kind: SwarmResourceKind::Entity,
            operation: SwarmOperation::Upsert,
            entity_id: "urn:ngsi-ld:Vehicle:swim-1".to_string(),
            version_at: "2024-01-01T00:00:00Z".to_string(),
            version_at_nanos: 1,
            recorded_at: "2024-01-01T00:00:01Z".to_string(),
            recorded_at_nanos: 2,
            source_peer_id: "peer-b".to_string(),
            event_kind: SwarmEventKind::Created,
            changed_attributes: vec!["speed".to_string()],
            snapshot: Some(json!({
                "id": "urn:ngsi-ld:Vehicle:swim-1",
                "type": "Vehicle",
                "speed": {"type": "Property", "value": 50},
                "createdAt": "2024-01-01T00:00:00Z",
                "modifiedAt": "2024-01-01T00:00:00Z"
            })),
        };
        let peer = SwimPeer {
            peer_id: "peer-b".to_string(),
            endpoint: "http://peer-b:1026/ngsi-ld/v1".to_string(),
            aliases: vec!["peer-b".to_string()],
            capabilities: vec!["swim".to_string()],
            neighbors: Vec::new(),
            status: PeerStatus::Alive,
            incarnation: 1,
            tenant: Some("default".to_string()),
        };

        let swim_request = test::TestRequest::post()
            .uri("/internal/swim")
            .set_json(SwimEventEnvelope {
                kind: SwimEventKind::Mutation,
                source: peer.clone(),
                peer,
                mutation: Some(mutation.clone()),
                recorded_at: mutation.recorded_at.clone(),
            })
            .to_request();
        let swim_response = test::call_service(&app, swim_request).await;
        assert_eq!(swim_response.status(), StatusCode::NO_CONTENT);

        let stored = state
            .repositories
            .entities
            .get("default", "urn:ngsi-ld:Vehicle:swim-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.doc.get("id").and_then(Value::as_str),
            Some("urn:ngsi-ld:Vehicle:swim-1")
        );

        let messages = queue.messages();
        assert_eq!(messages.len(), 1);
        match &messages[0] {
            QueueMessage::Notification(job) => {
                assert_eq!(job.target_kind, NotificationTargetKind::Subscription);
                assert_eq!(job.subscription_id, "urn:ngsi-ld:Subscription:swim-notify");
                assert_eq!(
                    job.payload.get("type").and_then(Value::as_str),
                    Some("Notification")
                );
                assert_eq!(
                    job.payload
                        .get("data")
                        .and_then(Value::as_array)
                        .and_then(|items| items.first())
                        .and_then(|item| item.get("id"))
                        .and_then(Value::as_str),
                    Some("urn:ngsi-ld:Vehicle:swim-1")
                );
            }
            other => panic!("unexpected queue message: {other:?}"),
        }

        let list_request = test::TestRequest::get()
            .uri("/internal/swim/mutations")
            .to_request();
        let list_response = test::call_service(&app, list_request).await;
        assert_eq!(list_response.status(), StatusCode::OK);
        let list_body = response_body(list_response).await;
        assert_eq!(
            list_body
                .as_array()
                .and_then(|items| items.first())
                .and_then(|item| item.get("mutationId"))
                .and_then(Value::as_str),
            Some("urn:ngsi-ld:SwarmMutation:peer-b:1")
        );
    }

    #[actix_web::test]
    async fn lists_entity_types_and_attributes() {
        let state = test_state();
        state
            .repositories
            .entities
            .insert(crate::domain::types::StoredDocument {
                tenant: "default".to_string(),
                ngsi_id: "urn:ngsi-ld:Vehicle:1".to_string(),
                doc: json!({
                    "id": "urn:ngsi-ld:Vehicle:1",
                    "type": "Vehicle",
                    "speed": {"type": "Property", "value": 42},
                    "driver": {"type": "Relationship", "object": "urn:ngsi-ld:Person:1"}
                }),
            })
            .await
            .unwrap();

        let app = test::init_service(App::new().app_data(state).configure(configure)).await;

        let types_request = test::TestRequest::get()
            .uri("/ngsi-ld/v1/types")
            .to_request();
        let types_response = test::call_service(&app, types_request).await;
        assert_eq!(types_response.status(), StatusCode::OK);
        let types_body = response_body(types_response).await;
        assert_eq!(
            types_body
                .get("typeList")
                .and_then(Value::as_array)
                .map(|items| items.len()),
            Some(1)
        );

        let attrs_request = test::TestRequest::get()
            .uri("/ngsi-ld/v1/attributes?details=true")
            .to_request();
        let attrs_response = test::call_service(&app, attrs_request).await;
        assert_eq!(attrs_response.status(), StatusCode::OK);
        let attrs_body = response_body(attrs_response).await;
        let items = attrs_body.as_array().unwrap();
        assert!(items.iter().any(|item| {
            item.get("attributeName").and_then(Value::as_str) == Some("speed")
                && item
                    .get("attributeTypes")
                    .and_then(Value::as_array)
                    .map(|types| types.iter().any(|value| value.as_str() == Some("Property")))
                    .unwrap_or(false)
        }));
    }

    #[actix_web::test]
    async fn retrieves_entity_type_and_attribute_details() {
        let state = test_state();
        state
            .repositories
            .entities
            .insert(crate::domain::types::StoredDocument {
                tenant: "default".to_string(),
                ngsi_id: "urn:ngsi-ld:Vehicle:2".to_string(),
                doc: json!({
                    "id": "urn:ngsi-ld:Vehicle:2",
                    "type": ["Vehicle", "Asset"],
                    "location": {"type": "GeoProperty", "value": {"type": "Point", "coordinates": [0.0, 0.0]}}
                }),
            })
            .await
            .unwrap();

        let app = test::init_service(App::new().app_data(state).configure(configure)).await;

        let type_request = test::TestRequest::get()
            .uri("/ngsi-ld/v1/types/Vehicle")
            .to_request();
        let type_response = test::call_service(&app, type_request).await;
        assert_eq!(type_response.status(), StatusCode::OK);
        let type_body = response_body(type_response).await;
        assert_eq!(
            type_body.get("typeName").and_then(Value::as_str),
            Some("Vehicle")
        );

        let attr_request = test::TestRequest::get()
            .uri("/ngsi-ld/v1/attributes/location")
            .to_request();
        let attr_response = test::call_service(&app, attr_request).await;
        assert_eq!(attr_response.status(), StatusCode::OK);
        let attr_body = response_body(attr_response).await;
        assert_eq!(
            attr_body.get("attributeName").and_then(Value::as_str),
            Some("location")
        );
        assert!(
            attr_body
                .get("attributeTypes")
                .and_then(Value::as_array)
                .map(|types| types
                    .iter()
                    .any(|value| value.as_str() == Some("GeoProperty")))
                .unwrap_or(false)
        );
    }

    #[actix_web::test]
    async fn entity_query_supports_expand_values_and_linked_entity_resolution() {
        let state = test_state();
        state
            .repositories
            .entities
            .insert(crate::domain::types::StoredDocument {
                tenant: "default".to_string(),
                ngsi_id: "urn:ngsi-ld:Vehicle:linked-1".to_string(),
                doc: json!({
                    "id": "urn:ngsi-ld:Vehicle:linked-1",
                    "type": "Vehicle",
                    "@context": {
                        "MercedesBrand": "https://example.org/brands/Mercedes"
                    },
                    "brandCode": {"type": "Property", "value": "MercedesBrand"},
                    "sensor": {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:Device:linked-1"
                    }
                }),
            })
            .await
            .unwrap();
        state
            .repositories
            .entities
            .insert(crate::domain::types::StoredDocument {
                tenant: "default".to_string(),
                ngsi_id: "urn:ngsi-ld:Device:linked-1".to_string(),
                doc: json!({
                    "id": "urn:ngsi-ld:Device:linked-1",
                    "type": "Device",
                    "humidity": {"type": "Property", "value": 40}
                }),
            })
            .await
            .unwrap();

        let app = test::init_service(App::new().app_data(state).configure(configure)).await;

        let request = test::TestRequest::get()
            .uri("/ngsi-ld/v1/entities?type=Vehicle&q=brandCode==https://example.org/brands/Mercedes;sensor%7BDevice:humidity%7D==40&expandValues=brandCode")
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_body(response).await;
        let items = body.as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0].get("id").and_then(Value::as_str),
            Some("urn:ngsi-ld:Vehicle:linked-1")
        );
    }

    #[actix_web::test]
    async fn merge_batch_updates_and_deletes_attributes() {
        let state = test_state();
        state
            .repositories
            .entities
            .insert(crate::domain::types::StoredDocument {
                tenant: "default".to_string(),
                ngsi_id: "urn:ngsi-ld:Vehicle:merge-1".to_string(),
                doc: json!({
                    "id": "urn:ngsi-ld:Vehicle:merge-1",
                    "type": "Vehicle",
                    "speed": {"type": "Property", "value": 42},
                    "brand": {"type": "Property", "value": "Fiat"},
                    "createdAt": "2024-01-01T00:00:00Z",
                    "modifiedAt": "2024-01-01T00:00:00Z"
                }),
            })
            .await
            .unwrap();

        let app = test::init_service(App::new().app_data(state.clone()).configure(configure)).await;

        let request = test::TestRequest::post()
            .uri("/ngsi-ld/v1/entityOperations/merge")
            .set_json(json!([
                {
                    "id": "urn:ngsi-ld:Vehicle:merge-1",
                    "type": "Vehicle",
                    "speed": {"type": "Property", "value": 84},
                    "brand": null
                }
            ]))
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let stored = state
            .repositories
            .entities
            .get("default", "urn:ngsi-ld:Vehicle:merge-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .doc
                .get("speed")
                .and_then(|value| value.get("value"))
                .and_then(Value::as_i64),
            Some(84)
        );
        assert!(stored.doc.get("brand").is_none());
    }

    #[actix_web::test]
    async fn merge_batch_returns_multi_status_for_missing_entities() {
        let state = test_state();
        state
            .repositories
            .entities
            .insert(crate::domain::types::StoredDocument {
                tenant: "default".to_string(),
                ngsi_id: "urn:ngsi-ld:Vehicle:merge-2".to_string(),
                doc: json!({
                    "id": "urn:ngsi-ld:Vehicle:merge-2",
                    "type": "Vehicle",
                    "speed": {"type": "Property", "value": 20},
                    "createdAt": "2024-01-01T00:00:00Z",
                    "modifiedAt": "2024-01-01T00:00:00Z"
                }),
            })
            .await
            .unwrap();

        let app = test::init_service(App::new().app_data(state.clone()).configure(configure)).await;

        let request = test::TestRequest::post()
            .uri("/ngsi-ld/v1/entityOperations/merge")
            .set_json(json!([
                {
                    "id": "urn:ngsi-ld:Vehicle:merge-2",
                    "type": "Vehicle",
                    "speed": {"type": "Property", "value": 21}
                },
                {
                    "id": "urn:ngsi-ld:Vehicle:merge-missing",
                    "type": "Vehicle",
                    "speed": {"type": "Property", "value": 22}
                }
            ]))
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::MULTI_STATUS);
        let body = response_body(response).await;
        assert_eq!(
            body.get("success")
                .and_then(Value::as_array)
                .map(|items| items.len()),
            Some(1)
        );
        assert_eq!(
            body.get("errors")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("entityId"))
                .and_then(Value::as_str),
            Some("urn:ngsi-ld:Vehicle:merge-missing")
        );

        let stored = state
            .repositories
            .entities
            .get("default", "urn:ngsi-ld:Vehicle:merge-2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored
                .doc
                .get("speed")
                .and_then(|value| value.get("value"))
                .and_then(Value::as_i64),
            Some(21)
        );
    }

    #[actix_web::test]
    async fn retrieves_source_identity_as_jsonld_for_tenant() {
        let app = test::init_service(App::new().app_data(test_state()).configure(configure)).await;

        let request = test::TestRequest::get()
            .uri("/ngsi-ld/v1/info/sourceIdentity")
            .insert_header((header::ACCEPT, "application/json+ld"))
            .insert_header((HEADER_TENANT, "tenant-a"))
            .to_request();
        let response = test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.starts_with("application/json+ld")),
            Some(true)
        );
        assert_eq!(
            response
                .headers()
                .get(HEADER_TENANT)
                .and_then(|value| value.to_str().ok()),
            Some("tenant-a")
        );

        let body = response_body(response).await;
        assert_eq!(
            body.get("type").and_then(Value::as_str),
            Some("ContextSourceIdentity")
        );
        assert_eq!(
            body.get("contextSourceAlias").and_then(Value::as_str),
            Some("test-broker-tenant-a")
        );
        assert!(
            body.get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .contains("/ngsi-ld/v1/info/sourceIdentity?tenant=tenant-a")
        );
        assert!(
            body.get("contextSourceUpTime")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .starts_with('P')
        );
        assert_eq!(
            body.get("@context")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(Value::as_str),
            Some(NGSI_LD_CORE_CONTEXT_V1_8)
        );
    }
}
