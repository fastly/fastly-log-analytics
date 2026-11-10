//! Saved filter views, shared by every user of the install like the self-hosted `/api/views`.
//! One KV record per service, `views:<service id>`, holds the list; writes go through a
//! compare-and-swap so two browsers saving at once both land.

use crate::admin::Record;
use crate::auth::Error;
use crate::http::{Request, Response};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const MAX_VIEWS: usize = 200;
const MAX_NAME_CHARS: usize = 120;
const MAX_FILTERS_BYTES: usize = 32 * 1024;
const MAX_FIELD_CHARS: usize = 200;
const DEFAULT_LIMIT: usize = 500;
const MAX_LIMIT: usize = 2000;

/// The subset of the runtime a view store needs, so the logic is testable without a Compute host.
pub trait ViewStore {
    fn get(&self, key: &str) -> Result<Option<Record>, String>;
    fn cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String>;
    fn random(&self) -> Result<[u8; 32], String>;
    fn now(&self) -> u64;
}

impl<R: crate::shell::Runtime> ViewStore for R {
    fn get(&self, key: &str) -> Result<Option<Record>, String> {
        self.admin_get(key)
    }
    fn cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String> {
        self.admin_cas(key, generation, value)
    }
    fn random(&self) -> Result<[u8; 32], String> {
        crate::shell::Runtime::random(self)
    }
    fn now(&self) -> u64 {
        crate::shell::Runtime::now(self)
    }
}

/// The wire shape of the self-hosted `SavedViewRecord`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SavedView {
    pub id: String,
    pub service_id: String,
    pub name: String,
    pub filters_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewInput {
    #[serde(default)]
    id: Option<String>,
    service_id: String,
    name: String,
    filters_json: String,
    #[serde(default)]
    time_range_type: Option<String>,
    #[serde(default)]
    start_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
    #[serde(default)]
    page: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
}

pub fn key(service_id: &str) -> String {
    format!("views:{service_id}")
}

fn parse_views(value: &Value) -> Vec<SavedView> {
    serde_json::from_value(value["views"].clone()).unwrap_or_default()
}

fn load<S: ViewStore>(
    store: &S,
    service_id: &str,
) -> Result<(Option<String>, Vec<SavedView>), Error> {
    match store.get(&key(service_id)).map_err(|_| Error::Storage)? {
        Some(record) => Ok((Some(record.generation), parse_views(&record.value))),
        None => Ok((None, Vec::new())),
    }
}

pub fn list<S: ViewStore>(
    store: &S,
    service_id: &str,
    limit: usize,
) -> Result<Vec<SavedView>, Error> {
    let (_, mut views) = load(store, service_id)?;
    views.truncate(limit);
    Ok(views)
}

fn update<S: ViewStore, T>(
    store: &S,
    service_id: &str,
    mut operation: impl FnMut(&mut Vec<SavedView>) -> Result<T, Error>,
) -> Result<T, Error> {
    for _ in 0..3 {
        let (generation, mut views) = load(store, service_id)?;
        let before = views.clone();
        let result = operation(&mut views)?;
        if views == before {
            return Ok(result);
        }
        let value = json!({"views": views, "updated_at": store.now()});
        if store
            .cas(&key(service_id), generation.as_deref(), &value)
            .map_err(|_| Error::Storage)?
        {
            return Ok(result);
        }
    }
    Err(Error::Conflict)
}

fn bounded(value: Option<String>) -> Result<Option<String>, Error> {
    match value {
        Some(text) if text.chars().count() > MAX_FIELD_CHARS => Err(Error::InvalidInput),
        other => Ok(other),
    }
}

fn new_id(store: &impl ViewStore) -> Result<String, Error> {
    let bytes = store.random().map_err(|_| Error::Storage)?;
    Ok(hex::encode(&bytes[..16]))
}

/// Creates a view, or replaces the one whose `id` the body names, like the self-hosted
/// `INSERT OR REPLACE`.
pub fn save<S: ViewStore>(store: &S, service_id: &str, body: &[u8]) -> Result<String, Error> {
    let input: ViewInput = serde_json::from_slice(body).map_err(|_| Error::InvalidInput)?;
    if input.service_id != service_id {
        return Err(Error::NotFound);
    }
    let name = input.name.trim().to_owned();
    if name.is_empty()
        || name.chars().count() > MAX_NAME_CHARS
        || input.filters_json.len() > MAX_FILTERS_BYTES
        || serde_json::from_str::<Value>(&input.filters_json).is_err()
    {
        return Err(Error::InvalidInput);
    }
    let id = match input.id.filter(|id| !id.is_empty()) {
        Some(id) if id.chars().count() <= MAX_FIELD_CHARS => id,
        Some(_) => return Err(Error::InvalidInput),
        None => new_id(store)?,
    };
    let view = SavedView {
        id: id.clone(),
        service_id: service_id.to_owned(),
        name,
        filters_json: input.filters_json,
        time_range_type: bounded(input.time_range_type)?,
        start_time: bounded(input.start_time)?,
        end_time: bounded(input.end_time)?,
        page: bounded(input.page)?,
        created_at: Some(
            bounded(input.created_at)?
                .unwrap_or_else(|| crate::admin::rfc3339_from_secs(store.now())),
        ),
    };
    update(store, service_id, |views| {
        if let Some(existing) = views.iter_mut().find(|v| v.id == view.id) {
            *existing = view.clone();
        } else if views.len() >= MAX_VIEWS {
            return Err(Error::InvalidInput);
        } else {
            views.push(view.clone());
        }
        Ok(())
    })?;
    Ok(id)
}

pub fn delete<S: ViewStore>(store: &S, service_id: &str, view_id: &str) -> Result<(), Error> {
    update(store, service_id, |views| {
        let before = views.len();
        views.retain(|v| v.id != view_id);
        if views.len() == before {
            return Err(Error::NotFound);
        }
        Ok(())
    })
}

fn query_param(request: &Request, name: &str) -> Option<String> {
    let query = request.path.split_once('?').map_or("", |(_, query)| query);
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
        .filter(|value| !value.is_empty())
}

fn json_response(status: u16, value: Value) -> Response {
    Response::new(status, value.to_string())
        .header("content-type", "application/json")
        .private()
}

/// Serves `/api/views…` for the install's active service `service_id`. Every signed-in role may
/// read and write views, as on the self-hosted backend.
pub fn handle<S: ViewStore>(
    store: &S,
    service_id: &str,
    request: &Request,
) -> Result<Response, Error> {
    let path = request.path.split('?').next().unwrap_or_default();
    let tail = path
        .strip_prefix("/api/views")
        .ok_or(Error::NotFound)?
        .trim_matches('/');
    let segment = (!tail.is_empty())
        .then(|| {
            url::form_urlencoded::parse(format!("v={tail}").as_bytes())
                .next()
                .map(|(_, value)| value.into_owned())
                .unwrap_or_default()
        })
        .filter(|segment| !segment.contains('/'));
    if !tail.is_empty() && segment.is_none() {
        return Err(Error::NotFound);
    }
    match (request.method.as_str(), segment) {
        ("GET", Some(wanted)) => {
            if wanted != service_id {
                return Err(Error::NotFound);
            }
            let limit = match query_param(request, "limit") {
                Some(value) => value
                    .parse::<usize>()
                    .ok()
                    .filter(|limit| (1..=MAX_LIMIT).contains(limit))
                    .ok_or(Error::InvalidInput)?,
                None => DEFAULT_LIMIT,
            };
            let views = list(store, service_id, limit)?;
            Ok(json_response(
                200,
                serde_json::to_value(views).map_err(|_| Error::Storage)?,
            ))
        }
        ("POST", None) => {
            if request.body.len() > crate::http::body_limit(request) {
                return Err(Error::InvalidInput);
            }
            let id = save(store, service_id, &request.body)?;
            Ok(json_response(201, json!({"id": id, "status": "success"})))
        }
        ("DELETE", Some(view_id)) => {
            let scoped = query_param(request, "service_id")
                .or_else(|| request.header("x-service-id").map(str::to_owned));
            if scoped.is_some_and(|scoped| scoped != service_id) {
                return Err(Error::NotFound);
            }
            delete(store, service_id, &view_id)?;
            Ok(Response::new(204, Vec::new()).private())
        }
        _ => Err(Error::NotFound),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct Memory {
        records: RefCell<BTreeMap<String, (u64, Value)>>,
        fail_cas: RefCell<u32>,
        randoms: RefCell<u8>,
    }

    impl ViewStore for Memory {
        fn get(&self, key: &str) -> Result<Option<Record>, String> {
            Ok(self.records.borrow().get(key).map(|(g, v)| Record {
                generation: g.to_string(),
                value: v.clone(),
            }))
        }
        fn cas(&self, key: &str, generation: Option<&str>, value: &Value) -> Result<bool, String> {
            if *self.fail_cas.borrow() > 0 {
                *self.fail_cas.borrow_mut() -= 1;
                return Ok(false);
            }
            let mut records = self.records.borrow_mut();
            let current = records.get(key).map(|(g, _)| g.to_string());
            if current.as_deref() != generation {
                return Ok(false);
            }
            let next = current.map_or(1, |g| g.parse::<u64>().unwrap() + 1);
            records.insert(key.to_owned(), (next, value.clone()));
            Ok(true)
        }
        fn random(&self) -> Result<[u8; 32], String> {
            *self.randoms.borrow_mut() += 1;
            Ok([*self.randoms.borrow(); 32])
        }
        fn now(&self) -> u64 {
            1_700_000_000
        }
    }

    fn request(method: &str, path: &str, body: Value) -> Request {
        Request {
            method: method.into(),
            path: path.into(),
            headers: BTreeMap::new(),
            body: if body.is_null() {
                Vec::new()
            } else {
                body.to_string().into_bytes()
            },
        }
    }

    fn body(response: &Response) -> Value {
        serde_json::from_slice(&response.body).unwrap()
    }

    fn call(store: &Memory, method: &str, path: &str, body: Value) -> Result<Response, Error> {
        handle(store, "svc1", &request(method, path, body))
    }

    fn failure(result: Result<Response, Error>) -> Error {
        match result {
            Ok(response) => panic!("expected an error, got status {}", response.status),
            Err(error) => error,
        }
    }

    fn view(name: &str) -> Value {
        json!({"service_id":"svc1","name":name,"filters_json":"{}"})
    }

    fn post(store: &Memory, view: Value) -> String {
        body(&call(store, "POST", "/api/views/", view).unwrap())["id"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn views_round_trip_through_the_service_record_for_every_user() {
        let store = Memory::default();
        let created = call(
            &store,
            "POST",
            "/api/views/",
            json!({"service_id":"svc1","name":" Errors ","filters_json":"{\"filters\":[]}","page":"/dashboard"}),
        )
        .unwrap();
        assert_eq!(created.status, 201);
        let id = body(&created)["id"].as_str().unwrap().to_owned();
        assert_eq!(body(&created)["status"], "success");
        let rows = body(&call(&store, "GET", "/api/views/svc1?limit=10", Value::Null).unwrap());
        assert_eq!(rows.as_array().unwrap().len(), 1);
        assert_eq!(rows[0]["id"], id);
        assert_eq!(rows[0]["name"], "Errors");
        assert_eq!(rows[0]["service_id"], "svc1");
        assert_eq!(rows[0]["page"], "/dashboard");
        assert!(rows[0]["created_at"].is_string());
        assert!(store.records.borrow().contains_key("views:svc1"));
        let path = format!("/api/views/{id}");
        assert_eq!(
            call(&store, "DELETE", &path, Value::Null).unwrap().status,
            204
        );
        assert_eq!(
            failure(call(&store, "DELETE", &path, Value::Null)),
            Error::NotFound
        );
        let rows = body(&call(&store, "GET", "/api/views/svc1", Value::Null).unwrap());
        assert_eq!(rows.as_array().unwrap().len(), 0);
    }

    #[test]
    fn saving_an_existing_id_replaces_it_and_a_lost_cas_race_is_retried() {
        let store = Memory::default();
        let first = post(&store, view("one"));
        *store.fail_cas.borrow_mut() = 1;
        let second = post(&store, view("two"));
        assert_ne!(first, second);
        let mut renamed = view("renamed");
        renamed["id"] = first.clone().into();
        assert_eq!(post(&store, renamed), first);
        let names: Vec<String> = list(&store, "svc1", 50)
            .unwrap()
            .into_iter()
            .map(|v| v.name)
            .collect();
        assert_eq!(names, vec!["renamed", "two"]);
        *store.fail_cas.borrow_mut() = 3;
        assert_eq!(
            failure(call(&store, "POST", "/api/views", view("x"))),
            Error::Conflict
        );
    }

    #[test]
    fn views_are_scoped_to_the_active_service_and_validated() {
        let store = Memory::default();
        let other = json!({"service_id":"svc2","name":"n","filters_json":"{}"});
        assert_eq!(
            failure(call(&store, "POST", "/api/views/", other)),
            Error::NotFound
        );
        assert_eq!(
            failure(call(&store, "GET", "/api/views/svc2", Value::Null)),
            Error::NotFound
        );
        for bad in [
            json!({"service_id":"svc1","name":"   ","filters_json":"{}"}),
            json!({"service_id":"svc1","name":"n","filters_json":"not json"}),
            json!({"service_id":"svc1","name":"n","filters_json":"{}","extra":1}),
            json!({"service_id":"svc1","name":"n","filters_json":"{}","page":"x".repeat(201)}),
            json!({"service_id":"svc1","name":"n"}),
        ] {
            assert_eq!(
                failure(call(&store, "POST", "/api/views/", bad)),
                Error::InvalidInput
            );
        }
        assert!(store.records.borrow().is_empty());
        assert_eq!(
            failure(call(&store, "GET", "/api/views/svc1?limit=0", Value::Null)),
            Error::InvalidInput
        );
        assert_eq!(
            failure(call(&store, "GET", "/api/views/svc1/extra", Value::Null)),
            Error::NotFound
        );
        let id = post(&store, view("n"));
        assert_eq!(
            failure(call(
                &store,
                "DELETE",
                &format!("/api/views/{id}?service_id=svc2"),
                Value::Null
            )),
            Error::NotFound
        );
        for _ in 1..MAX_VIEWS {
            post(&store, view("n"));
        }
        assert_eq!(
            failure(call(&store, "POST", "/api/views/", view("n"))),
            Error::InvalidInput
        );
        assert_eq!(list(&store, "svc1", MAX_LIMIT).unwrap().len(), MAX_VIEWS);
    }
}
