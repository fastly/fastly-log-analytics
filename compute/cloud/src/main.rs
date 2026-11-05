// Undecorated so a clock probe can be answered before its work runs (`send_to_client`).
#[cfg(target_arch = "wasm32")]
fn main() -> Result<(), fastly::Error> {
    use fastly_log_analytics::{
        http, sdk,
        shell::{Runtime, Shell},
        state::FastlyState,
    };
    use std::{io::Read, time::Instant};

    let started = Instant::now();
    let mut request = fastly::Request::from_client();
    let method = request.get_method().to_string();
    let path = request.get_path().to_owned();
    let headers = request
        .get_header_names()
        .filter_map(|name| {
            request
                .get_header_str(name)
                .map(|value| (name.as_str().to_lowercase(), value.to_owned()))
        })
        .collect();
    let mut body = Vec::new();
    request.take_body().take(65_537).read_to_end(&mut body)?;
    let input = http::Request {
        method: method.clone(),
        path: request
            .get_query_str()
            .map_or_else(|| path.clone(), |query| format!("{path}?{query}")),
        headers,
        body,
    };
    let send_raw = |output: http::Response| {
        let mut response = fastly::Response::from_status(output.status).with_body(output.body);
        for (name, value) in output.headers {
            response.set_header(name, value);
        }
        let status = response.get_status().as_u16();
        response.send_to_client();
        status
    };
    let send = |output: http::Response| send_raw(output.secured());
    let (status, after_ack) = match sdk::FastlyRuntime::open() {
        Ok(runtime) => {
            let worker_id = runtime
                .config("worker_service_id")
                .map_err(fastly::Error::msg)?;
            if worker_id.is_some() && std::env::var("FASTLY_SERVICE_ID").ok() == worker_id {
                (send(sdk::worker(&input, &runtime)), None)
            } else if sdk::is_relayed_shard(&input) {
                (
                    send(fastly_log_analytics::ingest::worker::handle_worker_ingest(
                        &runtime, &input,
                    )),
                    None,
                )
            } else {
                let state = FastlyState::open_with_budget(runtime.backend_budget())?;
                let shell = Shell::new(&runtime, &state, fastly_log_analytics::ASSETS);
                match shell.clock_ack(&input) {
                    // The work's own outcome is recorded in cron_runs and admin events.
                    Some(ack) => {
                        let status = send_raw(ack);
                        (status, Some(shell.handle(&input).status))
                    }
                    None => {
                        let mut output = shell.handle(&input).compress_hint();
                        match output.upstream.take() {
                            None => (send(output), None),
                            Some(upstream) => (stream(&runtime, upstream, output), None),
                        }
                    }
                }
            }
        }
        Err(_) => (
            send(http::Response::new(403, "Front authentication required").private()),
            None,
        ),
    };
    println!(
        "{}",
        serde_json::json!({"event":"request.end","level":"info","method":method,"path":safe_path(&path),"status":status,"work_status":after_ack,"duration_ms":started.elapsed().as_millis()})
    );
    Ok(())
}

/// Sends `output` with its body streamed from `upstream`, keeping the upstream's status and
/// length and range headers; a missing object is a 404 and any other failure a 502.
#[cfg(target_arch = "wasm32")]
fn stream(
    runtime: &fastly_log_analytics::sdk::FastlyRuntime,
    upstream: Box<fastly_log_analytics::http::Upstream>,
    output: fastly_log_analytics::http::Response,
) -> u16 {
    use fastly_log_analytics::shell::{upstream_failure, UPSTREAM_HEADERS};
    let send = |output: fastly_log_analytics::http::Response, body: fastly::Body| {
        let output = output.secured();
        let mut response = fastly::Response::from_status(output.status).with_body(body);
        for (name, value) in output.headers {
            response.set_header(name, value);
        }
        if upstream.head_only {
            response
                .set_framing_headers_mode(fastly::http::FramingHeadersMode::ManuallyFromHeaders);
        }
        response.send_to_client();
        output.status
    };
    let mut fetched = match runtime.upstream(&upstream) {
        Ok(fetched) => fetched,
        Err(_) => {
            let failure = upstream_failure(502).unwrap_or(output);
            let body = failure.body.clone().into();
            return send(failure, body);
        }
    };
    let status = fetched.get_status().as_u16();
    if let Some(failure) = upstream_failure(status) {
        let body = failure.body.clone().into();
        return send(failure, body);
    }
    let mut output = output;
    output.status = status;
    for name in UPSTREAM_HEADERS {
        if let Some(value) = fetched.get_header_str(name) {
            output.headers.insert(name.to_owned(), value.to_owned());
        }
    }
    if let Some(value) = fetched.get_header_str("x-flp-fos-cache") {
        output
            .headers
            .insert("x-flp-cdn-cache".to_owned(), value.to_owned());
    }
    send(output, fetched.take_body())
}

#[cfg(target_arch = "wasm32")]
fn safe_path(path: &str) -> &str {
    if path.starts_with("/api/admin/users/") {
        "/api/admin/users/:id"
    } else if path.starts_with("/api/admin/invites/") {
        "/api/admin/invites/:id"
    } else if path.len() > 256 {
        "/other"
    } else {
        path
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    eprintln!("Build flp for wasm32-wasip1; use shell-test-server for native acceptance tests");
    std::process::exit(2);
}
