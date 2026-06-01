# Resource-graph assertions (TESTING_PLAN_3 item 16).
#
# Run via `terraform test` against a tmp directory seeded with the
# generator's output + the `_providers.tf` stub from
# `tests/utils/test_terraform_resource_graph.py`. Uses `command = plan` so
# no real Fastly/AWS calls happen; assertions only inspect the planned
# resource graph.

run "fos_bucket_resource_exists" {
  command = plan

  assert {
    condition     = aws_s3_bucket.fos_bucket.bucket == "my-test-bucket"
    error_message = "aws_s3_bucket.fos_bucket.bucket must match the cfg's fos_bucket_name"
  }
}

run "cdn_proxy_has_both_required_dictionaries" {
  command = plan

  # `dictionary` is a block list in fastly_service_vcl; assert both names
  # appear. If a future refactor drops one, the dashboard's CDN auth path
  # breaks silently — this catches it at plan time.
  assert {
    condition     = length([for d in fastly_service_vcl.cdn_proxy.dictionary : d if d.name == "fos_credentials"]) == 1
    error_message = "cdn_proxy must declare exactly one fos_credentials dictionary"
  }

  assert {
    condition     = length([for d in fastly_service_vcl.cdn_proxy.dictionary : d if d.name == "cdn_auth"]) == 1
    error_message = "cdn_proxy must declare exactly one cdn_auth dictionary"
  }
}

run "cdn_proxy_has_fos_origin_backend" {
  command = plan

  # Without a backend pointing at *.fastlystorage.app, the CDN serves no
  # logs. This is the spine of the dashboard's read path.
  assert {
    condition = length([
      for b in fastly_service_vcl.cdn_proxy.backend :
      b
      if can(regex(".*\\.fastlystorage\\.app$", b.address))
    ]) >= 1
    error_message = "cdn_proxy must have at least one backend pointing to *.fastlystorage.app"
  }
}

run "cdn_proxy_has_required_cdn_snippets" {
  command = plan

  # The CDN snippets are what fix the negative-cache-CAS trap, race
  # condition generation, and the iceberg metadata pointer TTL — pinned
  # in the codebase memories. If any of these stop being emitted, we
  # regress to known production bugs.
  assert {
    condition = length([
      for s in fastly_service_vcl.cdn_proxy.snippet :
      s
      if s.name == "cdn-no-cache-404"
    ]) == 1
    error_message = "cdn-no-cache-404 snippet missing — see Fastly negative-cache CAS trap"
  }

  assert {
    condition = length([
      for s in fastly_service_vcl.cdn_proxy.snippet :
      s
      if s.name == "iceberg-metadata-pointer-ttl"
    ]) == 1
    error_message = "iceberg-metadata-pointer-ttl snippet missing"
  }

  assert {
    condition = length([
      for s in fastly_service_vcl.cdn_proxy.snippet :
      s
      if s.name == "cdn-race-condition-generation"
    ]) == 1
    error_message = "cdn-race-condition-generation snippet missing"
  }
}

run "logging_service_has_logging_s3_endpoint" {
  command = plan

  # The whole pipeline starts here — fastly writes log batches to FOS via
  # this endpoint. Assert the bucket name plumbs through correctly.
  assert {
    condition = length([
      for ep in fastly_service_vcl.logging_service.logging_s3 :
      ep
      if ep.bucket_name == "my-test-bucket"
    ]) == 1
    error_message = "logging_service must declare a logging_s3 endpoint pointing at my-test-bucket"
  }
}

run "logging_service_has_capture_recv_snippet" {
  command = plan

  # The recv-stage snippet ("Fastly Log Analysis Capture") is what
  # actually instructs Fastly to write to the endpoint. Without it,
  # logging_s3 declared above is dead config.
  assert {
    condition = length([
      for s in fastly_service_vcl.logging_service.snippet :
      s
      if s.name == "Fastly Log Analysis Capture" && s.type == "recv"
    ]) == 1
    error_message = "logging_service must declare the recv-stage Capture snippet"
  }
}

run "dictionary_items_wire_fos_credentials_to_bucket" {
  command = plan

  # The dictionary_items resource is what populates the dictionary at
  # apply time. A break here means the CDN can't actually fetch from
  # the bucket — dashboard returns empty logs.
  assert {
    condition     = fastly_service_dictionary_items.fos_credentials.items["region"] == "us-east-1"
    error_message = "fos_credentials dictionary items must include region=us-east-1"
  }
}
