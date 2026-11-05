table flp_front_auth {
  "front_key": "test-front-key-not-a-live-secret",
  "cdn_key": "test-cdn-key-not-a-live-secret",
}


table flp_fos_buckets {
  "AAAAAAAAAAAAAAAAAAAAAA": "fos-test-logs|us-east-1",
}


table flp_fos_credentials {
  "AAAAAAAAAAAAAAAAAAAAAA.ak": "fixture-access",
  "AAAAAAAAAAAAAAAAAAAAAA.sk": "fixture-secret",
}

include "../main.vcl";
