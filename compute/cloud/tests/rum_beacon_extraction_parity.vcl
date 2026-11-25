# Extract the RUM request ID; the beacon URL carries everything else.
set req.http.x-fos-edge-data:fastly_req_id = querystring.get(req.url, "req");
if (req.http.x-fos-edge-data:fastly_req_id == "") {
    set req.http.x-fos-edge-data:fastly_req_id = req.http.Fastly-Request-ID;
}
set req.url = querystring.filter(req.url, "service_id" + querystring.filtersep() + "req");
