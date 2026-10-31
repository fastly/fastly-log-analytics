"""Bot source cache — fetch, store, and match against known bot registries."""

from __future__ import annotations

import ipaddress  # noqa: F401
import socket  # noqa: F401
import sys
import time  # noqa: F401

import aiodns  # noqa: F401
import pytricia  # noqa: F401

from backend.utils.date_utils import iso_z_now  # noqa: F401
from backend.utils.module_shim import install_shim_module
from backend.utils.telemetry import atomic_write_json  # noqa: F401

from . import cidr as _cidr
from . import fetcher as _fetcher
from . import rdns as _rdns
from . import registry as _registry
from .cidr import (
    CidrIntervals,
    _build_pytricia_trie,
    _compile_feed_intervals,
    _compile_feed_tries,
    _get_compiled_ip_feeds,
    _get_feed_intervals,
    _get_feed_tries,
    _is_ip_in_cidrs,
    collapse_cidr_ranges,
    collapse_network_objects,
    ip_in_ranges,
    ip_int_in_intervals,
    match_ip_to_bot_source,
    parse_ip_int_version,
)
from .fetcher import (
    _atomic_write_cache,
    _validate_feed_url,
    fetch_and_cache_source,
    fetch_bot_source,
    fetch_external_cidrs,
    http_get_text,
    refresh_all_sources,
    sync_all_bot_sources,
)
from .rdns import (
    _CONCURRENCY_LIMIT,
    _MAX_PENDING_BACKLOG,
    _MAX_TOTAL_ROWS,
    _REAP_INTERVAL_S,
    _RESOLVER_TIMEOUT,
    _STALE_ROW_RETENTION_DAYS,
    _bulk_update_async,
    _bulk_update_sync,
    _discover_new_ips,
    _discover_new_ips_gen,
    _do_lookup,
    _do_lookup_async,
    _get_pending_count,
    _last_enrichment_at,
    _last_reap_monotonic,
    _maybe_reap_stale_rows,
    _read_con,
    _reap_stale_rows,
    _resolve_batch_async,
    _run_async_resolve,
    _select_ips_with_status,
    _select_stale_ips,
    _write_con,
    _write_lock,
    classify,
    enqueue,
    enrich_batch,
    get_hostnames,
    get_stats,
)
from .registry import (
    _BOT_TOKEN_SUFFIX_RE,
    _CACHE_DIR,
    _DOMAIN_STEM_RE,
    _LITERAL_ESCAPES,
    _SEED_PATH,
    BOT_SOURCES,
    CATEGORY_LABELS,
    _BotBoundedTTLCache,
    _build_indexed_ua_matcher,
    _cache_mtime,
    _cache_path,
    _compiled_ip_feeds,
    _compiled_ip_feeds_lock,
    _extract_strict_required_literal,
    _matcher_cache,
    _matcher_lock,
    _seed_entries_cache,
    _seed_lock,
    _seed_matcher_fn,
    _source_cache,
    _source_cache_lock,
    _version_cache,
    _version_cache_lock,
    build_matcher,
    build_well_known_bots_matcher,
    enrich_bot_metadata,
    extract_entry_referral_stems,
    extract_literal_substring,
    get_all_sources_meta,
    get_bot_by_id,
    get_bot_regex_pattern,
    get_ilike_prefilter_literals,
    get_pattern_set_version,
    load_source,
    load_well_known_bots_catalog,
    resolve_bot_display_name,
)

__all__ = [
    "BOT_SOURCES",
    "CATEGORY_LABELS",
    "CidrIntervals",
    "_BOT_TOKEN_SUFFIX_RE",
    "_CACHE_DIR",
    "_CONCURRENCY_LIMIT",
    "_DOMAIN_STEM_RE",
    "_LITERAL_ESCAPES",
    "_MAX_PENDING_BACKLOG",
    "_MAX_TOTAL_ROWS",
    "_REAP_INTERVAL_S",
    "_RESOLVER_TIMEOUT",
    "_SEED_PATH",
    "_STALE_ROW_RETENTION_DAYS",
    "_BotBoundedTTLCache",
    "_atomic_write_cache",
    "_build_indexed_ua_matcher",
    "_build_pytricia_trie",
    "_bulk_update_async",
    "_bulk_update_sync",
    "_cache_mtime",
    "_cache_path",
    "_compile_feed_intervals",
    "_compile_feed_tries",
    "_compiled_ip_feeds",
    "_compiled_ip_feeds_lock",
    "_discover_new_ips",
    "_discover_new_ips_gen",
    "_do_lookup",
    "_do_lookup_async",
    "_extract_strict_required_literal",
    "_get_compiled_ip_feeds",
    "_get_feed_intervals",
    "_get_feed_tries",
    "_get_pending_count",
    "_is_ip_in_cidrs",
    "_last_enrichment_at",
    "_last_reap_monotonic",
    "_matcher_cache",
    "_matcher_lock",
    "_maybe_reap_stale_rows",
    "_read_con",
    "_reap_stale_rows",
    "_resolve_batch_async",
    "_run_async_resolve",
    "_seed_entries_cache",
    "_seed_lock",
    "_seed_matcher_fn",
    "_select_ips_with_status",
    "_select_stale_ips",
    "_source_cache",
    "_source_cache_lock",
    "_validate_feed_url",
    "_version_cache",
    "_version_cache_lock",
    "_write_con",
    "_write_lock",
    "build_matcher",
    "build_well_known_bots_matcher",
    "classify",
    "collapse_cidr_ranges",
    "collapse_network_objects",
    "enqueue",
    "enrich_batch",
    "enrich_bot_metadata",
    "extract_entry_referral_stems",
    "extract_literal_substring",
    "fetch_and_cache_source",
    "fetch_bot_source",
    "fetch_external_cidrs",
    "get_all_sources_meta",
    "get_bot_by_id",
    "get_bot_regex_pattern",
    "get_hostnames",
    "get_ilike_prefilter_literals",
    "get_pattern_set_version",
    "get_stats",
    "http_get_text",
    "ip_in_ranges",
    "ip_int_in_intervals",
    "load_source",
    "load_well_known_bots_catalog",
    "match_ip_to_bot_source",
    "parse_ip_int_version",
    "refresh_all_sources",
    "resolve_bot_display_name",
    "sync_all_bot_sources",
]

_self = sys.modules[__name__]
_ALL_MODULES = (_self, _registry, _cidr, _rdns, _fetcher)

_registry._get_compiled_ip_feeds = _get_compiled_ip_feeds
_registry._get_feed_intervals = _get_feed_intervals
_registry._get_feed_tries = _get_feed_tries
_registry.ip_int_in_intervals = ip_int_in_intervals
_registry.parse_ip_int_version = parse_ip_int_version
_registry.classify = classify
_registry.get_hostnames = get_hostnames

_ShimModule = install_shim_module(__name__, _ALL_MODULES, only_existing_attrs=True)
