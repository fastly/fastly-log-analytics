"""Single-source-of-truth FOS log path templates.

Three producers historically embedded their own layout literals:
the declarative generator (slashed layout, the one live buckets use),
``fastly_api.add_logging_endpoint`` and the admin logging-settings update
flow (old dash layout). A settings update would silently switch a bucket's
layout mid-stream. These tests pin every producer to the one canonical
template in ``backend.provision.log_paths``.
"""

from datetime import UTC, datetime

from backend.provision.log_paths import (
    ANALYTICS_LOG_LAYOUT,
    analytics_log_path,
    minute_list_prefix,
    rum_log_path,
    rum_minute_list_prefix,
)


class TestAnalyticsLogPath:
    def test_no_prefix(self):
        assert analytics_log_path("") == "/raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"

    def test_with_prefix(self):
        assert analytics_log_path("logs") == "logs/raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"

    def test_prefix_slashes_normalized(self):
        assert analytics_log_path("/logs/") == "logs/raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


class TestRumLogPath:
    def test_no_prefix(self):
        assert rum_log_path("") == "/rum/raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"

    def test_with_prefix(self):
        assert rum_log_path("logs") == "logs/rum/raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"


class TestMinuteListPrefix:
    """The bounded-LIST prefix a dispatcher uses to target one closed minute."""

    def test_no_prefix(self):
        dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
        assert minute_list_prefix("", dt) == "raw/year=2026/month=08/day=27/hour=16/minute=53/"

    def test_with_prefix(self):
        dt = datetime(2026, 8, 27, 16, 5, tzinfo=UTC)
        assert minute_list_prefix("logs", dt) == "logs/raw/year=2026/month=08/day=27/hour=16/minute=05/"


class TestRumMinuteListPrefix:
    """The bounded-LIST prefix the celery-mode RUM discovery job uses to
    target one closed minute of the rum/raw/ tree — never the regular-log
    raw/ tree."""

    def test_no_prefix(self):
        dt = datetime(2026, 8, 27, 16, 53, tzinfo=UTC)
        assert rum_minute_list_prefix("", dt) == "rum/raw/year=2026/month=08/day=27/hour=16/minute=53/"

    def test_with_prefix(self):
        dt = datetime(2026, 8, 27, 16, 5, tzinfo=UTC)
        assert rum_minute_list_prefix("logs", dt) == "logs/rum/raw/year=2026/month=08/day=27/hour=16/minute=05/"


class TestProducersUseCanonicalLayout:
    def test_declarative_generator_matches(self):
        import inspect

        import backend.provision.declarative.generators as gen

        src = inspect.getsource(gen)
        assert "raw/%Y-%m-%d" not in src, "declarative generator must not use the dash layout"
        assert "log_paths" in src, "declarative generator must build paths via log_paths"

    def test_fastly_api_has_no_dash_layout_literal(self):
        import inspect

        import backend.provision.fastly_api as fa

        src = inspect.getsource(fa)
        assert "raw/%Y-%m-%d" not in src, "add_logging_endpoint must use log_paths, not the dash layout"

    def test_services_router_has_no_dash_layout_literal(self):
        import inspect

        import backend.routers.services.core as sc

        src = inspect.getsource(sc)
        assert "raw/%Y-%m-%d" not in src, "logging-settings update must use log_paths, not the dash layout"

    def test_layout_constant_matches_live_bucket_shape(self):
        # Observed live key: raw/2026/08/27/16/analytics_log_53.json.gz<suffix>
        assert ANALYTICS_LOG_LAYOUT == "raw/year=%Y/month=%m/day=%d/hour=%H/minute=%M/"
