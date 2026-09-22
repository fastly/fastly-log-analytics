from unittest.mock import MagicMock

import pytest

from backend.core import clickhouse_observer as observer


@pytest.fixture
def monitor(monkeypatch):
    client, store, meter = MagicMock(), MagicMock(), MagicMock()
    client.storage_health.return_value = {"up": 1, "disk_free": 40, "disk_total": 100}
    store.publication_lag_seconds.return_value = 12.5
    monkeypatch.setattr(observer, "get_meter", lambda: meter)
    monkeypatch.setattr(observer, "PgManifest", lambda: store)
    sampler = observer.ClickHouseObserver()
    sampler.start(client)
    yield sampler, client, store, meter
    sampler.stop()


def test_gauges_are_lazy_and_share_one_cheap_sample(monitor):
    sampler, client, store, meter = monitor
    client.storage_health.assert_not_called()
    names = {call.args[0] for call in meter.create_observable_gauge.call_args_list}
    assert names == {
        "app.clickhouse_up",
        "app.clickhouse_disk_free",
        "app.clickhouse_disk_total",
        "app.clickhouse_publication_lag_seconds",
    }
    assert sampler.observe("up")[0].value == 1
    assert sampler.observe("disk_free")[0].value == 40
    assert sampler.observe("disk_total")[0].value == 100
    assert sampler.observe("lag")[0].value == 12.5
    client.storage_health.assert_called_once_with()
    store.publication_lag_seconds.assert_called_once_with()
    assert all(not item.attributes for item in sampler.observe("lag"))


def test_errors_are_unknown_except_health_zero_and_never_keep_old_values(monitor):
    sampler, client, store, _ = monitor
    assert sampler.observe("lag")[0].value > 0
    client.storage_health.side_effect = RuntimeError("PRIVATE connection")
    store.publication_lag_seconds.side_effect = RuntimeError("PRIVATE DSN")
    sampler._sampled_at = 0
    assert sampler.observe("up")[0].value == 0
    assert sampler.observe("disk_free") == []
    assert sampler.observe("disk_total") == []
    assert sampler.observe("lag") == []
    client.storage_health.side_effect = None
    store.publication_lag_seconds.side_effect = None
    store.publication_lag_seconds.return_value = 0
    sampler._sampled_at = 0
    assert sampler.observe("lag")[0].value == 0
    assert sampler.observe("up")[0].value == 1


def test_stop_disables_callbacks_and_closes_client_restart_reuses_gauges(monitor):
    sampler, client, _, meter = monitor
    sampler.stop()
    sampler.stop()
    assert sampler.observe("up") == []
    client.close.assert_called_once_with()
    sampler.start(client)
    assert meter.create_observable_gauge.call_count == 4
    assert sampler.observe("up")[0].value == 1


def test_disabled_does_not_create_client_or_gauges(monkeypatch):
    monkeypatch.setattr(observer.config, "load_clickhouse_config", lambda: None)
    make = MagicMock(side_effect=AssertionError("opened disabled client"))
    monkeypatch.setattr(observer, "ClickHouseClient", make)
    observer.start_clickhouse_observer()
    make.assert_not_called()
