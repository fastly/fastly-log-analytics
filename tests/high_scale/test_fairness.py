from backend.high_scale.fairness import FairScheduler, WorkItem


def test_each_active_service_gets_minimum_progress() -> None:
    scheduler = FairScheduler()
    scheduler.register("svc-a", weight=1)
    scheduler.register("svc-b", weight=1)
    scheduler.enqueue(WorkItem("svc-a", 1, "a1"))
    scheduler.enqueue(WorkItem("svc-a", 1, "a2"))
    scheduler.enqueue(WorkItem("svc-b", 1, "b1"))

    dispatched = scheduler.dispatch(2)

    assert {item.payload for item in dispatched} == {"a1", "b1"}


def test_idle_capacity_is_borrowed_by_weight() -> None:
    scheduler = FairScheduler()
    scheduler.register("svc-a", weight=2)
    scheduler.register("svc-b", weight=1)
    for index in range(4):
        scheduler.enqueue(WorkItem("svc-a", 1, f"a{index}"))
        scheduler.enqueue(WorkItem("svc-b", 1, f"b{index}"))

    dispatched = scheduler.dispatch(6)

    assert [item.service_id for item in dispatched[:3]] == ["svc-a", "svc-b", "svc-a"]
    assert len(dispatched) == 6


def test_oversized_work_item_waits_for_capacity() -> None:
    scheduler = FairScheduler()
    scheduler.register("svc-a")
    scheduler.enqueue(WorkItem("svc-a", 3, "large"))

    assert scheduler.dispatch(2) == ()
    assert scheduler.dispatch(3)[0].payload == "large"
