"""Visibility fence shared by the high-scale readers of fact tables.

Fact rows are inserted behind a pending publication row, so a reader must require the batch's
latest publication row to be ``visible`` or it can surface a partially inserted batch.
"""

from __future__ import annotations


def visible_fact_clauses(domain: str) -> list[str]:
    """Predicates (with a ``{service_id:String}`` parameter) limiting a fact table to visible batches of ``domain``."""
    return [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        f"WHERE service_id={{service_id:String}} AND domain='{domain}' "
        "AND publication_state='visible')",
    ]
