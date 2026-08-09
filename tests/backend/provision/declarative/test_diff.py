"""Unit tests for diff computation between current and desired state."""

from backend.provision.declarative.diff import (
    Backend,
    LoggingEndpoint,
    VCLSnippet,
    compute_diff,
)


class TestDiffComputation:
    """Test diff computation logic."""

    def test_diff_detects_snippet_content_change(self):
        """Verify diff detects when snippet content changes."""
        current = [
            VCLSnippet(name="Fastly Log Analytics - vcl_recv", priority=10, body="old_vcl", subroutine="vcl_recv")
        ]
        desired = [
            VCLSnippet(name="Fastly Log Analytics - vcl_recv", priority=10, body="new_vcl", subroutine="vcl_recv")
        ]

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.snippets_to_update) == 1
        assert diff.snippets_to_update[0].name == "Fastly Log Analytics - vcl_recv"

    def test_diff_ignores_snippet_order(self):
        """Verify snippet order doesn't trigger false diffs."""
        s1 = VCLSnippet(name="S1", priority=10, body="c1", subroutine="vcl_recv")
        s2 = VCLSnippet(name="S2", priority=10, body="c2", subroutine="vcl_recv")

        current = [s1, s2]
        desired = [s2, s1]  # Different order

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.snippets_to_add) == 0
        assert len(diff.snippets_to_update) == 0
        assert len(diff.snippets_to_remove) == 0

    def test_diff_detects_new_snippet(self):
        """Verify diff detects new snippets."""
        current = []
        desired = [VCLSnippet(name="Fastly Log Analytics - vcl_recv", priority=10, body="vcl", subroutine="vcl_recv")]

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.snippets_to_add) == 1
        assert diff.snippets_to_add[0].name == "Fastly Log Analytics - vcl_recv"

    def test_diff_detects_removed_snippet(self):
        """Verify diff detects removed snippets."""
        current = [VCLSnippet(name="RUM - Recv", priority=100, body="old_rum_vcl", subroutine="vcl_recv")]
        desired = []

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.snippets_to_remove) == 1
        assert "RUM - Recv" in diff.snippets_to_remove

    def test_diff_detects_endpoint_change(self):
        """Verify diff detects endpoint changes."""
        current = [
            LoggingEndpoint(
                name="Fastly Log Analytics",
                endpoint_type="s3",
                path="raw/%Y/%m/%d/%H/log_%M.gz",
                period=60,
                response_condition="true",
                format_string="old_format",
                placement="none",
                response_object_name="",
            )
        ]
        desired = [
            LoggingEndpoint(
                name="Fastly Log Analytics",
                endpoint_type="s3",
                path="raw/%Y/%m/%d/%H/log_%M.gz",
                period=60,
                response_condition="true",
                format_string="new_format",
                placement="none",
                response_object_name="",
            )
        ]

        diff = compute_diff(
            current_snippets=[],
            desired_snippets=[],
            current_endpoints=current,
            desired_endpoints=desired,
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.endpoints_to_update) == 1

    def test_diff_detects_backend_change(self):
        """Verify diff detects backend changes."""
        current = [Backend(name="session_scorer", address="old.example.com", port=443)]
        desired = [Backend(name="session_scorer", address="new.example.com", port=443)]

        diff = compute_diff(
            current_snippets=[],
            desired_snippets=[],
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=current,
            desired_backends=desired,
        )
        assert len(diff.backends_to_update) == 1

    def test_diff_is_empty_when_no_changes(self):
        """Verify diff is empty when current matches desired."""
        snippet = VCLSnippet(name="S1", priority=10, body="code", subroutine="vcl_recv")
        endpoint = LoggingEndpoint(
            name="E1",
            endpoint_type="s3",
            path="path",
            period=60,
            response_condition="true",
            format_string="format",
            placement="none",
            response_object_name="",
        )
        backend = Backend(name="B1", address="addr", port=443)

        diff = compute_diff(
            current_snippets=[snippet],
            desired_snippets=[snippet],
            current_endpoints=[endpoint],
            desired_endpoints=[endpoint],
            current_backends=[backend],
            desired_backends=[backend],
        )
        assert diff.is_empty()

    def test_diff_summary_counts_correctly(self):
        """Verify diff.summary() counts changes correctly."""
        current = []
        desired = [
            VCLSnippet(name="S1", priority=10, body="c1", subroutine="vcl_recv"),
            VCLSnippet(name="S2", priority=10, body="c2", subroutine="vcl_recv"),
        ]

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        summary = diff.summary()
        assert summary["snippets_added"] == 2
        assert summary["snippets_updated"] == 0
        assert summary["snippets_removed"] == 0
