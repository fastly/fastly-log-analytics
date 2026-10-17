"""Wire models for Streaming → Content Security policies and assignments."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, StringConstraints

RuleCategory = Literal["country", "asn", "referer", "host", "proxy_type", "proxy_description"]
LiveCategory = Literal["country", "asn", "referer", "host", "proxy_type", "proxy_description", "subscriber_id", "ip"]
RuleAction = Literal["block", "pace", "exempt"]
AssignmentMode = Literal["WARN", "ENFORCE"]
AssignmentScope = Literal["default", "content_id"]
AssignmentStatus = Literal["scheduled", "active", "ended"]

_Value = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=2048)]


class PolicyRuleItem(BaseModel):
    value: _Value
    action: RuleAction
    # Throttle rate in kbps; 0 unless action is "pace" (the API's name for throttle).
    kbps: Annotated[int, Field(ge=0, le=100_000_000)] = 0
    # Display name of an ASN rule, "Comcast Cable (7922)" or "AS7922" (set on responses; ignored on requests).
    label: str | None = None


class PolicyRules(BaseModel):
    country: list[PolicyRuleItem] = []
    asn: list[PolicyRuleItem] = []
    referer: list[PolicyRuleItem] = []
    host: list[PolicyRuleItem] = []
    proxy_type: list[PolicyRuleItem] = []
    proxy_description: list[PolicyRuleItem] = []


class PolicyIndividual(BaseModel):
    """Individual blocks. Take precedence over exemptions."""

    subscriber_id: list[_Value] = []
    ip: list[_Value] = []


class PolicyBody(BaseModel):
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=120)]
    # Matched against the uppercased 2-letter country code; matches are blocked.
    country_regex: Annotated[str, StringConstraints(max_length=200)] | None = None
    rules: PolicyRules = PolicyRules()
    individual: PolicyIndividual = PolicyIndividual()


class PolicyUse(BaseModel):
    assignment_id: str
    scope_type: AssignmentScope
    content_id: str | None = None
    mode: AssignmentMode
    status: AssignmentStatus


class ContentSecurityPolicy(BaseModel):
    id: str
    name: str
    country_regex: str | None = None
    rev: int
    rules: PolicyRules
    individual: PolicyIndividual
    created_at: str
    created_by: str
    updated_at: str
    updated_by: str
    # Assignments that point at this policy, so an edit's reach is visible.
    used_by: list[PolicyUse] = []


class PublishResult(BaseModel):
    # published | not_configured (no blocker URL) | failed (saved, but the blocker did not accept it)
    status: Literal["published", "not_configured", "failed"]
    rev: int
    error: str | None = None


class PolicyResult(BaseModel):
    policy: ContentSecurityPolicy
    publish: PublishResult


class PoliciesResponse(BaseModel):
    configured: bool
    policies: list[ContentSecurityPolicy]


class AssignmentBody(BaseModel):
    policy_id: str
    scope_type: AssignmentScope
    content_id: Annotated[str, StringConstraints(strip_whitespace=True, max_length=512)] | None = None
    mode: AssignmentMode = "ENFORCE"
    # ISO-8601; null = unbounded. Start inclusive, end exclusive.
    starts_at: str | None = None
    ends_at: str | None = None


class ContentSecurityAssignment(BaseModel):
    id: str
    policy_id: str
    policy_name: str | None = None
    scope_type: AssignmentScope
    content_id: str | None = None
    mode: AssignmentMode
    starts_at: str | None = None
    ends_at: str | None = None
    status: AssignmentStatus
    created_at: str
    created_by: str


class AssignmentsResponse(BaseModel):
    configured: bool
    assignments: list[ContentSecurityAssignment]


class AssignmentResult(BaseModel):
    assignment: ContentSecurityAssignment
    publish: PublishResult


class RuleBody(BaseModel):
    category: LiveCategory
    value: _Value
    action: RuleAction = "block"
    kbps: Annotated[int, Field(ge=0, le=100_000_000)] = 0


class LiveRuleBody(RuleBody):
    # The content being viewed; selects the policy in effect. Null = the service default.
    content_id: Annotated[str, StringConstraints(strip_whitespace=True, max_length=512)] | None = None


class LiveRuleResult(BaseModel):
    policy: ContentSecurityPolicy
    assignment: ContentSecurityAssignment
    # True when no policy was in effect and a Default policy and assignment were created.
    created_default: bool
    publish: PublishResult


class EffectivePolicyResponse(BaseModel):
    content_id: str | None = None
    assignment: ContentSecurityAssignment | None = None
    policy: ContentSecurityPolicy | None = None
    # Other assignments (in this service) that share the policy; the reach of a live edit.
    shared_with: int = 0
    configured: bool


class ContentSecuritySyncStatusResponse(BaseModel):
    configured: bool
    rev: int
    published_rev: int
    in_sync: bool
    last_ok_at: str | None = None
    last_error: str | None = None


class RevisionItem(BaseModel):
    rev: int
    changed_by: str
    changed_at: str
    policy: ContentSecurityPolicy


class RevisionsResponse(BaseModel):
    revisions: list[RevisionItem]


class RegexPreviewResponse(BaseModel):
    valid: bool
    error: str | None = None
    # ISO 3166-1 alpha-2 codes the pattern blocks.
    matches: list[str] = []
