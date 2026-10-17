"""``_mask_ip_scalar`` drops the original padding of a masked value; that is intentional and pinned here."""

from __future__ import annotations

import pytest

from backend.core.share_db.validation import _mask_ip_scalar, mask_ip

pytestmark = pytest.mark.security_regression


@pytest.mark.parametrize("padded", ["  1.2.3.4", "1.2.3.4\n", "\t1.2.3.4 \r\n", " 2001:db8::1 "])
def test_padded_ip_is_masked_and_padding_is_not_preserved(padded):
    out = _mask_ip_scalar(padded)
    assert out == mask_ip(padded.strip())
    assert out == out.strip()


def test_xff_list_is_normalised_to_comma_space_separated_masked_elements():
    assert _mask_ip_scalar("1.2.3.4,5.6.7.8") == ", ".join([mask_ip("1.2.3.4"), mask_ip("5.6.7.8")])
    assert _mask_ip_scalar(" 1.2.3.4 ,\n5.6.7.8 ") == ", ".join([mask_ip("1.2.3.4"), mask_ip("5.6.7.8")])


def test_non_ip_values_keep_their_padding_verbatim():
    assert _mask_ip_scalar("  hello world\n") == "  hello world\n"
    assert _mask_ip_scalar("1.2.3.4, not-an-ip ") == "1.2.3.4, not-an-ip "
