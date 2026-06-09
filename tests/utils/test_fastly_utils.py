"""Tests for backend.core.fastly.utils."""

from backend.core.fastly.utils import load_vcl


def test_load_vcl_orders_auth_before_purge():
    """Verify that in the generated VCL, the authentication check and its
    accompanying 401 Unauthorized block are strictly defined before the
    unauthenticated-vulnerable FASTLYPURGE bypass shortcut.
    """
    vcl = load_vcl()
    assert vcl is not None

    # Find the positions of the key blocks in the generated VCL
    auth_err_pos = vcl.find('error 401 "Unauthorized"')
    purge_block_pos = vcl.find('if (req.method == "FASTLYPURGE")')

    assert auth_err_pos != -1, "Should find the authentication check block"
    assert purge_block_pos != -1, "Should find the FASTLYPURGE check block"

    assert auth_err_pos < purge_block_pos, (
        "Authentication check must strictly precede the FASTLYPURGE native execution to prevent unauthenticated cache evictions"
    )
