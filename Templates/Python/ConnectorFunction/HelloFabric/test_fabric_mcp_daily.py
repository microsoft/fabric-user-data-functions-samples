"""Transport-only relay probe with a private synthetic token."""

from test_fabric_mcp import _Response, _invoke, _load_function_app, _request


_DAILY_ENDPOINT = "https://dailyapi.fabric.microsoft.com/v1/mcp/fabriciq"


def test_daily_harness_relays_opaque_text_and_private_token(monkeypatch):
    app = _load_function_app()
    monkeypatch.setattr(app, "_FABRIC_MCP_ENDPOINT", _DAILY_ENDPOINT)
    upstream = 'data: {"opaque":true}\n\n'
    output, session = _invoke(
        app,
        _request(message={"opaqueRequest": True}),
        [_Response(upstream)],
        "private-token",
    )
    assert output == {"message": upstream}
    assert session.requests[0]["url"] == _DAILY_ENDPOINT
    assert "private-token" not in repr(output)
