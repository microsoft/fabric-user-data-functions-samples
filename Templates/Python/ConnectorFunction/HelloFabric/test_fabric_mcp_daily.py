"""Transport-only raw SSE relay probe with a private synthetic token."""

from test_fabric_mcp import _Response, _invoke, _load_function_app, _request


_DAILY_ENDPOINT = "https://dailyapi.fabric.microsoft.com/v1/mcp/fabriciq"


def test_daily_harness_relays_final_opaque_object_and_private_token(monkeypatch):
    app = _load_function_app()
    monkeypatch.setattr(app, "_FABRIC_MCP_ENDPOINT", _DAILY_ENDPOINT)
    response = _Response(
        raw=(
            b"event: custom\n"
            b'data: {"intermediate":{"opaque":true}}\n\n'
            b"event: final-custom\n"
            b'data: {"final":{"opaque":true}}\n\n'
        ),
        content_type="text/event-stream",
    )
    output, session = _invoke(
        app,
        _request(message={"opaqueRequest": True}),
        [response],
        "private-token",
    )
    assert output == {"message": {"final": {"opaque": True}}}
    assert session.requests[0]["url"] == _DAILY_ENDPOINT
    assert "private-token" not in repr(output)
