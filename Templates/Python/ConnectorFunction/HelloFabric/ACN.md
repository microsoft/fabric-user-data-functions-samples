# ACN connector runtime

The template's `Deploy.zip` and `SourceCode.zip` pin `azure-connectors==1.0.0`.
Update both archives together with `function_app.py`; changing the checked-in
Python source alone does not update the deployed template.

The public `rayfin_*_v1` entry points and camelCase operation names are unchanged.
Internally, the adapter uses the SDK's renamed modules and methods, explicit
async `DefaultAzureCredential` (preserving the old SDK's managed-identity/default
credential chain), and typed numeric and Boolean parameters.
Platform-supplied access tokens use a single-invocation async bearer credential;
they are never interpreted as API keys or cached across callers.

List operations fetch exactly one SDK page and return its original JSON envelope,
including null fields and continuation metadata. They do not automatically fetch
all pages. `searchUserAsync` has no continuation-token input in SDK 1.0.
The adapter does not accept arbitrary next-link URLs.

Planner PATCH operations use the SDK's empty-success response handling. A 204
returns JSON `null`, not an error. Connector and transport failures retain the
existing UDF error envelope.

## Local checks

From this directory, in an isolated Python environment:

```powershell
python -m pip install azure-connectors==1.0.0
python -B -m unittest discover -s . -p "test_acn*.py"
```

`test_acn_connectors.py` covers adapter dispatch and error behavior.
`test_acn_sdk.py` executes the released clients against substituted HTTP responses,
including Planner 204s, all admitted operation signatures, first-page envelopes,
and bearer-token policy behavior. These are local contract regressions, not proof
that a deployed Daily connection or upstream connector is healthy.
