# Host extension version (maintainer note)

`Deploy.zip` / `SourceCode.zip` in this folder ship a **pre-built `bin/`** that contains the
Azure Functions Fabric host extension DLLs. Daily / Edog deploy that `bin/` **as-is**.

> ⚠️ The `Version="1.*-*"` pin in `extensions.csproj` is only applied when the project is
> **built**. It is **not** re-resolved at deploy time. Editing the csproj alone does **not**
> change the host version a UDF actually runs — whatever DLL is baked into `bin/` is what runs.

## To ship a newer host you must rebuild `bin/` and repackage both zips

From a machine authenticated to the `AppDev_FunctionSet_Official` NuGet feed:

```powershell
# 1. Build the extensions project ("1.*-*" resolves to the latest published 1.x host)
dotnet restore extensions.csproj
dotnet build -c Release extensions.csproj

# 2. Replace the bin/ entries inside Deploy.zip with the contents of bin/Release/net6.0/*
# 3. Copy Deploy.zip over SourceCode.zip  (both zips MUST be byte-identical)
```

Verify before opening a PR:

- `bin/Microsoft.Azure.WebJobs.Extensions.Fabric.dll` FileVersion is the new version.
- `bin/extensions.json` references that same `Version=`.
- `Deploy.zip` and `SourceCode.zip` are byte-identical.

The version a running UDF logs is: `Azure Functions Fabric Extension Version: <x.y.z>` (function app Invocation Logs).
