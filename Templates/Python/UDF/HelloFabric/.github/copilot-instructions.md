## Copilot Instructions — Fabric UDF

Primary objective: Generate correct Microsoft Fabric User Data Functions (UDFs) in Python with strict grounding, zero fabrication, and minimal verbosity.

### Constitutional Rules (Always)
- Conceptual grounding: All conceptual / architectural / policy / security / performance answers MUST be grounded ONLY in Microsoft Docs via the Microsoft Docs MCP server. If that server (`microsoft.docs.mcp`) is not available, respond with EXACTLY (no extra words): "Microsoft Docs MCP server not available. Install/enable it: https://learn.microsoft.com/en-us/training/support/mcp-get-started#configure-vs-code"
- Sample grounding: For any code generation or modification, first fetch `samples-llms.txt` from the official GitHub samples index (#fetch https://raw.githubusercontent.com/microsoft/fabric-user-data-functions-samples/refs/heads/main/PYTHON/samples-llms.txt) and then fetch at least one concrete Python sample it links to. Implement strictly by adapting the closest single sample. If no sample is available, state that explicitly and do not invent APIs.
- UDF essentials: Follow the mandatory UDF practices below (single-file layout, parameter casing, connections/placeholders, warnings, validation, and response schema).

### Scope & Layout
- "Function" means a Fabric UDF entry point in this repo.
- Single-file code: Place all executable UDF code in `function_app.py`. Only add small helpers in the same file unless a separate module is clearly necessary.
- No alternate hosting/execution models unless the user explicitly requests a pivot away from Fabric UDFs.

### Sample-First Workflow (Code Tasks)
1) Fetch index: Retrieve `samples-llms.txt` from GitHub and parse it.
2) Fetch sample: Follow a #fetch link to a concrete Python sample that best matches the needed binding/decorator pattern. Selection order:
   - Same trigger/binding decorators; else closest semantics; else smallest working example.
3) Adapt patterns: Reuse the sample’s binding signatures, decorators, parameter naming style, logging, response envelope, and error handling. Do not combine multiple unique sample patterns unless necessary.
4) Cite: Name the sample file used (file path only). Avoid large quotations.
5) If a matching sample cannot be fetched: say so explicitly and implement the safest minimal, clearly marked "Unverified — needs doc lookup".

### Concept Questions (Docs Tasks)
- Source: Microsoft Docs through active MCP only; paraphrase concisely.
- If MCP missing: return only the directive line above—no additions.

### UDF Essentials
- Naming: function names snake_case; parameters camelCase.
- Parameter enforcement:
  - Public UDF parameters MUST be lowerCamelCase. Convert snake_case inputs to camelCase (`user_id` → `userId`) and proceed; add a short inline comment mapping if relevant.
  - Do not leave underscores in parameter names. If conflict occurs after conversion, append a numeric suffix (e.g., `userId2`).
  - Signature defaults: PROHIBITED. No '=' in the parameter list (covers `None`, union/optional, numeric/string literals, splat defaults). All params are required.
- Validation: At start, validate required parameters for None/empty/type; return the sample’s standard error envelope or raise ValueError per sample pattern.
 - Logging: Use standard library logging directly (`import logging` then calls like `logging.info(...)`); Use info for start/end and key branches, warning for recoverable anomalies, error for failures; never silently swallow exceptions.
- Response schema: Mirror the sample’s schema exactly (commonly `status`/`result`/`error`). Add fields only if explicitly requested.

### Dependencies, Connections, Warnings
- Prefer stdlib. Add external libraries only with explicit user need or clear benefit (perf/correctness/security). Place custom wheels in `privateLibraries/` if used.
- Connections: Verify configured alias names. If unknown, insert a clear placeholder such as `PLACEHOLDER_DATA_LAKE_CONN  # TODO: replace with configured connection alias` and proceed.
- `definition.json` (if present) outranks assumptions; align imports/bindings to it.
- Warning policy: Emit a single bold warning only when introducing a new external library or adding an unverified connection alias (or changing prior connection assumptions). Do not repeat the same warning in later turns unless a new item appears.

### Guardrails
- Do not invent APIs or bindings not seen in samples or docs.
- Keep edits minimal and focused on the request; avoid unrelated refactors.
- If uncertainty remains after samples/docs, choose the safest minimal approach and label it Unverified.

### Output Discipline
- Be concise. Summarize what changed and why in a few bullets.
- Reference the explicit sample by filename/path. Avoid long code pastes from samples; adapt the pattern instead.
