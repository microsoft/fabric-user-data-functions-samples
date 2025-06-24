# Copilot Instructions for Fabric User Data Functions

## Project Overview
This is a Microsoft Fabric User Data Functions (UDF) project written in Python. UDFs are serverless functions that process data within Fabric environments.

## Key Information for Code Generation

### Project Structure
- **All code goes in `function_app.py`** - single file for all functions
- `definition.json` - contains configured libraries and connections

### Dependencies Management
- **⚠️ CRITICAL**: Always check `definition.json` for configured libraries before using imports
- **You can add libraries to `requirements.txt`** but **warn in bold** that user must configure them in Fabric portal
- **Warn in bold** when suggesting libraries not in `definition.json`
- Libraries added locally won't work in production until configured in Fabric portal
- Custom packages go in `privateLibraries/` as wheel files

### Connections
- **⚠️ CRITICAL**: Check `definition.json` "connectedDataSources" before using connections
- **You can suggest connection code** but **warn in bold** that user must configure connections in Fabric portal if not in `definition.json`
- **Connections won't work in production** until configured in Fabric portal
- Connections must be added via Fabric portal

### Function Development Best Practices
- Use descriptive snake_case names for functions
- **Use camelCase for parameter names** (never snake_case for parameters)
- **Always include inline Python docstrings** to describe what functions do
- Include proper error handling and logging
- Test locally with F5 debugging before deployment

### Available VS Code Tasks
- `func: host start` - Start function host
- `pip install (functions)` - Install dependencies  

### When Generating Code
1. **Always check dependencies** against `definition.json`
2. **Always check connections** against `definition.json`
3. **Include type hints** for all functions
4. **Add logging** for debugging
5. **Include error handling**
6. **Warn boldly** about unconfigured libraries/connections

### Reference Files
- Use samples from the [Python samples folder](https://raw.githubusercontent.com/microsoft/fabric-user-data-functions-samples/refs/heads/main/PYTHON/samples-llms.txt) for patterns
- Reference the [Python SDK documentation](https://raw.githubusercontent.com/microsoft/fabric-user-data-functions-samples/refs/heads/main/PYTHON/sdk-llms-full.txt) for available functions
- Follow Microsoft Fabric UDF documentation standards