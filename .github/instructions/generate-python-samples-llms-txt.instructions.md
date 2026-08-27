**Expert Prompt for Generating Microsoft Fabric User Data Functions samples-llms.txt**

You are tasked with creating a comprehensive samples-llms.txt file for the Microsoft Fabric User Data Functions Python samples repository. Follow these specific requirements:

## Format Requirements
1. **Strictly follow the llms.txt specification format:**
   ```
   # Title
   
   > Optional description goes here
   
   Optional details go here
   
   ## Section name
   
   - [Link title](https://link_url): Optional link details
   
   ## Optional
   
   - [Link title](https://link_url)
   ```

## Content Requirements
1. **Read and analyze ALL Python sample files** in the repository structure:
   - DataManipulation/*.py
   - Lakehouse/*.py  
   - SQLDB/*.py
   - Warehouse/*.py
   - UDFDataTypes/*.py

2. **Use the attached `index.json` file as the authoritative source** for:
   - Section names (use the "name" field exactly)
   - Section descriptions (use the "description" field exactly)
   - Function names (use the "name" field exactly, not filenames)
   - Function descriptions (use the "description" field exactly)
   - Only include functions that exist in index.json (ignore any additional files)

3. **URL Structure Requirements:**
   - Use raw GitHub URLs for all links
   - Format: `https://raw.githubusercontent.com/microsoft/fabric-user-data-functions-samples/refs/heads/main/PYTHON/{folder}/{filename}.py`
   - Example: `https://raw.githubusercontent.com/microsoft/fabric-user-data-functions-samples/refs/heads/main/PYTHON/Warehouse/query_data_from_warehouse.py`

## Specific Structure
1. **Title:** "Microsoft Fabric User Data Functions - Python Samples"
2. **Description (blockquote):** Brief overview of serverless functions for Fabric platform
3. **Details paragraph:** Explain what the functions can do
4. **Sections:** Match index.json categories exactly:
   - Use category "name" as section header
   - Add category "description" as first line under each section
   - List functions using their display names from index.json, not filenames
   - Use function descriptions exactly as written in index.json

## Quality Checks
- Ensure no duplicate sections or links
- Verify all URLs are properly formatted
- Cross-reference with actual file structure to confirm files exist
- Maintain consistent formatting throughout
- Follow the exact order from index.json

## Key Details from Our Context
- Repository: `microsoft/fabric-user-data-functions-samples`
- Branch: `main`
- Directory: PYTHON
- Reference format from: https://llmstxt.org/llms.txt
- Must align perfectly with the provided index.json structure and content

## Output Requirements
**Create a complete, production-ready samples-llms.txt file that should be saved in the PYTHON directory of this repository (fabric-user-data-functions-samples/PYTHON/samples-llms.txt). This file serves as comprehensive documentation for LLMs to understand Microsoft Fabric User Data Functions Python samples.**