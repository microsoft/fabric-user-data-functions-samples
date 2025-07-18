# Fabric User data functions Samples 
You can find these sample code snippets for functions that can be used in Fabric User data functions.  These functions can also be available to users in Fabric portal and VS Code. The text in the sample file will be inserted at the end of the function_app.py file.

>![NOTE] These are not complete runnable samples. They are snippets that are inserted in User data function item in Fabric. 


**Fabric portal**
1. Open User data functions in the portal and select  **Insert samples** to add your code snippet. 
    ![Sample code snippet for Fabric User data functions in portal](images/sample-code-snippet-functions-fabric-portal.png)

**Fabric User data functions in VS Code**
1. Select ~**Local folder** and open **Functions**. Select **+** to add a new function from a sample. 
    ![Add function](images/add-function-in-VS-Code.png)
2. View all the sample categories to select a sample to add within function_app.py.
    ![View code samples in VS Code](images/view-code-samples-in-VS-Code.png)

 

## How to contribute

You can contribute to more function samples here. Follow the structure and checklist below:

Before submitting your sample function, ensure you complete all the following steps:

### 1. Code Preparation
- Write your sample function with proper docstring following the format to provide guidance to the user what the function does and how to use it. 
  ```python
  def functionname(<arguments>)-><output>:
  '''
  Description: Brief description of what the function does
  
  Args:
  - param1 (type): Description of parameter
  
  Returns: type: Description of return value
  '''
  ```
  - Include clear examples in your docstring
  - Document any special setup requirements in comments
- **DO NOT** include these lines in your code (they may exist in the base function_app.py):
  ```python
  import fabric.functions as fn
  udf = fn.UserDataFunctions()
  ```
- Include necessary import statements for libraries used in your function
- Use appropriate decorators `@udf.function()` before your function definition. 
- Use `@udf.connection()` decorator for any Fabric data source connections that your sample requires. 

### 2. File Organization
- Create your sample file as `<sample_name>.py` in the `PYTHON` folder
- Choose an appropriate subfolder or create a new one if existing subfolders don't match your content:
  - `Warehouse/` - Functions working with Fabric warehouses
  - `Lakehouse/` - Functions working with Fabric lakehouses  
  - `SQLDB/` - Functions working with SQL databases
  - `DataManipulation/` - Functions for data transformation and analysis
  - `UDFDataTypes/` - Functions demonstrating UDF SDK data types
  - Create new subfolder if your sample doesn't fit existing categories

### 3. Index.json Update
- Update the appropriate `PYTHON/index.json` file
- Add your sample entry with:
  - Descriptive `name` (shown in bold in QuickPick)
  - Clear `description` (shown at end of first line)
  - Optional `detail` (shown on second line)
  - Current `dateAdded` in ISO format (e.g., "2024-12-07T00:00:00Z")
  - Correct `data` path relative to PYTHON folder
- Ensure JSON syntax is valid (no trailing commas, proper brackets)

### 4. Testing and Validation
Test and validate your function code and share the conclusions of the test in the PR you submit. 
- Verify your function follows Fabric UDF patterns
- Test that required libraries are commonly available or document special requirements
- Ensure function handles errors appropriately

### 5. Submission
- Submit a Pull Request (PR) to the repository
- Include a clear description of what your sample does
- Mention any new dependencies or requirements
- Wait for product team review and address any feedback

### Sample code snippet example

```python

import pandas as pd

@udf.function()
def my_sample_function(data: list) -> dict:
    '''
    Description: Process input data and return summary statistics
    
    Args:
    - data (list): List of dictionaries containing numeric data
    
    Returns: dict: Summary statistics including mean and count
    '''
    df = pd.DataFrame(data)
    return {"mean": df.mean().to_dict(), "count": len(df)}
```



