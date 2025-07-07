"""
Fabric UDF for unzipping files in lakehouse folders - Correct Implementation
Based on official Fabric samples: https://github.com/microsoft/fabric-user-data-functions-samples

SETUP INSTRUCTIONS:
1. Create data connections to your lakehouse(s) via "Manage connections"
2. Replace the alias values below with your actual connection aliases
3. Publish the functions using the Publish button
"""
import fabric.functions as fn
import logging

import zipfile
import io

udf = fn.UserDataFunctions()

# ===============================
# MAIN UNZIP FUNCTION - CORRECT IMPLEMENTATION
# ===============================

@udf.connection(argName="myLakehouse", alias="<My Lakehouse alias>")
@udf.function()
def unzip_file_in_lakehouse(
    myLakehouse: fn.FabricLakehouseClient,
    zipFilePath: str,
    extractToFolder: str
) -> dict:
    """
    Unzips a file within a lakehouse folder using the correct Fabric API.
    
    Args:
        zipFilePath: Path to zip file (e.g., "order.zip" - no "Files/" prefix needed)
        extractToFolder: Target folder name (e.g., "extracted")
    
    Returns:
        Dict with status, message, extracted_files list, and count
    
    Example usage:
        result = unzip_file_in_lakehouse("order.zip", "extracted")
    """
    try:
        # Set default value if not provided
        if not extractToFolder:
            extractToFolder = "extracted"
        
        logging.info(f"Starting unzip operation for: {zipFilePath}")
        logging.info(f"Target folder: {extractToFolder}")
            
        # Connect to the Lakehouse Files
        connection = myLakehouse.connectToFiles()
        logging.info("Successfully connected to lakehouse files")
        
        try:
            # Get the zip file client
            zipFile = connection.get_file_client(zipFilePath)
            logging.info(f"Got file client for: {zipFilePath}")
            
            # Download the zip file
            downloadFile = zipFile.download_file()
            zip_content = downloadFile.readall()
            logging.info(f"Downloaded zip file, size: {len(zip_content) if zip_content else 0} bytes")
            
            if not zip_content:
                logging.error(f"Zip file is empty or not found: {zipFilePath}")
                return {
                    "status": "error",
                    "message": f"Zip file is empty or not found: {zipFilePath}",
                    "extracted_files": [],
                    "extracted_count": 0
                }
            
            # Create BytesIO buffer from zip content
            zip_buffer = io.BytesIO(zip_content)
            extracted_files = []
            
            # Extract files from zip
            with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                logging.info(f"Found {len(file_list)} files in zip archive")
                
                for file_name in file_list:
                    # Skip directories
                    if file_name.endswith('/'):
                        logging.info(f"Skipping directory: {file_name}")
                        continue
                        
                    try:
                        # Read file content from zip
                        file_content = zip_ref.read(file_name)
                        logging.info(f"Extracting file: {file_name} ({len(file_content)} bytes)")
                        
                        # Create target path (folder/filename)
                        target_path = f"{extractToFolder}/{file_name}"
                        
                        # Get file client for the target path
                        targetFile = connection.get_file_client(target_path)
                        
                        # Upload the extracted file content
                        targetFile.upload_data(file_content, overwrite=True)
                        logging.info(f"Successfully uploaded: {target_path}")
                        
                        extracted_files.append(target_path)
                        
                    except Exception as e:
                        logging.error(f"Failed to extract {file_name}: {str(e)}")
                        continue  # Skip files that fail to extract
            
            # Close connections
            zipFile.close()
            connection.close()
            logging.info("Closed all connections")
            
            logging.info(f"Extraction completed successfully! Extracted {len(extracted_files)} files")
            return {
                "status": "success",
                "message": f"Successfully extracted {len(extracted_files)} files",
                "extracted_files": extracted_files,
                "extracted_count": len(extracted_files),
                "source_zip": zipFilePath,
                "target_folder": extractToFolder
            }
            
        except Exception as e:
            logging.error(f"Error during zip processing: {str(e)}")
            # Make sure to close connection even if error occurs
            try:
                connection.close()
                logging.info("Closed connection after error")
            except:
                pass
            
            return {
                "status": "error",
                "message": f"Unable to read zip file: {zipFilePath}. Error: {str(e)}",
                "extracted_files": [],
                "extracted_count": 0
            }
        
    except zipfile.BadZipFile:
        logging.error(f"Invalid zip file: {zipFilePath}")
        return {
            "status": "error",
            "message": f"Invalid zip file: {zipFilePath}",
            "extracted_files": [],
            "extracted_count": 0
        }
    except Exception as e:
        logging.error(f"Unexpected error in unzip_file_in_lakehouse: {str(e)}")
        return {
            "status": "error",
            "message": f"Error: {str(e)}",
            "extracted_files": [],
            "extracted_count": 0
        }


# ===============================
# LIST ZIP CONTENTS FUNCTION
# ===============================

@udf.connection(argName="myLakehouse", alias="<My Lakehouse alias>")
@udf.function()
def list_zip_contents(
    myLakehouse: fn.FabricLakehouseClient,
    zipFilePath: str
) -> dict:
    """
    Lists contents of a zip file without extracting using correct Fabric API.
    
    Args:
        zipFilePath: Path to zip file (e.g., "data.zip")
    
    Returns:
        Dict with status, file list, and metadata
    
    Example usage:
        contents = list_zip_contents("data.zip")
    """
    try:
        logging.info(f"Listing contents of zip file: {zipFilePath}")
        
        # Connect to the Lakehouse Files
        connection = myLakehouse.connectToFiles()
        logging.info("Successfully connected to lakehouse files")
        
        try:
            # Get the zip file client
            zipFile = connection.get_file_client(zipFilePath)
            logging.info(f"Got file client for: {zipFilePath}")
            
            # Download the zip file
            downloadFile = zipFile.download_file()
            zip_content = downloadFile.readall()
            logging.info(f"Downloaded zip file, size: {len(zip_content) if zip_content else 0} bytes")
            
            if not zip_content:
                logging.error(f"Zip file is empty or not found: {zipFilePath}")
                return {
                    "status": "error",
                    "message": f"Zip file is empty or not found: {zipFilePath}",
                    "files": [],
                    "file_count": 0
                }
            
            # Create BytesIO buffer from zip content
            zip_buffer = io.BytesIO(zip_content)
            file_info = []
            
            # List files in zip
            with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
                for info in zip_ref.infolist():
                    file_info.append({
                        "filename": info.filename,
                        "file_size": info.file_size,
                        "compressed_size": info.compress_size,
                        "is_directory": info.filename.endswith('/')
                    })
                    logging.info(f"Found: {info.filename} ({info.file_size} bytes)")
            
            # Close connections
            zipFile.close()
            connection.close()
            logging.info("Closed all connections")
            
            logging.info(f"Found {len(file_info)} items in zip file")
            return {
                "status": "success",
                "message": f"Found {len(file_info)} items in zip file",
                "files": file_info,
                "file_count": len(file_info),
                "zip_path": zipFilePath
            }
            
        except Exception as e:
            logging.error(f"Error during zip content listing: {str(e)}")
            # Make sure to close connection even if error occurs
            try:
                connection.close()
                logging.info("Closed connection after error")
            except:
                pass
            
            return {
                "status": "error",
                "message": f"Unable to read zip file: {zipFilePath}. Error: {str(e)}",
                "files": [],
                "file_count": 0
            }
        
    except zipfile.BadZipFile:
        logging.error(f"Invalid zip file: {zipFilePath}")
        return {
            "status": "error",
            "message": f"Invalid zip file: {zipFilePath}",
            "files": [],
            "file_count": 0
        }
    except Exception as e:
        logging.error(f"Unexpected error in list_zip_contents: {str(e)}")
        return {
            "status": "error",
            "message": f"Error: {str(e)}",
            "files": [],
            "file_count": 0
        }


# ===============================
# TEST LAKEHOUSE CONNECTION
# ===============================

@udf.connection(argName="myLakehouse", alias="<My Lakehouse alias>")
@udf.function()
def test_lakehouse_connection(myLakehouse: fn.FabricLakehouseClient) -> dict:
    """
    Test function to verify lakehouse connection using correct Fabric API.
    """
    try:
        logging.info("Testing lakehouse connection...")
        
        # Connect to the Lakehouse Files
        connection = myLakehouse.connectToFiles()
        logging.info("Successfully connected to lakehouse files")
        
        # Test basic connection
        connection_info = {
            "connection_type": str(type(connection)),
            "lakehouse_type": str(type(myLakehouse)),
            "connection_established": True
        }
        logging.info(f"Connection info: {connection_info}")
        
        # Close connection
        connection.close()
        logging.info("Closed connection successfully")
        
        return {
            "status": "success",
            "message": "Lakehouse connection successful",
            "connection_info": connection_info
        }
        
    except Exception as e:
        logging.error(f"Connection test failed: {str(e)}")
        return {
            "status": "error",
            "message": f"Connection test failed: {str(e)}",
            "connection_info": {}
        }


# ===============================
# USAGE EXAMPLES FUNCTION
# ===============================

@udf.function()
def get_unzip_examples() -> dict:
    """
    Returns usage examples for the unzip functions.
    """
    return {
        "step_1": "First run: test_lakehouse_connection() to verify connection",
        "step_2": "Upload your zip file to the lakehouse Files folder",
        "step_3": "Run: unzip_file_in_lakehouse('your_file.zip', 'extracted')",
        "basic_unzip": "unzip_file_in_lakehouse('data.zip', 'extracted')",
        "custom_folder": "unzip_file_in_lakehouse('order.zip', 'processed_orders')",
        "list_contents": "list_zip_contents('archive.zip')",
        "setup_reminder": "Replace '<My Lakehouse alias>' with your actual lakehouse connection alias",
        "file_paths": "Use just the filename, no 'Files/' prefix needed (e.g., 'data.zip' not 'Files/data.zip')",
        "note": "Files are extracted to the lakehouse Files folder in the specified subfolder"
    }


# ===============================
# SETUP INSTRUCTIONS
# ===============================
"""
CORRECT SETUP BASED ON OFFICIAL FABRIC SAMPLES:

1. ✅ Create lakehouse connection:
   - Click "Manage connections" in Functions portal
   - Add your lakehouse from OneLake catalog
   - Copy the generated alias

2. ✅ Update code:
   - Replace "<My Lakehouse alias>" with your actual alias
   - Example: @udf.connection(argName="myLakehouse", alias="MyDataLakehouse")

3. ✅ Upload test file:
   - Upload a zip file directly to your lakehouse Files folder
   - Note the filename (e.g., "test.zip")

4. ✅ Test and run:
   - Publish functions using "Publish" button
   - Run test_lakehouse_connection() to verify
   - Run unzip_file_in_lakehouse("test.zip", "extracted")
   - Check lakehouse Files/extracted/ folder for results



EXAMPLE USAGE:
# Test connection first
test_result = test_lakehouse_connection()
print(test_result)

# List zip contents
contents = list_zip_contents("order.zip")
print(f"Found {contents['file_count']} files")

# Extract zip file
result = unzip_file_in_lakehouse("order.zip", "extracted")
if result["status"] == "success":
    print(f"Extracted {result['extracted_count']} files to {result['target_folder']}")
    for file_path in result["extracted_files"]:
        print(f"  - {file_path}")
"""
