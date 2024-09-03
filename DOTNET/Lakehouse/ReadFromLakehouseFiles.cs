using Azure.Storage.Files.DataLake;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using Microsoft.Azure.Functions.Worker.Extensions.Fabric.Attributes;
using System.Text;

namespace HelloFabric
{
    public class FabricFunctions
    {
        private readonly ILogger _logger;

        public FabricFunctions(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<FabricFunctions>();
        }

        // Example of using a FabricItemInput to query a Warehouse and then write the data to a csv in a Lakehouse
        // Uncomment and fill in the Warehouse alias and Lakehouse alias you would like to use 
        [Function("QueryWarehouseAndWriteToLakehouseCsv")]
         public async Task<IEnumerable<object>> QueryWarehouseAndWriteToCsv([FabricItemInput("<My Warehouse Alias>")] SqlConnection myWarehouse, [FabricItemInput("<My Lakehouse Alias>")] DataLakeDirectoryClient myLakehouse)
         {
            myWarehouse.Open();

            // You can use the SqlConnection to run sql against the item specified
            string query = $"SELECT * FROM (VALUES ('John Smith',  31) , ('Kayla Jones', 33)) AS Employee(EmpName, DepID);";

            using SqlCommand command = new SqlCommand(query, myWarehouse);
            using var reader = await command.ExecuteReaderAsync();

            // See below link for ADO.net SqlClient examples. We will be converting the results to csv format
            // https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-code-examples

            StringBuilder csvBuilder = new StringBuilder();
            csvBuilder.AppendLine("EmpName,DepID");

            while (reader.Read())
            {
                var empName = reader["EmpName"];
                var depId = reader["DepID"];
                csvBuilder.AppendLine($"{empName},{depId}");
            }

            string csv = csvBuilder.ToString();
            
            // Upload the csv string to the lakehouse under the filename Employees<timestamp>.csv
            string csvFileName = $"Employees{DateTime.Now.ToFileTimeUtc()}.csv";
            _logger.LogInformation($"Getting file client for file name: {csvFileName}");
            var fileClient = myLakehouse.GetFileClient(csvFileName);

            byte[] byteArray = Encoding.UTF8.GetBytes(csv);
            MemoryStream stream = new MemoryStream(byteArray);

            // Upload the file
            await fileClient.UploadAsync(stream, overwrite: true);
            _logger.LogInformation($"File {csvFileName} uploaded to {myLakehouse.FileSystemName} Lakehouse");

            myWarehouse.Close();

            // You might want to return some information about the upload or just complete the function
            return new List<object>() { 
                $"File {csvFileName} is written to {myLakehouse.FileSystemName} Lakehouse. You can delete it from the Lakehouse after trying this sample.",
                $"Contents of the file: {csv}"
             };
         }
        
    }
}
