using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using Microsoft.Azure.Functions.Worker.Extensions.Fabric.Attributes;
using Microsoft.Azure.Functions.Worker.Extensions.Fabric;

namespace HelloFabric
{
    public class FabricFunctions
    {
        private readonly ILogger _logger;

        public FabricFunctions(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<FabricFunctions>();
        }
        
         [Function("GetDataFromDataWarehouse")]
         public async Task<IEnumerable<object>> Run(
             [FabricItemInput("<DataWarehouse Name>")] SqlConnection myDatawarehouse)
         {
             
             // Replace with the query you want to run
             string query = $"SELECT * FROM (VALUES ('John Smith',  31) , ('Kayla Jones', 33)) AS Employee(EmpName, DepID);";
 
            // You can use the SqlConnection to run sql against the item specified
             using SqlCommand command = new SqlCommand(query, myDatawarehouse);
 
             myDatawarehouse.Open();
 
             using var reader = await command.ExecuteReaderAsync();
 
             //See below link for ADO.net SqlClient examples
             //https:learn.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-code-examples
 
             List<Object[]> results = new List<Object[]>();
             while (reader.Read())
             {
                Object[] row = new Object[reader.FieldCount];
 
                reader.GetValues(row);
                results.Add(row);
 
             }
             
             return results;
         }

    }
}
