using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace HelloFabric
{
    public class FabricFunctions
    {
        private readonly ILogger _logger;

        public FabricFunctions(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<FabricFunctions>();
        }

        [Function(nameof(HelloFabric))]
        public string HelloFabric(string name)
        {
            _logger.LogInformation("C# Fabric data function is called.");

            return $"Welcome to Fabric Functions, {name}, at {DateTime.Now}!";
        }
    }
}
