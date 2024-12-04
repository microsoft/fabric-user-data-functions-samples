using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace hellofabric
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
            _logger.LogInformation("C# Fabric data function HelloFabric is called.");
            return $"Welcome to Fabric Functions, {name}, at {DateTime.Now}!";
        }

        [Function(nameof(sum))]
        public int sum(int a, int b)
        {
            _logger.LogInformation("C# Fabric data function sum is called.");
            return a + b;
        }
    }
}
