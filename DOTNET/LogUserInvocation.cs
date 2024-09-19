/*

*/
        [Function(nameof(LogUserInvocation))]
        public string LogUserInvocation([UserDataFunctionContext] UserDataFunctionContext context, 
            [FabricItemInput("InvocationLog")] SqlConnection invocationWarehouse, string invokedFrom)
        {
            _logger.LogInformation("C# Fabric data function is called.");
            WriteInvocationToLogDatabase(invokedFrom,context, invocationWarehouse);

            return $"User {context.ExecutingUser.PreferredUsername} ran function/pipeline at {DateTime.Now}!";
        }

        private void WriteInvocationToLogDatabase(string invokedFrom, UserDataFunctionContext context, SqlConnection logDatabase)
        {
            logDatabase.Open();
            string query = $"INSERT INTO InvocationLog.Invocations.Log VALUES ('{context.InvocationId}', '{context.ExecutingUser.PreferredUsername}', '{context.ExecutingUser.Oid}', '{invokedFrom}')";
            using SqlCommand command = new SqlCommand(query, logDatabase);
            var rows = command.ExecuteNonQuery();
            logDatabase.Close();
        }
