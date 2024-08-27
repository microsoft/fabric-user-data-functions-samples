/* Description 
This sample validates a JSON string schema 
*/

[Function("ValidateJSONSchema")] 
        public bool ValidateJSONSchema(string message) // Pass the JSON object as a string for message variable
        {
            _logger.LogInformation("Validating JSON Schema for message: {message}", message);

            //define the JSON schema
            JSchema schema = JSchema.Parse(@"{
                'type': 'object',
                'properties': {
                    'name': {'type':'string'},
                    'roles': {'type': 'array'}
                }
                }");
                     
                bool valid= false;
                if (message != null)
                {                
                    JObject input = JObject.Parse(message);
                    valid = input.IsValid(schema);
                }
                return valid;  
        }
        