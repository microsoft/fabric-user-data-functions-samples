/* Description
Use an English language dictionary to get random words  https://www.nuget.org/packages/DictionaryLib_Calvin_Hsia
Contains 2 English Language Dictionaries: small (53,000 words) and large (172,000 words)
Allows you to see if a string is a valid English word. Also can generate random words. Works on Windows, Android
Sample Android Phone word game https://github.com/calvinhsia/WordScape
*/

/* How to use:
Usings: No new 'usings' required
PackageReference: Need to modify the HelloFabric.csproj to add this line:
 		<PackageReference Include="DictionaryLib_Calvin_Hsia" Version="1.0.7" />
*/


        private readonly DictionaryLib.DictionaryLib dict = new(DictionaryLib.DictionaryType.Large); 
        [Function(nameof(RandWords))]
        public string RandWords(int? length)
        {
            if (!length.HasValue)
            {
                length = 5;
            }
            var randword = dict.RandomWord(); // gets a single random word
            var result = Enumerable.Range(0, length.Value).Select(i => dict.RandomWord()).ToList(); // gets a list of random words
            return string.Join(" ", result);
        }


