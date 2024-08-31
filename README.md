# fabric-user-data-functions
Samples for fabric user data functions 


User can select function samples.
Samples are categorized first by runtime (DOTNET, PYTHON) using folders
Then can be hierarchical with recursive JSON

index.JSON Format:


```typescript
interface ISampleFunction {
    name: string; // shown on 1st line of QuickPick Item
    description: string; // shown at end of 1st line of QuickPick Item
    detail?: string; // shown on 2nd line of QuickPick Item
    dateAdded?: string; // date added to the repo, so we can sort/filter
    tag: string; // additional tag to filter on
    data: ISampleFunction[] | string; // if string, it's the file name to download. Else it's an array of ISampleFunction
}

```

Additional filtering, sorting by user may be added in the future with tag, dateAdded
