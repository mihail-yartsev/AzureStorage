# AzureStorage

## How to use

```cs
var settingsManager = configuration.LoadSettings<TSettings>("SettingsUrl");
var connectionStringManager = settingsManager.ConnectionString(x => ...);
var tableStorage = AzureTableStorage<TEntity>.Create(connectionStringManager, "TableName", log)
```
