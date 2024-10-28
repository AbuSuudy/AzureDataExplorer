using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System.Data;

namespace AzureDataExplorer
{
    internal class ADXAccess
    {
        private readonly static string kustoUri = Environment.GetEnvironmentVariable("KUSTO_URI");
        private readonly static string ingestUri = Environment.GetEnvironmentVariable("KUSTO_INGEST");         
        private readonly static string tenantId = Environment.GetEnvironmentVariable("TENANT_ID");
        private readonly static string databaseName = "adxdb";
        private readonly static string tableName = "StormEvents";
        private readonly static string tableMappingName = "StormEvents_CSV_Mapping";
        private readonly static string blobPath = "https://kustosamples.blob.core.windows.net/samplefiles/StormEvents.csv";
        private static KustoConnectionStringBuilder kustoConnectionStringBuilder = new KustoConnectionStringBuilder(kustoUri).WithAadAzCliAuthentication();

        public static async Task CreateTable()
        {
            using (var kustoClient = KustoClientFactory.CreateCslAdminProvider(kustoConnectionStringBuilder))
            {
                var command = CslCommandGenerator.GenerateTableCreateCommand(
                    tableName,
                    new[]
                    {
                        Tuple.Create("StartTime", "System.DateTime"),
                        Tuple.Create("EndTime", "System.DateTime"),
                        Tuple.Create("EpisodeId", "System.Int32"),
                        Tuple.Create("EventId", "System.Int32"),
                        Tuple.Create("State", "System.String"),
                        Tuple.Create("EventType", "System.String"),
                        Tuple.Create("InjuriesDirect", "System.Int32"),
                        Tuple.Create("InjuriesIndirect", "System.Int32"),
                        Tuple.Create("DeathsDirect", "System.Int32"),
                        Tuple.Create("DeathsIndirect", "System.Int32"),
                        Tuple.Create("DamageProperty", "System.Int32"),
                        Tuple.Create("DamageCrops", "System.Int32"),
                        Tuple.Create("Source", "System.String"),
                        Tuple.Create("BeginLocation", "System.String"),
                        Tuple.Create("EndLocation", "System.String"),
                        Tuple.Create("BeginLat", "System.Double"),
                        Tuple.Create("BeginLon", "System.Double"),
                        Tuple.Create("EndLat", "System.Double"),
                        Tuple.Create("EndLon", "System.Double"),
                        Tuple.Create("EpisodeNarrative", "System.String"),
                        Tuple.Create("EventNarrative", "System.String"),
                        Tuple.Create("StormSummary", "System.Object"),
                    }
                );
                await kustoClient.ExecuteControlCommandAsync(databaseName, command);
            }
        }

        public static async Task IngestionMapping()
        {
            using (var kustoClient = KustoClientFactory.CreateCslAdminProvider(kustoConnectionStringBuilder))
            {
                var command = CslCommandGenerator.GenerateTableMappingCreateCommand(
                    IngestionMappingKind.Csv,
                    tableName,
                    tableMappingName,
                    new ColumnMapping[]
                    {
                        new() { ColumnName = "StartTime", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "0" } } },
                        new() { ColumnName = "EndTime", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "1" } } },
                        new() { ColumnName = "EpisodeId", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "2" } } },
                        new() { ColumnName = "EventId", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "3" } } },
                        new() { ColumnName = "State", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "4" } } },
                        new() { ColumnName = "EventType", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "5" } } },
                        new() { ColumnName = "InjuriesDirect", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "6" } } },
                        new() { ColumnName = "InjuriesIndirect", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "7" } } },
                        new() { ColumnName = "DeathsDirect", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "8" } } },
                        new() { ColumnName = "DeathsIndirect", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "9" } } },
                        new() { ColumnName = "DamageProperty", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "10" } } },
                        new() { ColumnName = "DamageCrops", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "11" } } },
                        new() { ColumnName = "Source", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "12" } } },
                        new() { ColumnName = "BeginLocation", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "13" } } },
                        new() { ColumnName = "EndLocation", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "14" } } },
                        new() { ColumnName = "BeginLat", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "15" } } },
                        new() { ColumnName = "BeginLon", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "16" } } },
                        new() { ColumnName = "EndLat", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "17" } } },
                        new() { ColumnName = "EndLon", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "18" } } },
                        new() { ColumnName = "EpisodeNarrative", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "19" } } },
                        new() { ColumnName = "EventNarrative", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "20" } } },
                        new() { ColumnName = "StormSummary", Properties = new Dictionary<string, string> { { MappingConsts.Ordinal, "21" } } }
                    }
                );

                await kustoClient.ExecuteControlCommandAsync(databaseName, command);
            }
        }

        public static async Task Batching()
        {
            using (var kustoClient = KustoClientFactory.CreateCslAdminProvider(kustoConnectionStringBuilder))
            {
                var command = CslCommandGenerator.GenerateTableAlterIngestionBatchingPolicyCommand(
                    databaseName,
                    tableName,
                    new IngestionBatchingPolicy(
                        maximumBatchingTimeSpan: TimeSpan.FromSeconds(10),
                        maximumNumberOfItems: 100,
                        maximumRawDataSizeMB: 1024
                    )
                );
                kustoClient.ExecuteControlCommand(command);
            }

           
            var ingestConnectionStringBuilder = new KustoConnectionStringBuilder(ingestUri).WithAadUserPromptAuthentication(tenantId);

            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConnectionStringBuilder))
            {
                var properties = new KustoQueuedIngestionProperties(databaseName, tableName)
                {
                    Format = DataSourceFormat.csv,
                    IngestionMapping = new IngestionMapping
                    {
                        IngestionMappingReference = tableMappingName,
                        IngestionMappingKind = IngestionMappingKind.Csv
                    },
                    IgnoreFirstRecord = true
                };

                await ingestClient.IngestFromStorageAsync(blobPath, properties);
            }

        }

        public static bool CheckIfTableExist()
        {
            bool tableExist = false;

            using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kustoConnectionStringBuilder))
            {
                using (var response = kustoClient.ExecuteQuery(databaseName, ".show tables", null))
                {
                    int tableNameResult = response.GetOrdinal("TableName");

                    while (response.Read())
                    {
                        if (response.GetString(tableNameResult) == tableName)
                        {
                            tableExist = true;
                            break;
                        }
                    }
                }
            }

            return tableExist;
        }

        public static void StormEventsData()
        {

            using (var kustoClient = KustoClientFactory.CreateCslQueryProvider(kustoConnectionStringBuilder))
            {

                string query = @"StormEvents
                         | where EventType == 'Tornado'
                         | extend TotalDamage = DamageProperty + DamageCrops
                         | summarize DailyDamage=sum(TotalDamage) by State, bin(StartTime, 1d)
                         | where DailyDamage > 100000000
                         | order by DailyDamage desc";

                using (IDataReader response = kustoClient.ExecuteQuery(databaseName, query, null))
                {
                    //GetOrdinal returns index based on the name
                    int columnNoStartTime = response.GetOrdinal("StartTime");
                    int columnNoState = response.GetOrdinal("State");
                    int columnNoDailyDamage = response.GetOrdinal("DailyDamage");

                    Console.WriteLine("Daily tornado damages over 100,000,000$:");

                    while (response.Read())
                    {
                        Console.WriteLine("{0} - {1}, {2}",
                          response.GetDateTime(columnNoStartTime),
                          response.GetString(columnNoState),
                          response.GetInt64(columnNoDailyDamage));
                    }
                }
            }
        }

        public static void RowCount()
        {
            using (ICslQueryProvider cslQueryProvider = KustoClientFactory.CreateCslQueryProvider(kustoConnectionStringBuilder))
            {
                var results = cslQueryProvider.ExecuteQuery<long>(databaseName, $"{tableName} | count");
                Console.WriteLine($"Row Count: {results.SingleOrDefault()}");
            };
        }
    }
}
