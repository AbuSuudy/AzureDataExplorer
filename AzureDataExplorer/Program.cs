namespace AzureDataExplorer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {

            if (!ADXAccess.CheckIfTableExist())
            {
                await ADXAccess.CreateTable();
                await ADXAccess.IngestionMapping();
                await ADXAccess.Batching();
            }
            else
            {
                ADXAccess.RowCount();
                ADXAccess.StormEventsData();
            }

        }

    }
}
