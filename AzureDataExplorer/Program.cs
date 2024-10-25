namespace AzureDataExplorer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {

            if (!ADXDAL.CheckIfTableExist())
            {
                await ADXDAL.CreateTable();
                await ADXDAL.IngestionMapping();
                await ADXDAL.Batching();
            }
            else
            {
                ADXDAL.RowCount();
                ADXDAL.StormEventsData();
            }

        }

    }
}
