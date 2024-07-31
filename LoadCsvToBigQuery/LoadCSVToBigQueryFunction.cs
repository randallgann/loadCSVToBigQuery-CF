using CloudNative.CloudEvents;
using Google.Cloud.Functions.Framework;
using Google.Cloud.BigQuery.V2;
using Google.Cloud.Storage.V1;
using Google.Events.Protobuf.Cloud.Storage.V1;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Logging;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using CsvHelper.Configuration.Attributes;
using System.Reflection;
using System.Collections.Generic;

namespace LoadCsvToBigQuery;

public class LoadCSVToBigQueryFunction : ICloudEventFunction<StorageObjectData>
{
    private readonly BigQueryClient _bigQueryClient;
    private readonly StorageClient _storageClient;
    private readonly ILogger<LoadCSVToBigQueryFunction> _logger;

    public LoadCSVToBigQueryFunction(ILogger<LoadCSVToBigQueryFunction> logger)
    {
        _logger = logger;
    }
    public async Task HandleAsync(CloudEvent cloudEvent, StorageObjectData data, CancellationToken cancellationToken)
    {
        string bucketName = data.Bucket;
        string objectName = data.Name;
        string datasetId = Environment.GetEnvironmentVariable("DATASET_ID");
        string tableId = Environment.GetEnvironmentVariable("TABLE_ID");
        string project_id = Environment.GetEnvironmentVariable("GCP_PROJECT");
        BigQueryClient _bigQueryClient = BigQueryClient.Create(project_id);
        StorageClient _storageClient = StorageClient.Create();

        _logger.LogInformation($"Processing file: {objectName} from bucket: {bucketName}");

        int LoadedToDBCount = 0;
        int UpdatedToDBCount = 0;
        int DiscardedCount = 0;

        var stream = new MemoryStream();
        await _storageClient.DownloadObjectAsync(bucketName, objectName, stream);
        stream.Seek(0, SeekOrigin.Begin);

        using (var reader = new StreamReader(stream))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        {
            try
            {
                var records = csv.GetRecords<CsvRecord>().ToList();
                int totalRecords = records.Count;
                _logger.LogInformation($"Total records to be processed: {totalRecords}");

                int processedCount = 0;


                foreach (var record in records)
                {
                    var existingRecord = await GetExistingRecordAsync(_bigQueryClient, datasetId, tableId, record.MLS);

                    if (existingRecord == null)
                    {
                        await InsertRecord(_bigQueryClient, datasetId, tableId, record);
                        LoadedToDBCount++;
                    }
                    else if (HasRecordChanged(existingRecord, record))
                    {
                        await InsertRecord(_bigQueryClient, datasetId, tableId, record);
                        UpdatedToDBCount++;
                    }
                    else
                    {
                        _logger.LogInformation($"Record with MLS: {record.MLS} already exists in BigQuery and has not changed. Skipping.");
                        DiscardedCount++;
                    }

                    processedCount++;

                    if (processedCount % 1000 == 0)
                    {
                        _logger.LogInformation($"Processed {processedCount} records out of {totalRecords}. Remaining: {totalRecords - processedCount}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to read CSV file: {objectName}. Exception: {ex.Message}");
            }

            _logger.LogInformation($"CSV data from {objectName} inserted into BigQuery table {tableId}.  Loaded: {LoadedToDBCount}, Updated: {UpdatedToDBCount}, Discarded: {DiscardedCount}");

            // Delete the file after processing
            try
            {
                await DeleteFileAsync(_storageClient, bucketName, objectName);
                _logger.LogInformation($"Successfully deleted file: {objectName} from bucket: {bucketName}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to delete file: {objectName} from bucket: {bucketName}. Exception: {ex.Message}");
            }
        }
    }

    private async Task DeleteFileAsync(StorageClient storageClient, string bucketName, string objectName)
    {
        await storageClient.DeleteObjectAsync(bucketName, objectName);
        _logger.LogInformation($"File {objectName} deleted from bucket {bucketName}");
    }

    private async Task<BigQueryRow> GetExistingRecordAsync(BigQueryClient client, string datasetId, string tableId, string mls)
    {
        var query = $"SELECT * FROM `{datasetId}.{tableId}` WHERE mls = @mls";
        var parameters = new[] { new BigQueryParameter("mls", BigQueryDbType.String, mls) };
        var result = await client.ExecuteQueryAsync(query, parameters);

        return result.SingleOrDefault();
    }

    private bool HasRecordChanged(BigQueryRow existingRecord, CsvRecord newRecord)
    {
        return !existingRecord["class"].ToString().Equals(newRecord.Class?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["property_type"].ToString().Equals(newRecord.PropertyType?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["status"].ToString().Equals(newRecord.Status?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["price"].ToString().Equals(new string(newRecord.Price?.Where(char.IsDigit).ToArray()), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["county"].ToString().Equals(newRecord.County?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["address"].ToString().Equals(newRecord.Address?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["city"].ToString().Equals(newRecord.City?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["zip"].ToString().Equals(newRecord.Zip?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["beds"].ToString().Equals(newRecord.Beds?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["baths"].ToString().Equals(newRecord.Baths?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["half_baths"].ToString().Equals(newRecord.HalfBaths?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["garage"].ToString().Equals(newRecord.Garage?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["sq_feet"].ToString().Equals(newRecord.SqFeet?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["list_agent"].ToString().Equals(newRecord.ListAgent?.Trim(), StringComparison.OrdinalIgnoreCase) ||
               !existingRecord["list_office"].ToString().Equals(newRecord.ListOffice?.Trim(), StringComparison.OrdinalIgnoreCase);
    }

    private async Task InsertRecord(BigQueryClient client, string datasetId, string tableId, CsvRecord record)
    {
        var price = int.TryParse(new string(record.Price?.Where(char.IsDigit).ToArray()), out var parsedPrice) ? parsedPrice : 0;
        var sqFeet = int.TryParse(record.SqFeet?.Trim(), out var parsedSqFeet) ? parsedSqFeet : 0;
        var priceSqFeet = sqFeet > 0 ? price / sqFeet : 0;

        var recordDictionary = new Dictionary<string, object>
            {
                {"mls", record.MLS?.Trim()},
                {"class", record.Class?.Trim()},
                {"property_type", record.PropertyType?.Trim()},
                {"status", record.Status?.Trim()},
                {"price", new string(record.Price?.Where(char.IsDigit).ToArray())},
                {"county", record.County?.Trim()},
                {"address", record.Address?.Trim()},
                {"city", record.City?.Trim()},
                {"zip", record.Zip?.Trim()},
                {"beds", record.Beds?.Trim()},
                {"baths", record.Baths?.Trim()},
                {"half_baths", record.HalfBaths?.Trim()},
                {"garage", record.Garage?.Trim()},
                {"sq_feet", record.SqFeet?.Trim()},
                {"price_sq_feet", priceSqFeet},
                {"last_updt_ts", DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss")},
                {"list_agent", record.ListAgent?.Trim()},
                {"list_office", record.ListOffice?.Trim()}
            };

        var rowInsert = new BigQueryInsertRow
            {
                recordDictionary
            };
        _logger.LogInformation("Attempting to insert row into BigQuery");

        try
        {
            await client.InsertRowsAsync(datasetId, tableId, new[] { rowInsert });
            var recordValues = string.Join(", ", recordDictionary.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
            _logger.LogInformation($"Successfully inserted row with values: {recordValues}");
        }
        catch (Exception ex)
        {
            var recordValues = string.Join(", ", recordDictionary.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
            _logger.LogError($"Failed to insert row with values: {recordValues}. Exception: {ex.Message}");
        }
    }

    public class CsvRecord
    {
        [Name("Picture Count")]
        public int PictureCount { get; set; }
        [Name("MLS #")]
        public string MLS { get; set; }
        [Name("Class")]
        public string Class { get; set; }
        [Name("Property Type")]
        public string PropertyType { get; set; }
        [Name("Status")]
        public string Status { get; set; }
        [Name("Price")]
        public string Price { get; set; }
        [Name("County")]
        public string County { get; set; }
        [Name("Address")]
        public string Address { get; set; }
        [Name("City")]
        public string City { get; set; }
        [Name("Zip")]
        public string Zip { get; set; }
        [Name("#Br")]
        public string Beds { get; set; }
        [Name("#FBath")]
        public string Baths { get; set; }
        [Name("#HalfBa")]
        public string HalfBaths { get; set; }
        [Name("Gar")]
        public string Garage { get; set; }
        [Name("Sq Feet")]
        public string SqFeet { get; set; }
        [Name("List Agent - Agt Name")]
        public string ListAgent { get; set; }
        [Name("List Off 1 - Ofc Name")]
        public string ListOffice { get; set; }
    }
}
