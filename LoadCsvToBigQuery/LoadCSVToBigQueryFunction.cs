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

        var stream = new MemoryStream();
        await _storageClient.DownloadObjectAsync(bucketName, objectName, stream);
        stream.Seek(0, SeekOrigin.Begin);

        using (var reader = new StreamReader(stream))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        {
            try
            {
                var records = csv.GetRecords<CsvRecord>();

                foreach (var record in records)
                {

                    var recordDictionary = new Dictionary<string, object>
                    {
                        {"mls", record.MLS?.Trim()},
                        {"class", record.Class?.Trim()},
                        {"property_type", record.PropertyType?.Trim()},
                        {"status", record.Status?.Trim()},
                        {"price", record.Price?.Replace("$", "").Trim()},
                        {"county", record.County?.Trim()},
                        {"address", record.Address?.Trim()},
                        {"city", record.City?.Trim()},
                        {"zip", record.Zip?.Trim()},
                        {"beds", record.Beds?.Trim()},
                        {"baths", record.Baths?.Trim()},
                        {"half_baths", record.HalfBaths?.Trim()},
                        {"garage", record.Garage?.Trim()},
                        {"sq_feet", record.SqFeet?.Trim()},
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
                        await _bigQueryClient.InsertRowsAsync(datasetId, tableId, new[] { rowInsert });
                        var recordValues = string.Join(", ", recordDictionary.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                        _logger.LogInformation($"Successfully inserted row with values: {recordValues}");
                    }
                    catch (Exception ex)
                    {
                        var recordValues = string.Join(", ", recordDictionary.Select(kvp => $"{kvp.Key}: {kvp.Value}"));
                        _logger.LogError($"Failed to insert row with values: {recordValues}. Exception: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to read CSV file: {objectName}. Exception: {ex.Message}");
            }

            _logger.LogInformation($"CSV data from {objectName} inserted into BigQuery table {tableId}");
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
