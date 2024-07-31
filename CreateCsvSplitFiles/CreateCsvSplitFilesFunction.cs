using CloudNative.CloudEvents;
using Google.Cloud.Functions.Framework;
using Google.Cloud.Storage.V1;
using Google.Events.Protobuf.Cloud.Storage.V1;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CreateCsvSplitFiles;

public class CreateCsvSplitFilesFunction : ICloudEventFunction<StorageObjectData>
{
    private readonly StorageClient _storageClient;
    private readonly ILogger<CreateCsvSplitFilesFunction> _logger;

    public CreateCsvSplitFilesFunction(ILogger<CreateCsvSplitFilesFunction> logger)
    {
        _logger = logger;
    }
    public async Task HandleAsync(CloudEvent cloudEvent, StorageObjectData data, CancellationToken cancellationToken)
    {
        _logger.LogInformation("CreateCsvSplitFilesFunction triggered by Cloud Storage event.");
        string bucketName = data.Bucket;
        string objectName = data.Name;

        string splitFilesBucketName = Environment.GetEnvironmentVariable("SPLIT_FILES_BUCKET_NAME");

        StorageClient _storageClient = StorageClient.Create();

        try
        {
            using (var memoryStream = new MemoryStream())
            {
                await _storageClient.DownloadObjectAsync(bucketName, objectName, memoryStream);
                memoryStream.Seek(0, SeekOrigin.Begin);
                _logger.LogInformation($"Successfully downloaded file: {objectName} from bucket: {bucketName}");

                using (var reader = new StreamReader(memoryStream, Encoding.UTF8))
                {
                    try
                    {
                        int fileCounter = 1;
                        int lineCounter = 0;
                        List<string> currentBatch = new List<string>();
                        string? line;

                        // Read the header
                        string? header = reader.ReadLine();
                        if (header == null)
                        {
                            throw new Exception("The CSV file is empty");
                        }

                        while ((line = reader.ReadLine()) != null)
                        {
                            currentBatch.Add(line);
                            lineCounter++;

                            if (lineCounter == 100)
                            {
                                WriteBatchToFile(currentBatch, header, _storageClient, fileCounter, splitFilesBucketName);
                                currentBatch.Clear();
                                lineCounter = 0;
                                fileCounter++;
                            }
                        }

                        // Write any remaining lines
                        if (currentBatch.Count > 0)
                        {
                            WriteBatchToFile(currentBatch, header, _storageClient, fileCounter, splitFilesBucketName);
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.LogError($"Error splitting files: {objectName} from bucket: {bucketName}");
                        _logger.LogError(e.Message);
                    }
                }
            }
        }
        catch (Exception e)
        {
            _logger.LogError($"Error downloading file: {objectName} from bucket: {bucketName}");
            _logger.LogError(e.Message);
        }
        _logger.LogInformation("CreateCsvSplitFilesFunction completed.");
    }

    public void WriteBatchToFile(List<string> batch, string header, StorageClient storageClient, int fileNumber, string splitFilesBucketName)
    {
        string fileName = $"zip-code-split-file-{fileNumber:D3}.csv";

        using (var memoryStream = new MemoryStream())
        using (var writer = new StreamWriter(memoryStream, Encoding.UTF8))
        {
            try
            {
                writer.WriteLine(header);
                foreach (var line in batch)
                {
                    writer.WriteLine(line);
                }
                writer.Flush();
                memoryStream.Position = 0;

                storageClient.UploadObject(splitFilesBucketName, fileName, "text/csv", memoryStream);
                _logger.LogInformation($"Uploaded {fileName} to bucket {splitFilesBucketName}.");

            }
            catch (Exception e)
            {
                _logger.LogError($"Error writing batch to file: {fileName}");
                _logger.LogError(e.Message);
            }
        }
    }
}
