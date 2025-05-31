using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

public static class CheckSqlChanges
{
    private static readonly string connectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING");
    private static readonly string blobConnectionString = Environment.GetEnvironmentVariable("BLOB_CONNECTION_STRING");
    private static readonly string containerName = Environment.GetEnvironmentVariable("BLOB_CONTAINER_NAME");

    private static long lastSyncVersion = 0;

    [FunctionName("CheckSqlChanges")]
    public static async Task Run([TimerTrigger("*/5 * * * * *")] TimerInfo myTimer, ILogger log)
    {
        log.LogInformation($"Azure Function executed at: {DateTime.UtcNow}");

        try
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                // Get latest Change Tracking version
                SqlCommand versionCommand = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", conn);
                long currentVersion = (long)await versionCommand.ExecuteScalarAsync();

                if (lastSyncVersion == 0) lastSyncVersion = currentVersion;
                if (lastSyncVersion >= currentVersion) return;

                // Query for INSERTED or UPDATED changes
                string query = @"
                    SELECT CT.SYS_CHANGE_VERSION, CT.SYS_CHANGE_OPERATION, T.*
                    FROM CHANGETABLE(CHANGES poc_database.dbo.poc_table, @lastSyncVersion) AS CT
                    LEFT JOIN poc_database.dbo.poc_table AS T ON CT.id = T.id
                    WHERE CT.SYS_CHANGE_OPERATION IN ('I', 'U')
                    ORDER BY CT.SYS_CHANGE_VERSION DESC";

                SqlCommand command = new SqlCommand(query, conn);
                command.Parameters.AddWithValue("@lastSyncVersion", lastSyncVersion);

                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    var changesList = new List<object>();

                    while (await reader.ReadAsync())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader[i];
                        }

                        changesList.Add(row);
                    }

                    if (changesList.Count > 0)
                    {
                        string jsonPayload = JsonSerializer.Serialize(new { changes = changesList });

                        // Upload to Azure Blob Storage
                        await UploadToBlobStorage(jsonPayload, log);

                        log.LogInformation("Changes uploaded successfully.");
                        lastSyncVersion = currentVersion;
                    }
                    else
                    {
                        log.LogInformation("No relevant changes detected.");
                    }
                }
            }
        }
        catch (Exception ex)
        {
            log.LogError($"Error: {ex.Message}");
        }
    }

    private static async Task UploadToBlobStorage(string jsonData, ILogger log)
    {
        try
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(blobConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.CreateIfNotExistsAsync();

            string fileName = $"sql_changes_{DateTime.UtcNow:yyyyMMdd_HHmmss}.json";
            BlobClient blobClient = containerClient.GetBlobClient(fileName);

            byte[] byteArray = Encoding.UTF8.GetBytes(jsonData);
            using (var stream = new System.IO.MemoryStream(byteArray))
            {
                await blobClient.UploadAsync(stream, overwrite: true);
            }

            log.LogInformation($"Uploaded JSON file: {fileName}");
        }
        catch (Exception ex)
        {
            log.LogError($"Blob upload error: {ex.Message}");
        }
    }
}
