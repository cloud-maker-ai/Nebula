using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

namespace Nebula.Config
{
    /// <summary>
    /// Provides operations for managing a service's configuration.
    /// </summary>
    /// <remarks>
    /// <para>This is a thread safe implementation.</para>
    /// </remarks>
    public class ServiceDbConfigManager : IServiceDbConfigRegistry
    {
        private readonly string _serviceName;
        private readonly IServiceConfigSignatureGenerator _signatureGenerator;

        private readonly Dictionary<string, IDocumentStoreConfigSource> _configSources = new Dictionary<string, IDocumentStoreConfigSource>();
        private bool _serviceUpdateRequired;

        private readonly object _sync = new object();

        /// <summary>
        /// Initialises a new instance of the <see cref="ServiceDbConfigManager"/> class.
        /// </summary>
        /// <param name="serviceName">The service name.</param>
        public ServiceDbConfigManager(string serviceName)
        {
            if (serviceName == null)
                throw new ArgumentNullException(nameof(serviceName));

            _serviceName = serviceName;
            _signatureGenerator = new ServiceConfigSignatureGenerator(serviceName);
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="ServiceDbConfigManager"/> class for testing.
        /// </summary>
        /// <param name="serviceName">The service name.</param>
        /// <param name="signatureGenerator">The config signature generator.</param>
        /// <remarks>
        /// <para>This constructor should be used for internal testing only.</para>
        /// </remarks>
        internal ServiceDbConfigManager(string serviceName, IServiceConfigSignatureGenerator signatureGenerator)
        {
            if (serviceName == null)
                throw new ArgumentNullException(nameof(serviceName));
            if (signatureGenerator == null)
                throw new ArgumentNullException(nameof(signatureGenerator));

            _serviceName = serviceName;
            _signatureGenerator = signatureGenerator;
        }

        internal string ServiceName
        {
            get { return _serviceName; }
        }

        /// <inheritdoc />
        public void RegisterStoreConfigSource(IDocumentStoreConfigSource source)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            RegisterSource(source);
        }

        /// <inheritdoc />
        public bool IsStoreConfigRegistered(IDocumentStoreConfigSource source)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            lock (_sync)
            {
                return _configSources.ContainsValue(source);
            }
        }

        internal void EnsureConfigCurrent(IDocumentClient client, DocumentDbConfig dbConfig)
        {
            if (client == null)
                throw new ArgumentNullException(nameof(client));
            if (dbConfig == null)
                throw new ArgumentNullException(nameof(dbConfig));

            EnsureCurrent(client, dbConfig);
        }

        internal string CreateDocumentContentKey(string storeName, string documentName)
        {
            if (storeName == null)
                throw new ArgumentNullException(nameof(storeName));
            if (documentName == null)
                throw new ArgumentNullException(nameof(documentName));

            return $"{GetDocumentContentPrefix()}_{storeName}_{documentName}";
        }

        private void RegisterSource(IDocumentStoreConfigSource source)
        {
            // This processing must be synchronised to ensure that service updates are kept consistent with the
            // registration of new sources. The locking flow is simplified by ensuring that a service update
            // cannot happen at the same time as a source being registered. That has a trade off of efficiency
            // because callers may be blocked at registration while waiting for a service update on another thread.

            var storeName = source.GetConfig().StoreName;

            lock (_sync)
            {
                if (!_configSources.ContainsKey(storeName))
                {
                    // There is no store registered with the given name. Add the new source and mark a service update as
                    // being required on the next service config check.
                    _configSources.Add(storeName, source);
                    _serviceUpdateRequired = true;
                }
            }
        }

        private void EnsureCurrent(IDocumentClient client, DocumentDbConfig dbConfig)
        {
            // This processing must be synchronised to ensure that service updates are kept consistent. See further
            // commentary in RegisterSource.

            List<DocumentStoreConfig> stores;

            lock (_sync)
            {
                // Return early if no service update is required.
                if (!_serviceUpdateRequired)
                {
                    return;
                }

                stores = _configSources.Values.Select(x => x.GetConfig()).ToList();
            }

            var configSignature = CreateSignature(stores);

            lock (_sync)
            {
                try
                {
                    EnsureServiceConfigAsync(client, dbConfig, configSignature, stores).Wait();
                }
                catch (AggregateException e)
                {
                    foreach (var exception in e.Flatten().InnerExceptions)
                    {
                        if (exception is DocumentClientException)
                        {
                            throw new NebulaConfigException("Failed to update service configuration due to document client error", e);
                        }
                    }

                    throw new NebulaConfigException("Failed to update service configuration", e.Flatten());
                }

                // Service update successful.
                _serviceUpdateRequired = false;
            }
        }

        private string CreateSignature(List<DocumentStoreConfig> stores)
        {
            try
            {
                return _signatureGenerator.CreateSignature(stores);
            }
            catch (Exception e)
            {
                throw new NebulaConfigException("Configuration signature generation failed", e);
            }
        }

        private async Task EnsureServiceConfigAsync(
            IDocumentClient client,
            DocumentDbConfig dbConfig,
            string configSignature,
            List<DocumentStoreConfig> configs)
        {
            var existingDocument = await GetConfigRecordAsync(client, dbConfig);

            if (existingDocument != null)
            {
                // Config already exists.
                ServiceConfigRecord existingConfigRecord = (dynamic)existingDocument;

                if (existingConfigRecord.Signature == configSignature)
                {
                    // No changes to the config. Nothing to do.
                    return;
                }

                // Config has been updated. Perform an update.
                await UpdateCollectionConfigAsync(client, dbConfig, configs);
            }

            // There has been a config change. Update the config record.

            var configRecordId = GetConfigRecordId();

            var configRecord = new ServiceConfigRecord
            {
                Id = configRecordId,
                Signature = configSignature,
                PartitionKey = configRecordId
            };

            // Update the store config record.
            await StoreConfigRecordAsync(client, dbConfig, configRecord, existingDocument);
        }

        private Uri GetDocumentCollectionUri(DocumentDbConfig dbConfig)
        {
            return UriFactory.CreateDocumentCollectionUri(dbConfig.DatabaseId, dbConfig.CollectionName);
        }

        private async Task UpdateCollectionConfigAsync(IDocumentClient client, DocumentDbConfig dbConfig, List<DocumentStoreConfig> storeConfigs)
        {
            DocumentCollection collection = await client.ReadDocumentCollectionAsync(GetDocumentCollectionUri(dbConfig));

            RemoveIndexes(collection);
            AddIndexes(collection, storeConfigs);

            var requestOptions = new RequestOptions();
            requestOptions.AccessCondition = new AccessCondition
            {
                Type = AccessConditionType.IfMatch,
                Condition = collection.ETag
            };

            await client.ReplaceDocumentCollectionAsync(collection, requestOptions);
        }

        private async Task StoreConfigRecordAsync(
            IDocumentClient client,
            DocumentDbConfig dbConfig,
            ServiceConfigRecord newRecord,
            Document existingDocument)
        {
            var collectionUri = GetDocumentCollectionUri(dbConfig);

            if (existingDocument == null)
            {
                // No record exists yet. Use the creation method to ensure failure if another process has modified the
                // record that a failure is produced.
                await client.CreateDocumentAsync(collectionUri, newRecord);
            }
            else
            {
                // Add IfMatch header to ensure the record has not been changed by another process.
                RequestOptions options = new RequestOptions
                {
                    AccessCondition = new AccessCondition
                    {
                        Condition = existingDocument.ETag,
                        Type = AccessConditionType.IfMatch
                    }
                };

                await client.UpsertDocumentAsync(collectionUri, newRecord, options);
            }
        }

        private async Task<Document> GetConfigRecordAsync(IDocumentClient client, DocumentDbConfig dbConfig)
        {
            var configRecordId = GetConfigRecordId();

            var documentUri = UriFactory.CreateDocumentUri(dbConfig.DatabaseId, dbConfig.CollectionName, configRecordId);

            // The collection is partitioned and the config record is not excluded from that requirement. The partition
            // key is simply set as the config record id.
            var requestOptions = new RequestOptions { PartitionKey = new PartitionKey(configRecordId) };

            try
            {
                return await client.ReadDocumentAsync(documentUri, requestOptions);
            }
            catch (DocumentClientException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                // Document not found.
                return null;
            }
        }

        private string GetConfigRecordId()
        {
            return "ConfigRecord_" + _serviceName;
        }

        private string GetDocumentContentPrefix()
        {
            return "content_" + _serviceName;
        }

        private void RemoveIndexes(DocumentCollection collection)
        {
            var contentPrefix = GetDocumentContentPrefix();
            var pathPrefix = $"/{contentPrefix}_";

            for (var i = 0; i < collection.IndexingPolicy.IncludedPaths.Count; i++)
            {
                var path = collection.IndexingPolicy.IncludedPaths[i];

                if (path.Path.StartsWith(pathPrefix))
                {
                    collection.IndexingPolicy.IncludedPaths.RemoveAt(i);
                    i -= 1;
                }
            }

            for (var i = 0; i < collection.IndexingPolicy.ExcludedPaths.Count; i++)
            {
                var path = collection.IndexingPolicy.ExcludedPaths[i];

                if (path.Path.StartsWith(pathPrefix))
                {
                    collection.IndexingPolicy.ExcludedPaths.RemoveAt(i);
                    i -= 1;
                }
            }
        }

        private void AddIndexes(DocumentCollection collection, List<DocumentStoreConfig> storeConfigs)
        {
            foreach (var storeConfig in storeConfigs)
            {
                foreach (var config in storeConfig.Documents)
                {
                    var documentKey = CreateDocumentContentKey(storeConfig.StoreName, config.DocumentName);

                    foreach (var path in config.InclusionIndexes)
                    {
                        // Update the path to include the full content path.
                        var newPath = new IncludedPath
                        {
                            Path = $"/{documentKey}{path.Path}",
                            Indexes = path.Indexes
                        };

                        collection.IndexingPolicy.IncludedPaths.Add(newPath);
                    }

                    foreach (var path in config.ExclusionIndexes)
                    {
                        // Update the path to include the full content path.
                        var newPath = new ExcludedPath
                        {
                            Path = $"/{documentKey}{path.Path}"
                        };

                        collection.IndexingPolicy.ExcludedPaths.Add(newPath);
                    }
                }
            }
        }

        internal class ServiceConfigRecord
        {
            [JsonProperty("id")]
            public string Id { get; set; }

            public string Signature { get; set; }

            [JsonProperty("_partitionKey")]
            public string PartitionKey { get; set; }
        }
    }
}