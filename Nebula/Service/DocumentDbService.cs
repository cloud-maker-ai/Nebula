using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;

namespace Nebula.Service
{
    /// <inheritdoc cref="IDocumentDbService" />
    public class DocumentDbService : IDocumentDbService, IDisposable
    {
        private readonly ServiceDbConfigManager _configManager;
        private readonly DocumentDbConfig _dbConfig;
        private readonly DocumentClient _client;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentDbService"/> class.
        /// </summary>
        /// <param name="configManager">The service config manager.</param>
        /// <param name="dbConfig">The document db config.</param>
        public DocumentDbService(ServiceDbConfigManager configManager, DocumentDbConfig dbConfig)
        {
            if (configManager == null)
                throw new ArgumentNullException(nameof(configManager));
            if (dbConfig == null)
                throw new ArgumentNullException(nameof(dbConfig));

            _configManager = configManager;
            _dbConfig = dbConfig;

            var connectionPolicy = new ConnectionPolicy {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                RetryOptions = new RetryOptions {
                    MaxRetryAttemptsOnThrottledRequests = 20,
                    MaxRetryWaitTimeInSeconds = 60
                }
            };

            _client = new DocumentClient(new Uri(dbConfig.ServiceEndpoint), dbConfig.AuthKey, connectionPolicy);
        }

        internal DocumentClient Client
        {
            get { return _client; }
        }

        /// <inheritdoc />
        public async Task StartAsync(IEnumerable<IDocumentStoreConfigSource> storeConfigSources)
        {
            if (storeConfigSources == null)
                throw new ArgumentNullException(nameof(storeConfigSources));

            await _client.OpenAsync();

            await CreateDatabaseIfNotExistsAsync();

            await CreateCollectionIfNotExistsAsync();

            await CreateStoredProceduresAsync();

            await EnsureStoreConfigCurrentAsync(storeConfigSources);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _client?.Dispose();
        }

        private async Task CreateDatabaseIfNotExistsAsync()
        {
            var database = new Database { Id = _dbConfig.DatabaseId };

            await MakeClientCall(
                () => _client.CreateDatabaseIfNotExistsAsync(database),
                $"Failed to create database: {_dbConfig.DatabaseId}");
        }

        private async Task CreateCollectionIfNotExistsAsync()
        {
            // The collection must be partitioned. Ensure that is switched on to test constraints around that.
            var partitionKeyDefinition = new PartitionKeyDefinition();
            partitionKeyDefinition.Paths.Add("/_partitionKey");

            DocumentCollection documentCollection = new DocumentCollection {
                Id = _dbConfig.CollectionName,
                IndexingPolicy = new IndexingPolicy(),
                PartitionKey = partitionKeyDefinition,

                // TTL support is enabled on the collection. A value of '-1' means that TTL is enabled but documents
                // are not expired unless TTL is specified on a per document basis.
                DefaultTimeToLive = -1
            };

            var requestOptions = new RequestOptions
            {
                OfferThroughput = _dbConfig.DefaultRus
            };

            await MakeClientCall(
                () => _client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(_dbConfig.DatabaseId), documentCollection, requestOptions),
                "Failed to create collection");
        }

        private async Task CreateStoredProceduresAsync()
        {
            var definitions = GetStoredProcedureDefinitions(Assembly.GetExecutingAssembly());

            foreach (var definition in definitions)
            {
                var attribute = definition.Item1;
                var storedProcedure = definition.Item2;

                var spId = attribute.Name;

                var storedProcedureUri = UriFactory.CreateStoredProcedureUri(
                    _dbConfig.DatabaseId, _dbConfig.CollectionName, spId);

                var storedProcedureResponse = await MakeClientCall(
                    () => _client.ReadStoredProcedureAsync(storedProcedureUri),
                    $"Failed to read stored procedure: {storedProcedureUri}");

                if (storedProcedureResponse == null)
                {
                    var collectionUri = UriFactory.CreateDocumentCollectionUri(_dbConfig.DatabaseId, _dbConfig.CollectionName);

                    var storedProcedureRecord = new StoredProcedure
                    {
                        Id = spId,
                        Body = storedProcedure.Script
                    };

                    await MakeClientCall(
                        () => _client.CreateStoredProcedureAsync(collectionUri, storedProcedureRecord),
                        $"Failed to create stored procedure: {storedProcedureUri}");
                }
            }
        }

        private async Task EnsureStoreConfigCurrentAsync(IEnumerable<IDocumentStoreConfigSource> storeConfigSources)
        {
            foreach (var source in storeConfigSources)
            {
                _configManager.RegisterStoreConfigSource(source);
            }

            await _configManager.EnsureConfigCurrent(_client, _dbConfig);
        }

        private static IEnumerable<Tuple<StoredProcedureAttribute, StoredProcedureDefinition>> GetStoredProcedureDefinitions(Assembly assembly)
        {
            foreach (Type type in assembly.GetTypes())
            {
                var attributes = type.GetCustomAttributes(typeof(StoredProcedureAttribute), true);

                if (attributes.Length > 0)
                {
                    var attribute = (StoredProcedureAttribute)attributes[0];
                    var storedProcedureDefinition = (StoredProcedureDefinition)Activator.CreateInstance(type);

                    yield return Tuple.Create(attribute, storedProcedureDefinition);
                }
            }
        }

        private async Task<TReturn> MakeClientCall<TReturn>(Func<Task<TReturn>> call, string onErrorReason)
        {
            try
            {
                return await call();
            }
            catch (Exception e)
            {
                HandleClientError(e, onErrorReason);
                return default(TReturn);
            }
        }

        private void HandleClientError(Exception e, string onErrorReason)
        {
            if (e is DocumentClientException clientException)
            {
                if (clientException.Error.Code == "NotFound")
                {
                    return;
                }

                if (clientException.Error.Code == "Conflict")
                {
                    // A manager on another thread/service won the race.
                    return;
                }

                throw new NebulaServiceException(onErrorReason, clientException);
            }

            throw new NebulaServiceException("Unknown client failure - " + onErrorReason, e);
        }
    }
}