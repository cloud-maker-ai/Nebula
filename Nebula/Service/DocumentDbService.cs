using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace Nebula.Service
{
    /// <inheritdoc cref="IDocumentDbService" />
    public class DocumentDbService : IDocumentDbService, IDisposable
    {
        private readonly DocumentDbConfig _dbConfig;
        private readonly DocumentClient _client;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentDbService"/> class.
        /// </summary>
        /// <param name="dbConfig">The document db config.</param>
        public DocumentDbService(DocumentDbConfig dbConfig)
        {
            if (dbConfig == null)
                throw new ArgumentNullException(nameof(dbConfig));

            _dbConfig = dbConfig;

            var connectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            _client = new DocumentClient(new Uri(dbConfig.ServiceEndpoint), dbConfig.AuthKey, connectionPolicy);
        }

        internal DocumentClient Client
        {
            get { return _client; }
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            await _client.OpenAsync();

            await CreateDatabaseIfNotExistsAsync();

            await CreateCollectionIfNotExistsAsync();

            await CreateStoredProceduresAsync();
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

            DocumentCollection documentCollection = new DocumentCollection
            {
                Id = _dbConfig.CollectionName,
                IndexingPolicy = new IndexingPolicy(),
                PartitionKey = partitionKeyDefinition,
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