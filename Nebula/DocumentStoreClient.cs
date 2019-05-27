using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;
using Nebula.Utils;
using Newtonsoft.Json;

namespace Nebula
{
    internal abstract class DocumentStoreClient<TDbDocument> where TDbDocument : DocumentStoreClient<TDbDocument>.DbDocument
    {
        private readonly DocumentDbAccess _dbAccess;
        private readonly DocumentStoreConfig _config;
        private readonly Uri _collectionUri;

        private readonly Dictionary<Type, string> _storedProcedures = new Dictionary<Type, string>();

        protected DocumentStoreClient(DocumentDbAccess dbAccess, DocumentStoreConfig config, IEnumerable<Type> storedProcedures)
        {
            if (dbAccess == null)
                throw new ArgumentNullException(nameof(dbAccess));
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            _dbAccess = dbAccess;
            _config = config;

            _collectionUri = UriFactory.CreateDocumentCollectionUri(dbAccess.DbConfig.DatabaseId, dbAccess.DbConfig.CollectionName);

            if (storedProcedures != null)
            {
                _storedProcedures = storedProcedures.ToDictionary(x => x, GetStoredProcedureName);
            }
        }

        protected DocumentDbAccess DbAccess
        {
            get { return _dbAccess; }
        }

        protected Uri CollectionUri
        {
            get { return _collectionUri; }
        }

        protected string CreateDbRecordId<TDocument>(DocumentTypeMapping<TDocument> mapping, params string[] values)
        {
            if (values.Length == 0)
                throw new ArgumentException("At least one value required", nameof(values));

            // The db record id must be globally unique. That is ensured by generating a GUID here that is based off
            // a string that includes the service name, store name and document name as well as the document id and
            // version. Having the id generated deterministically like this allows us to ensure that multiple clients
            // for the same store, fails when racing on updating a particular document (with the same id).

            var prefix = CreateContentKey(mapping);

            return GuidUtility.Create(GuidUtility.UrlNamespace, prefix + "_" + string.Join("_", values)).ToString();
        }

        private string CreateAttachmentId(string documentRecordId, string attachmentKey)
        {
            return GuidUtility.Create(GuidUtility.UrlNamespace, $"{documentRecordId}_{attachmentKey}").ToString();
        }

        private IDocumentClient GetClient()
        {
            return _dbAccess.GetClient();
        }

        protected async Task CreateDocumentAsync(DbDocument dbRecord)
        {
            var client = GetClient();

            await MakeClientCall(async () => await client.CreateDocumentAsync(_collectionUri, dbRecord), "Failed to write document");
        }

        protected async Task CreateDocumentAttachmentAsync<TDocument, TAttachment>(
            string documentRecordId,
            string partitionKey,
            AttachmentTypeMapping<TDocument, TAttachment> attachmentMapping,
            TAttachment attachment)
        {
            var client = GetClient();

            var documentUri = UriFactory.CreateDocumentUri(
                _dbAccess.DbConfig.DatabaseId, _dbAccess.DbConfig.CollectionName, documentRecordId);

            await MakeClientCall(async () =>
            {
                await client.CreateAttachmentAsync(
                    documentUri,
                    attachmentMapping.Writer(attachment),
                    new MediaOptions
                    {
                        // Managed attachments are always stored as octet streams by CosmosDb. The value for content
                        // type must be set to `application/octet-stream` when storing managed attachments otherwise
                        // the CosmosDb API produces an error stating that the payload is invalid. This is not the
                        // case for unmanaged attachments which require the content type to be set for the externally
                        // stored data.
                        ContentType = "application/octet-stream",

                        // The (poorly) named `Slug` property is what CosmosDb uses as the unique identifier for the
                        // attachment. This is the ID that is used for direct attachment retrieval and must be unique
                        // within the context of the parent document.
                        Slug = CreateAttachmentId(documentRecordId, attachmentMapping.AttachmentName)
                    }, new RequestOptions
                    {
                        PartitionKey = new PartitionKey(partitionKey)
                    });
            }, "Failed to create document attachment");
        }

        protected async Task<TAttachment> GetDocumentAttachmentAsync<TDocument, TAttachment>(
            string documentRecordId,
            string partitionKey,
            AttachmentTypeMapping<TDocument, TAttachment> attachmentMapping)
        {
            var client = GetClient();

            var attachmentUri = UriFactory.CreateAttachmentUri(
                _dbAccess.DbConfig.DatabaseId,
                _dbAccess.DbConfig.CollectionName,
                documentRecordId,
                CreateAttachmentId(documentRecordId, attachmentMapping.AttachmentName));

            var requestOptions = new RequestOptions { PartitionKey = new PartitionKey(partitionKey) };

            var attachmentResponse = await MakeClientCall(
                async () => await client.ReadAttachmentAsync(attachmentUri, requestOptions),
                "Failed to read document attachment");

            if (attachmentResponse == null)
            {
                // Attachment not found.
                return default(TAttachment);
            }

            var mediaResponse = await MakeClientCall(
                async () => await client.ReadMediaAsync(attachmentResponse.Resource.MediaLink),
                "Failed to read document attachment media");

            if (mediaResponse == null)
            {
                // Media link not found. This would indicate a bug in attachment storage process.
                return default(TAttachment);
            }

            return DeserialiseAttachment(attachmentMapping, mediaResponse);
        }

        protected async Task<IList<TDbDocument>> ExecuteQueryAsync(IQueryable<TDbDocument> query)
        {
            return await MakeClientCall(async () => await Task.Factory.StartNew(query.ToList), "Failed to execute query");
        }

        protected async Task<IList<TDbDocument>> ExecuteQueriesAsync(IList<IQueryable<TDbDocument>> queries)
        {
            return await MakeClientCall(async () => await Task.Factory.StartNew(() => queries.SelectMany(x => x.ToList()).ToList()), "Failed to execute query");
        }

        protected void SetDocumentContent<TDocument>(DbDocument dbDocument, TDocument content, DocumentTypeMapping<TDocument> mapping)
        {
            try
            {
                dbDocument.SetPropertyValue(CreateContentKey(mapping), content);
            }
            catch (JsonException e)
            {
                throw new NebulaStoreException("Failed to serialise document content", e);
            }
        }

        protected void SetDocumentContentFromExisting<TDocument>(
            DbDocument dbDocument,
            DbDocument existingDocument,
            DocumentTypeMapping<TDocument> mapping)
        {
            var contentKey = CreateContentKey(mapping);
            var contentValue = existingDocument.GetPropertyValue<object>(contentKey);

            try
            {
                dbDocument.SetPropertyValue(contentKey, contentValue);
            }
            catch (JsonException e)
            {
                throw new NebulaStoreException("Failed to serialise document content", e);
            }
        }

        protected bool TryGetDocumentContent<TDocument>(
            DbDocument document,
            DocumentTypeMapping<TDocument> mapping,
            out TDocument result,
            out DocumentReadFailureDetails failure)
        {
            try
            {
                result = document.GetPropertyValue<TDocument>(CreateContentKey(mapping));
            }
            catch (JsonSerializationException e)
            {
                result = default(TDocument);
                failure = new DocumentReadFailureDetails("Failed to deserialise document", e.Message);
                return false;
            }

            if (result == null)
            {
                failure = new DocumentReadFailureDetails("Failed to deserialise document", "Missing document content body");
                return false;
            }

            failure = null;
            return true;
        }

        protected string CreateContentKey<TDocument>(DocumentTypeMapping<TDocument> mapping)
        {
            return _dbAccess.ConfigManager.CreateDocumentContentKey(_config.StoreName, mapping.DocumentName);
        }

        private TAttachment DeserialiseAttachment<TDocument, TAttachment>(
            AttachmentTypeMapping<TDocument, TAttachment> attachmentMapping,
            MediaResponse mediaResponse)
        {
            try
            {
                return attachmentMapping.Reader(mediaResponse.Media);
            }
            catch (Exception e)
            {
                throw new NebulaStoreException("Failed to deserialise document attachment", e);
            }
        }

        protected async Task<TReturn> MakeClientCall<TReturn>(Func<Task<TReturn>> call, string onErrorReason)
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

        protected TReturn MakeClientCall<TReturn>(Func<TReturn> call, string onErrorReason)
        {
            try
            {
                return call();
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
                    // Not found results do not produce exceptions. The not found indication is represented in the
                    // result of the operation.
                    return;
                }

                if (clientException.Error.Code == "Conflict")
                {
                    throw new NebulaStoreConcurrencyException("Document modified by another process");
                }

                if (clientException.Error.Message != null)
                {
                    if (clientException.Error.Message.Contains("Resource with specified id or name already exists"))
                    {
                        throw new NebulaStoreConcurrencyException("Document modified by another process");
                    }

                    // Stored procedure errors are embedded in the error message.

                    if (clientException.Error.Message.Contains("[SP_ERROR=CONFLICT]"))
                    {
                        throw new NebulaStoreConcurrencyException("Document modified by another process");
                    }
                }

                throw new NebulaStoreException(onErrorReason, clientException);
            }

            throw new NebulaStoreException("Unknown client failure - " + onErrorReason, e);
        }

        protected async Task ExecuteStoredProcedureAsync<TProcedure>(string partitionKey, params dynamic[] procedureParams)
        {
            var client = GetClient();

            if (!_storedProcedures.TryGetValue(typeof(TProcedure), out var procedureName))
            {
                throw new NebulaStoreException($"Stored procedure '{typeof(TProcedure).FullName}' not found");
            }

            var storedProcedureUri = UriFactory.CreateStoredProcedureUri(
                DbAccess.DbConfig.DatabaseId, DbAccess.DbConfig.CollectionName, procedureName);

            var requestOptions = new RequestOptions
            {
                PartitionKey = new PartitionKey(partitionKey)
            };

            await MakeClientCall(async () => await client.ExecuteStoredProcedureAsync<dynamic>(
                storedProcedureUri, requestOptions, procedureParams), "Failed to write document");
        }

        private static string GetStoredProcedureName(Type type)
        {
            var attribute = type
                .GetCustomAttributes(typeof(StoredProcedureAttribute), true)
                .SingleOrDefault() as StoredProcedureAttribute;

            if (attribute == null)
            {
                throw new ArgumentException($"Stored procedure '{type.FullName}' is missing an attribute");
            }

            return attribute.Name;
        }

        internal class DbDocument : Document
        {
            // The '_' prefix is used here instead of the '@' convention followed elsewhere. That is because partition
            // key properties cannot start with the '@' character. This makes sense given it's cosmos db metadata and
            // underscores are used for that.
            [JsonProperty("_partitionKey")]
            public string PartitionKey { get; set; }

            [JsonProperty("@documentId")]
            public string DocumentId { get; set; }

            [JsonProperty("@service")]
            public string Service { get; set; }
        }
    }
}