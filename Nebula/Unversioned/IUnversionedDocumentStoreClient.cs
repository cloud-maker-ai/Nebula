using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nebula.Unversioned
{
    /// <summary>
    /// Defines methods for reading and writing un-versioned documents.
    /// </summary>
    public interface IUnversionedDocumentStoreClient
    {
        /// <summary>
        /// Creates a document.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="document">The document.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="operationOptions">The operation options.</param>
        /// <returns>A task that represents the asynchronous create operation.</returns>
        Task CreateDocumentAsync<TDocument>(TDocument document, DocumentTypeMapping<TDocument> mapping, OperationOptions operationOptions);

        /// <summary>
        /// Updates an existing document.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="document">The document.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="operationOptions">The operation options.</param>
        /// <returns>A task that represents the asynchronous update operation.</returns>
        Task UpdateDocumentAsync<TDocument>(TDocument document, DocumentTypeMapping<TDocument> mapping, OperationOptions operationOptions);

        /// <summary>
        /// Creates or updates a document.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="document">The document.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="operationOptions">The operation options.</param>
        /// <returns>A task that represents the asynchronous upsert operation.</returns>
        Task UpsertDocumentAsync<TDocument>(TDocument document, DocumentTypeMapping<TDocument> mapping, OperationOptions operationOptions);

        /// <summary>
        /// Deletes a document.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="id">The document id.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous delete operation.</returns>
        Task DeleteDocumentAsync<TDocument>(string id, DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Gets a document by id.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="id">The document id.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>If the id does not exist then the result will be null.</para>
        /// </remarks>
        Task<DocumentReadResult<TDocument>> GetDocumentAsync<TDocument>(string id, DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Gets documents by id.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="ids">The document ids.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>If a document is not found it is not included in the result.</para>
        /// </remarks>
        Task<DocumentBatchReadResult<TDocument>> GetDocumentsAsync<TDocument>(IEnumerable<string> ids, DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Gets documents matching a query.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        Task<DocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Gets documents matching a query.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The db parameters.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>This overload is preferred when building queries based on user sourced values. Db parameters
        /// should be used to protect against NoSQL injection attacks.</para>
        /// </remarks>
        Task<DocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            IEnumerable<DbParameter> parameters,
            DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Gets all documents.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        Task<IList<DocumentWithMetadata<TDocument>>> GetDocumentsAsync<TDocument>(DocumentTypeMapping<TDocument> mapping);
    }
}