using System.Collections.Generic;
using System.Threading.Tasks;

namespace Nebula.Versioned
{
    /// <summary>
    /// Defines methods for reading and writing versioned documents.
    /// </summary>
    public interface IVersionedDocumentStoreClient
    {
        /// <summary>
        /// Gets a document by id.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="id">The document id.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="options">The read options.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>If the id does not exist then the result will be null.</para>
        /// <para>The latest version of the document is included in the result. If the document is deleted then
        /// the latest version will be the deleted document.</para>
        /// </remarks>
        Task<VersionedDocumentReadResult<TDocument>> GetDocumentAsync<TDocument>(string id, DocumentTypeMapping<TDocument> mapping, VersionedDocumentReadOptions options);

        /// <summary>
        /// Gets a specific document version by id.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="id">The document id.</param>
        /// <param name="version">The document version.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>If the id or version does not exist then the result will be null.</para>
        /// </remarks>
        Task<VersionedDocumentReadResult<TDocument>> GetDocumentAsync<TDocument>(string id, int version, DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Gets document metadata by id.
        /// </summary>
        /// <param name="id">The document id.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>If the id does not exist then the result will be null.</para>
        /// <para>The result includes metadata for all versions of the document.</para>
        /// </remarks>
        Task<VersionedDocumentMetadataReadResult> GetDocumentMetadataAsync<TDocument>(string id, DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Gets documents by id.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="ids">The document ids.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="options">The read options.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>The latest version of the documents are included in the result. If a document is deleted then
        /// the latest version will be the deleted document.</para>
        /// </remarks>
        Task<VersionedDocumentBatchReadResult<TDocument>> GetDocumentsAsync<TDocument>(IEnumerable<string> ids, DocumentTypeMapping<TDocument> mapping, VersionedDocumentReadOptions options);

        /// <summary>
        /// Gets documents matching a query.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="options">The read options.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>The latest version of the documents are included in the result. If a document is deleted then
        /// the latest version will be the deleted document.</para>
        /// </remarks>
        Task<VersionedDocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            DocumentTypeMapping<TDocument> mapping,
            VersionedDocumentReadOptions options);

        /// <summary>
        /// Gets documents matching a query.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The db parameters.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>The latest version of the documents are included in the result. If a document is deleted then
        /// the latest version will be the deleted document.</para>
        /// <para>This overload is preferred when building queries based on user sourced values. Db parameters
        /// should be used to protect against NoSQL injection attacks.</para>
        /// </remarks>
        Task<VersionedDocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            IEnumerable<DbParameter> parameters,
            DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Gets documents matching a query.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="query">The query.</param>
        /// <param name="parameters">The db parameters.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="options">The read options.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>The latest version of the documents are included in the result. If a document is deleted then
        /// the latest version will be the deleted document.</para>
        /// <para>This overload is preferred when building queries based on user sourced values. Db parameters
        /// should be used to protect against NoSQL injection attacks.</para>
        /// </remarks>
        Task<VersionedDocumentQueryResult<TDocument>> GetDocumentsAsync<TDocument>(
            string query,
            IEnumerable<DbParameter> parameters,
            DocumentTypeMapping<TDocument> mapping,
            VersionedDocumentReadOptions options);

        /// <summary>
        /// Gets all documents.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="options">The read options.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        /// <remarks>
        /// <para>The latest version of the documents are included in the result. If a document is deleted then
        /// the latest version will be the deleted document.</para>
        /// </remarks>
        Task<IList<VersionedDocumentWithMetadata<TDocument>>> GetDocumentsAsync<TDocument>(DocumentTypeMapping<TDocument> mapping, VersionedDocumentReadOptions options);

        /// <summary>
        /// Upserts a document.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="document">The document.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="operationOptions">The operation options.</param>
        /// <returns>A task that represents the asynchronous upsert operation.</returns>
        Task<VersionedDocumentUpsertResult<TDocument>> UpsertDocumentAsync<TDocument>(TDocument document, DocumentTypeMapping<TDocument> mapping, OperationOptions operationOptions);

        /// <summary>
        /// Deletes a document.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="id">The document id.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <param name="operationOptions">The operation options.</param>
        /// <returns>A task that represents the asynchronous delete operation.</returns>
        Task DeleteDocumentAsync<TDocument>(string id, DocumentTypeMapping<TDocument> mapping, OperationOptions operationOptions);

        /// <summary>
        /// Gets a document attachment.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <typeparam name="TAttachment">The type of attachment.</typeparam>
        /// <param name="document">The document.</param>
        /// <param name="documentVersion">The document version.</param>
        /// <param name="attachmentMapping">The attachment mapping.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        Task<TAttachment> GetAttachmentAsync<TDocument, TAttachment>(TDocument document, int documentVersion, AttachmentTypeMapping<TDocument, TAttachment> attachmentMapping);

        /// <summary>
        /// Creates a document attachment.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <typeparam name="TAttachment">The type of attachment.</typeparam>
        /// <param name="document">The document.</param>
        /// <param name="documentVersion">The document version.</param>
        /// <param name="attachmentMapping">The attachment mapping.</param>
        /// <param name="attachment">The attachment.</param>
        /// <returns>A task that represents the asynchronous get operation.</returns>
        Task CreateAttachmentAsync<TDocument, TAttachment>(TDocument document, int documentVersion, AttachmentTypeMapping<TDocument, TAttachment> attachmentMapping, TAttachment attachment);
    }
}