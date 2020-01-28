using System.Threading.Tasks;

namespace Nebula.Versioned
{
    /// <summary>
    /// Provides operations for purging documents from a store.
    /// </summary>
    /// <remarks>
    /// <para>Warning: The client provides low-level operation that perform hard-deletes on
    /// store data. Executing operations will result in documents being completely removed
    /// from the target database. Restore operations will not be available following a purge
    /// and version history will no longer be available.</para>
    /// </remarks>
    public interface IVersionedDocumentStorePurgeClient
    {
        /// <summary>
        /// Purges a document from the store.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="id">The document id.</param>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous purge operation.</returns>
        /// <remarks>
        /// <para>Warning: This is a hard-delete operation. The data and version history
        /// will be removed from the store.</para>
        /// </remarks>
        Task PurgeDocumentAsync<TDocument>(string id, DocumentTypeMapping<TDocument> mapping);

        /// <summary>
        /// Purges all documents of a type from the store.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="mapping">The document type mapping.</param>
        /// <returns>A task that represents the asynchronous purge operation.</returns>
        /// <remarks>
        /// <para>Warning: This is a hard-delete operation. The data and version history
        /// will be removed from the store.</para>
        /// </remarks>
        Task PurgeDocumentsAsync<TDocument>(DocumentTypeMapping<TDocument> mapping);
    }
}