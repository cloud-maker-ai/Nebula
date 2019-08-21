using System;

namespace Nebula.Versioned
{
    /// <summary>
    /// A document store result.
    /// </summary>
    public class VersionedDocumentUpsertResult<TDocument>
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentUpsertResult{TDocument}"/> class.
        /// </summary>
        /// <param name="documentId">The document id.</param>
        /// <param name="metadata">The document metadata.</param>
        /// <param name="document">The document that was stored.</param>
        public VersionedDocumentUpsertResult(string documentId, VersionedDocumentMetadata metadata, TDocument document)
        {
            if (documentId == null)
                throw new ArgumentNullException(nameof(documentId));
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            DocumentId = documentId;
            DocumentVersion = metadata.Version;
            Document = document;
            DocumentWithMetadata = new VersionedDocumentWithMetadata<TDocument>(metadata, document);
        }

        /// <summary>
        /// Gets the document id.
        /// </summary>
        public string DocumentId { get; }

        /// <summary>
        /// Gets the document version.
        /// </summary>
        public int DocumentVersion { get; }

        /// <summary>
        /// Gets the document that was stored.
        /// </summary>
        public TDocument Document { get; }

        /// <summary>
        /// Gets the document that was stored, along with the version metadata.
        /// </summary>
        public VersionedDocumentWithMetadata<TDocument> DocumentWithMetadata { get; }
    }
}