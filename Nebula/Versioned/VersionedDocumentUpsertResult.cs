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
        /// <param name="documentVersion">The document version.</param>
        /// <param name="document">The document that was stored.</param>
        public VersionedDocumentUpsertResult(string documentId, int documentVersion, TDocument document)
        {
            if (documentId == null)
                throw new ArgumentNullException(nameof(documentId));
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            DocumentId = documentId;
            DocumentVersion = documentVersion;
            Document = document;
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
    }
}