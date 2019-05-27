using System;
using System.Collections.Immutable;

namespace Nebula.Versioned
{
    /// <summary>
    /// A document metadata read result.
    /// </summary>
    public class VersionedDocumentMetadataReadResult
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentMetadataReadResult"/> class.
        /// </summary>
        /// <param name="documentId">The document id.</param>
        /// <param name="metadata">The document metadata records.</param>
        public VersionedDocumentMetadataReadResult(string documentId, ImmutableList<VersionedDocumentMetadata> metadata)
        {
            if (documentId == null)
                throw new ArgumentNullException(nameof(documentId));
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            DocumentId = documentId;
            Metadata = metadata;
        }

        /// <summary>
        /// The document id.
        /// </summary>
        public string DocumentId { get; }

        /// <summary>
        /// The document metadata records.
        /// </summary>
        public ImmutableList<VersionedDocumentMetadata> Metadata { get; }
    }
}