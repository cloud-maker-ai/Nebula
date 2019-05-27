using System;

namespace Nebula.Versioned
{
    /// <summary>
    /// A document with associated metadata.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    public class VersionedDocumentWithMetadata<TDocument>
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentWithMetadata{TDocument}"/> class.
        /// </summary>
        /// <param name="metadata">The metadata.</param>
        /// <param name="document">The document.</param>
        public VersionedDocumentWithMetadata(VersionedDocumentMetadata metadata, TDocument document)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            Metadata = metadata;
            Document = document;
        }

        /// <summary>
        /// Gets the metadata.
        /// </summary>
        public VersionedDocumentMetadata Metadata { get; }

        /// <summary>
        /// Gets the document.
        /// </summary>
        public TDocument Document { get; }
    }
}