using System;

namespace Nebula.Unversioned
{
    /// <summary>
    /// A document with associated metadata.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    public class DocumentWithMetadata<TDocument>
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentWithMetadata{TDocument}"/> class.
        /// </summary>
        /// <param name="metadata">The metadata.</param>
        /// <param name="document">The document.</param>
        public DocumentWithMetadata(DocumentMetadata metadata, TDocument document)
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
        public DocumentMetadata Metadata { get; }

        /// <summary>
        /// Gets the document.
        /// </summary>
        public TDocument Document { get; }
    }
}