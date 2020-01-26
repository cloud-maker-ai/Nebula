using System;

namespace Nebula.Unversioned
{
    /// <summary>
    /// A document read result.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    public class DocumentReadResult<TDocument>
    {
        private DocumentReadResult(
            string documentId,
            DocumentMetadata metadata,
            TDocument document,
            DocumentReadFailureDetails failureDetails,
            DocumentReadResultType resultType)
        {
            DocumentId = documentId;
            FailureDetails = failureDetails;
            ResultType = resultType;

            // Both may be null.
            Metadata = metadata;
            Document = document;
        }

        /// <summary>
        /// Gets the document metadata.
        /// </summary>
        public DocumentMetadata Metadata { get; }

        /// <summary>
        /// Gets the document id.
        /// </summary>
        public string DocumentId { get; }

        /// <summary>
        /// Gets the document.
        /// </summary>
        public TDocument Document { get; }

        /// <summary>
        /// Gets the failure details.
        /// </summary>
        public DocumentReadFailureDetails FailureDetails { get; }

        /// <summary>
        /// Gets the read result type.
        /// </summary>
        public DocumentReadResultType ResultType { get; }

        /// <summary>
        /// Creates a successful read result.
        /// </summary>
        /// <param name="documentId">The document id.</param>
        /// <param name="metadata">The document metadata.</param>
        /// <param name="document">The document.</param>
        /// <returns>A document read result indicating success.</returns>
        public static DocumentReadResult<TDocument> CreateOkay(string documentId, DocumentMetadata metadata, TDocument document)
        {
            if (documentId == null)
                throw new ArgumentNullException(nameof(documentId));
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));
            if (document == null)
                throw new ArgumentNullException(nameof(document));

            return new DocumentReadResult<TDocument>(documentId, metadata, document, null, DocumentReadResultType.Loaded);
        }

        /// <summary>
        /// Creates a unsuccessful read result.
        /// </summary>
        /// <param name="documentId">The document id.</param>
        /// <param name="metadata">The document metadata.</param>
        /// <param name="failureDetails">The failure details.</param>
        /// <returns>A document read result indicating failure.</returns>
        public static DocumentReadResult<TDocument> CreateFailure(
            string documentId,
            DocumentMetadata metadata,
            DocumentReadFailureDetails failureDetails)
        {
            if (documentId == null)
                throw new ArgumentNullException(nameof(documentId));
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));
            if (failureDetails == null)
                throw new ArgumentNullException(nameof(failureDetails));

            return new DocumentReadResult<TDocument>(documentId, metadata, default(TDocument), failureDetails, DocumentReadResultType.Failed);
        }
    }
}