using System;
using Nebula.Config;

namespace Nebula
{
    /// <summary>
    /// Defines details and mappings required for reading and writing documents to storage.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    /// <remarks>
    /// <para>The document type mapping is used by store processing code to support reading and writing documents to the
    /// underlying storage.</para>
    /// <para>Common document operations require this mapping to provide common functionality for different types of
    /// documents.</para>
    /// </remarks>
    public class DocumentTypeMapping<TDocument>
    {
        private readonly DocumentConfigBuilder _documentConfigBuilder;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentTypeMapping{TDocument}" /> class.
        /// </summary>
        /// <param name="documentConfigBuilder">The document configuration builder.</param>
        /// <param name="documentName">The document name.</param>
        /// <param name="idMapper">The document id mapper.</param>
        /// <param name="partitionKeyMapper">The partition key mapper.</param>
        /// <param name="idPropertyName">The name of the property containing the document id.</param>
        internal DocumentTypeMapping(
            DocumentConfigBuilder documentConfigBuilder,
            string documentName,
            Func<TDocument, string> idMapper,
            Func<TDocument, string> partitionKeyMapper,
            string idPropertyName)
        {
            if (documentConfigBuilder == null)
                throw new ArgumentNullException(nameof(documentConfigBuilder));
            if (documentName == null)
                throw new ArgumentNullException(nameof(documentName));
            if (idMapper == null)
                throw new ArgumentNullException(nameof(idMapper));
            if (partitionKeyMapper == null)
                throw new ArgumentNullException(nameof(partitionKeyMapper));
            if (idPropertyName == null)
                throw new ArgumentNullException(nameof(idPropertyName));

            _documentConfigBuilder = documentConfigBuilder;

            DocumentName = documentName;
            IdMapper = idMapper;
            PartitionKeyMapper = partitionKeyMapper;
            IdPropertyName = idPropertyName;
        }

        /// <summary>
        /// The document type name.
        /// </summary>
        public string DocumentName { get; }

        /// <summary>
        /// Gets the id mapper.
        /// </summary>
        /// <remarks>
        /// <para>The id mapper returns a string from a given document.</para>
        /// </remarks>
        public Func<TDocument, string> IdMapper { get; }

        /// <summary>
        /// Gets the partition key mapper.
        /// </summary>
        /// <remarks>
        /// <para>The partition key mapper returns a partition key from a given document.</para>
        /// <para>The partition key is used to deterine what logical partition the document should be stored in. A key value
        /// should be picked that is the same for different versions of the same document. The key should also have enough
        /// entropy to spread documents over a high number of partitions.</para>
        /// </remarks>
        public Func<TDocument, string> PartitionKeyMapper { get; }

        /// <summary>
        /// Gets the name of the property containing the document id.
        /// </summary>
        public string IdPropertyName { get; }

        /// <summary>
        /// Creates a new attachment mapping.
        /// </summary>
        /// <typeparam name="TAttachment">The type of attachment.</typeparam>
        /// <param name="attachmentName">The attachment name.</param>
        /// <returns>The attachment mapping builder.</returns>
        public AttachmentTypeMappingBuilder<TDocument, TAttachment> AddAttachmentMapping<TAttachment>(string attachmentName)
        {
            if (attachmentName == null)
                throw new ArgumentNullException(nameof(attachmentName));

            return _documentConfigBuilder.AddAttachmentMapping<TDocument, TAttachment>(this, attachmentName);
        }
    }
}