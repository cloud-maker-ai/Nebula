using System;
using Nebula.Config;

namespace Nebula
{
    /// <summary>
    /// A builder for <see cref="DocumentTypeMapping{TDocument}"/> classes.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    public class DocumentTypeMappingBuilder<TDocument>
    {
        private readonly DocumentConfigBuilder _documentConfigBuilder;
        private readonly string _documentName;
        private Func<TDocument, string> _idMapper;
        private Func<TDocument, string> _partitionMapper;
        private string _idPropertyName;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentTypeMappingBuilder{TDocument}"/> class.
        /// </summary>
        /// <param name="documentConfigBuilder">The document configuration builder.</param>
        /// <param name="documentName">The document name.</param>
        internal DocumentTypeMappingBuilder(DocumentConfigBuilder documentConfigBuilder, string documentName)
        {
            if (documentConfigBuilder == null)
                throw new ArgumentNullException(nameof(documentConfigBuilder));
            if (documentName == null)
                throw new ArgumentNullException(nameof(documentName));

            _idPropertyName = "Id";
            _documentConfigBuilder = documentConfigBuilder;
            _documentName = documentName;
        }

        /// <summary>
        /// Sets the document id mapper.
        /// </summary>
        /// <param name="mappingFunc">The document id mapper.</param>
        /// <returns>The builder.</returns>
        public DocumentTypeMappingBuilder<TDocument> SetIdMapper(Func<TDocument, string> mappingFunc)
        {
            if (mappingFunc == null)
                throw new ArgumentNullException(nameof(mappingFunc));

            _idMapper = mappingFunc;
            return this;
        }

        /// <summary>
        /// Sets the partition mapper.
        /// </summary>
        /// <param name="mappingFunc">The partition mapper.</param>
        /// <returns>The builder.</returns>
        public DocumentTypeMappingBuilder<TDocument> SetPartitionMapper(Func<TDocument, string> mappingFunc)
        {
            if (mappingFunc == null)
                throw new ArgumentNullException(nameof(mappingFunc));

            _partitionMapper = mappingFunc;
            return this;
        }

        /// <summary>
        /// Sets the id property name.
        /// </summary>
        /// <param name="propertyName">The name of the property containing the document id.</param>
        /// <returns>The builder.</returns>
        /// <remarks>
        /// <para>The property name is 'Id' by default. Calling this method changes the default value.</para>
        /// </remarks>
        public DocumentTypeMappingBuilder<TDocument> SetPartitionMapper(string propertyName)
        {
            if (propertyName == null)
                throw new ArgumentNullException(nameof(propertyName));

            _idPropertyName = propertyName;
            return this;
        }

        /// <summary>
        /// Builds a <see cref="DocumentTypeMapping{TDocument}"/> class.
        /// </summary>
        /// <returns>The document type mapping.</returns>
        public DocumentTypeMapping<TDocument> Finish()
        {
            if (_idMapper == null)
            {
                throw new InvalidOperationException("Id mapper required");
            }

            if (_partitionMapper == null)
            {
                throw new InvalidOperationException("Partition mapper required");
            }

            return new DocumentTypeMapping<TDocument>(_documentConfigBuilder, _documentName, _idMapper, _partitionMapper, _idPropertyName);
        }
    }
}