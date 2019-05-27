using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Nebula.Config
{
    /// <summary>
    /// A builder for <see cref="DocumentStoreConfig"/> classes.
    /// </summary>
    public class DocumentStoreConfigBuilder
    {
        private readonly string _storeName;
        private readonly Dictionary<string, DocumentConfigBuilder> _documents;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentStoreConfigBuilder"/> class.
        /// </summary>
        /// <param name="storeName">The store name.</param>
        public DocumentStoreConfigBuilder(string storeName)
        {
            if (storeName == null)
                throw new ArgumentNullException(nameof(storeName));

            _storeName = storeName;
            _documents = new Dictionary<string, DocumentConfigBuilder>();
        }

        /// <summary>
        /// Adds document configuration.
        /// </summary>
        /// <param name="name">The document name.</param>
        /// <returns>The document configuration builder.</returns>
        public DocumentConfigBuilder AddDocument(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (_documents.ContainsKey(name))
                throw new ArgumentException("Document already added", nameof(name));

            var documentBuilder = new DocumentConfigBuilder(name);

            _documents[name] = documentBuilder;

            return documentBuilder;
        }

        /// <summary>
        /// Creates a new document mapping.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <param name="documentName">The document name.</param>
        /// <returns>The document mapping builder.</returns>
        /// <remarks>
        /// <para>The document must have already been added to this builder.</para>
        /// </remarks>
        public DocumentTypeMappingBuilder<TDocument> AddDocumentMapping<TDocument>(string documentName)
        {
            if (!_documents.TryGetValue(documentName, out var documentConfigBuilder))
                throw new ArgumentException("Document not defined", nameof(documentName));

            return new DocumentTypeMappingBuilder<TDocument>(documentConfigBuilder, documentName);
        }

        /// <summary>
        /// Builds a <see cref="DocumentStoreConfig"/> class.
        /// </summary>
        /// <returns>The document store configuration.</returns>
        public DocumentStoreConfig Finish()
        {
            Dictionary<string, DocumentConfig> documentTypes = new Dictionary<string, DocumentConfig>();

            foreach (var pair in _documents)
            {
                documentTypes[pair.Key] = pair.Value.Finish();
            }

            return new DocumentStoreConfig(_storeName, ImmutableList.CreateRange(documentTypes.Values));
        }
    }
}