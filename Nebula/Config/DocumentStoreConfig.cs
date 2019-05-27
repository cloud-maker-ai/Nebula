using System;
using System.Collections.Immutable;

namespace Nebula.Config
{
    /// <summary>
    /// Defines the configuration of a document store.
    /// </summary>
    public class DocumentStoreConfig
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentStoreConfig"/> class.
        /// </summary>
        /// <param name="storeName">The store name.</param>
        /// <param name="documentConfig">The store's document configuration.</param>
        internal DocumentStoreConfig(string storeName, ImmutableList<DocumentConfig> documentConfig)
        {
            if (storeName == null)
                throw new ArgumentNullException(nameof(storeName));
            if (documentConfig == null)
                throw new ArgumentNullException(nameof(documentConfig));

            StoreName = storeName;
            Documents = documentConfig;
        }

        /// <summary>
        /// The document store name.
        /// </summary>
        public string StoreName { get; }

        internal ImmutableList<DocumentConfig> Documents { get; }
    }
}