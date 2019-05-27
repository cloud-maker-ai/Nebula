using System.Collections.Generic;
using System.Collections.Immutable;

namespace Nebula.Versioned
{
    /// <summary>
    /// A document query result.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    public class VersionedDocumentQueryResult<TDocument>
    {
        private readonly ImmutableList<VersionedDocumentReadResult<TDocument>> _loaded;
        private readonly ImmutableList<VersionedDocumentReadResult<TDocument>> _failed;

        private static readonly VersionedDocumentQueryResult<TDocument> _empty = new VersionedDocumentQueryResult<TDocument>(null, null);

        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentQueryResult{TDocument}"/> class.
        /// </summary>
        /// <param name="loaded">The loaded documents.</param>
        /// <param name="failed">The documents that failed to load.</param>
        public VersionedDocumentQueryResult(
            ImmutableList<VersionedDocumentReadResult<TDocument>> loaded,
            ImmutableList<VersionedDocumentReadResult<TDocument>> failed)
        {
            _loaded = loaded;
            _failed = failed;
        }

        /// <summary>
        /// Gets a query result representing no matched results.
        /// </summary>
        public static VersionedDocumentQueryResult<TDocument> Empty
        {
            get { return _empty; }
        }

        /// <summary>
        /// Gets the loaded documents.
        /// </summary>
        public IList<VersionedDocumentReadResult<TDocument>> Loaded
        {
            get { return _loaded ?? ImmutableList<VersionedDocumentReadResult<TDocument>>.Empty; }
        }

        /// <summary>
        /// Gets the documents that failed to load.
        /// </summary>
        public IList<VersionedDocumentReadResult<TDocument>> Failed
        {
            get { return _failed ?? ImmutableList<VersionedDocumentReadResult<TDocument>>.Empty; }
        }
    }
}