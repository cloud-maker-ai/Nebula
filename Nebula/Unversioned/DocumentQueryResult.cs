using System.Collections.Generic;
using System.Collections.Immutable;

namespace Nebula.Unversioned
{
    /// <summary>
    /// A document query result.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    public class DocumentQueryResult<TDocument>
    {
        private readonly ImmutableList<DocumentReadResult<TDocument>> _loaded;
        private readonly ImmutableList<DocumentReadResult<TDocument>> _failed;

        private static readonly DocumentQueryResult<TDocument> _empty = new DocumentQueryResult<TDocument>(null, null);

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentQueryResult{TDocument}"/> class.
        /// </summary>
        /// <param name="loaded">The loaded documents.</param>
        /// <param name="failed">The documents that failed to load.</param>
        public DocumentQueryResult(
            ImmutableList<DocumentReadResult<TDocument>> loaded,
            ImmutableList<DocumentReadResult<TDocument>> failed)
        {
            _loaded = loaded;
            _failed = failed;
        }

        /// <summary>
        /// Gets a query result representing no matched results.
        /// </summary>
        public static DocumentQueryResult<TDocument> Empty
        {
            get { return _empty; }
        }

        /// <summary>
        /// Gets the loaded documents.
        /// </summary>
        public IList<DocumentReadResult<TDocument>> Loaded
        {
            get { return _loaded ?? ImmutableList<DocumentReadResult<TDocument>>.Empty; }
        }

        /// <summary>
        /// Gets the documents that failed to load.
        /// </summary>
        public IList<DocumentReadResult<TDocument>> Failed
        {
            get { return _failed ?? ImmutableList<DocumentReadResult<TDocument>>.Empty; }
        }
    }
}