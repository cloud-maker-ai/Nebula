using System.Collections.Generic;
using System.Collections.Immutable;

namespace Nebula.Unversioned
{
    /// <summary>
    /// A document batch read result.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    public class DocumentBatchReadResult<TDocument>
    {
        private readonly ImmutableList<DocumentReadResult<TDocument>> _loaded;
        private readonly ImmutableList<string> _missing;
        private readonly ImmutableList<DocumentReadResult<TDocument>> _failed;

        private static readonly DocumentBatchReadResult<TDocument> _empty = new DocumentBatchReadResult<TDocument>(null, null, null);

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentBatchReadResult{TDocument}"/> class.
        /// </summary>
        /// <param name="loaded">The loaded documents.</param>
        /// <param name="missing">The missing documents.</param>
        /// <param name="failed">The failed documents.</param>
        public DocumentBatchReadResult(
            ImmutableList<DocumentReadResult<TDocument>> loaded,
            ImmutableList<string> missing,
            ImmutableList<DocumentReadResult<TDocument>> failed)
        {
            _loaded = loaded;
            _missing = missing;
            _failed = failed;
        }

        /// <summary>
        /// A result that includes no documents.
        /// </summary>
        public static DocumentBatchReadResult<TDocument> Empty
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
        /// Gets the missing documents.
        /// </summary>
        public IList<string> Missing
        {
            get { return _missing ?? ImmutableList<string>.Empty; }
        }

        /// <summary>
        /// Gets the failed documents.
        /// </summary>
        public IList<DocumentReadResult<TDocument>> Failed
        {
            get { return _failed ?? ImmutableList<DocumentReadResult<TDocument>>.Empty; }
        }
    }
}