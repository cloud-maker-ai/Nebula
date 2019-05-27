using System.Collections.Generic;
using System.Collections.Immutable;

namespace Nebula.Versioned
{
    /// <summary>
    /// A document batch read result.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    public class VersionedDocumentBatchReadResult<TDocument>
    {
        private readonly ImmutableList<VersionedDocumentReadResult<TDocument>> _loaded;
        private readonly ImmutableList<string> _missing;
        private readonly ImmutableList<VersionedDocumentReadResult<TDocument>> _failed;

        private static readonly VersionedDocumentBatchReadResult<TDocument> _empty = new VersionedDocumentBatchReadResult<TDocument>(null, null, null);

        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentBatchReadResult{TDocument}"/> class.
        /// </summary>
        /// <param name="loaded">The loaded documents.</param>
        /// <param name="missing">The missing documents.</param>
        /// <param name="failed">The failed documents.</param>
        public VersionedDocumentBatchReadResult(
            ImmutableList<VersionedDocumentReadResult<TDocument>> loaded,
            ImmutableList<string> missing,
            ImmutableList<VersionedDocumentReadResult<TDocument>> failed)
        {
            _loaded = loaded;
            _missing = missing;
            _failed = failed;
        }

        /// <summary>
        /// A result that includes no documents.
        /// </summary>
        public static VersionedDocumentBatchReadResult<TDocument> Empty
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
        /// Gets the missing documents.
        /// </summary>
        public IList<string> Missing
        {
            get { return _missing ?? ImmutableList<string>.Empty; }
        }

        /// <summary>
        /// Gets the failed documents.
        /// </summary>
        public IList<VersionedDocumentReadResult<TDocument>> Failed
        {
            get { return _failed ?? ImmutableList<VersionedDocumentReadResult<TDocument>>.Empty; }
        }
    }
}