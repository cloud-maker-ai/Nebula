using System;

namespace Nebula
{
    /// <summary>
    /// Provides the current database access context.
    /// </summary>
    public sealed class DocumentDbAccessProvider : IDocumentDbAccessProvider
    {
        private readonly IDocumentDbAccess _dbAccess;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentDbAccessProvider"/> class.
        /// </summary>
        /// <param name="dbAccessFactory">The db access factory.</param>
        public DocumentDbAccessProvider(IDocumentDbAccessFactory dbAccessFactory)
        {
            if (dbAccessFactory == null)
                throw new ArgumentNullException(nameof(dbAccessFactory));

            _dbAccess = dbAccessFactory.Create();
        }

        /// <inheritdoc />
        public IDocumentDbAccess GetDbAccess()
        {
            return _dbAccess;
        }
    }
}