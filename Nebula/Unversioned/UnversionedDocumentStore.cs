using System;
using Nebula.Config;

namespace Nebula.Unversioned
{
    /// <summary>
    /// A base class for un-versioned document store implementations.
    /// </summary>
    public abstract class UnversionedDocumentStore : IDocumentStoreConfigSource
    {
        /// <summary>
        /// Creates a new instance of the <see cref="UnversionedDocumentStore"/> class.
        /// </summary>
        /// <param name="dbAccessProvider">The db access provider.</param>
        protected UnversionedDocumentStore(IDocumentDbAccessProvider dbAccessProvider)
        {
            if (dbAccessProvider == null)
                throw new ArgumentNullException(nameof(dbAccessProvider));

            DbAccess = dbAccessProvider.GetDbAccess();
        }

        protected abstract DocumentStoreConfig StoreConfig { get; }

        protected abstract IUnversionedDocumentStoreClient StoreClient { get; }

        protected IDocumentDbAccess DbAccess { get; }

        protected static IUnversionedDocumentStoreClient CreateStoreLogic(
            IDocumentDbAccess dbAccess,
            DocumentStoreConfig config)
        {
            return new UnversionedDocumentStoreClient(GetDbAccessImplementation(dbAccess), config);
        }

        protected static IUnversionedDocumentStoreClient CreateStoreLogic(
            IDocumentDbAccess dbAccess,
            DocumentStoreConfig config,
            IDocumentMetadataSource metadataSource)
        {
            return new UnversionedDocumentStoreClient(GetDbAccessImplementation(dbAccess), config, metadataSource);
        }

        protected void ThrowTerminatingError(string message, Exception exception = null)
        {
            throw new NebulaStoreException(message, exception);
        }

        DocumentStoreConfig IDocumentStoreConfigSource.GetConfig()
        {
            return StoreConfig;
        }

        private static DocumentDbAccess GetDbAccessImplementation(IDocumentDbAccess dbAccess)
        {
            var documentDbAccess = dbAccess as DocumentDbAccess;

            if (documentDbAccess == null)
            {
                throw new InvalidOperationException("Document db access interface is not a supported type");
            }

            return documentDbAccess;
        }
    }
}