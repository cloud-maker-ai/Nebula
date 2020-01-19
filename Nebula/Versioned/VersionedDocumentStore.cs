using System;
using Nebula.Config;

namespace Nebula.Versioned
{
    /// <summary>
    /// A base class for versioned document store implementations.
    /// </summary>
    public abstract class VersionedDocumentStore : IDocumentStoreConfigSource
    {
        /// <summary>
        /// Creates a new instance of the <see cref="VersionedDocumentStore"/> class.
        /// </summary>
        /// <param name="dbAccessProvider">The db access provider.</param>
        /// <param name="registerConfigSource">A boolean indicating if the config source should be registered.</param>
        /// <remarks>
        /// <para>This store is added to the database configuration registry for management if
        /// <paramref name="registerConfigSource"/> is <c>true</c>.</para>
        /// <para>If <paramref name="registerConfigSource"/> is false then the store must be registered by the caller.</para>
        /// </remarks>
        protected VersionedDocumentStore(IDocumentDbAccessProvider dbAccessProvider, bool registerConfigSource)
        {
            if (dbAccessProvider == null)
                throw new ArgumentNullException(nameof(dbAccessProvider));

            DbAccess = dbAccessProvider.GetDbAccess();

            if (registerConfigSource)
            {
                DbAccess.ConfigRegistry.RegisterStoreConfigSource(this);
            }
        }

        protected abstract DocumentStoreConfig StoreConfig { get; }

        protected abstract IVersionedDocumentStoreClient StoreClient { get; }

        protected IDocumentDbAccess DbAccess { get; }

        protected static IVersionedDocumentStoreClient CreateStoreLogic(IDocumentDbAccess dbAccess, DocumentStoreConfig config)
        {
            var documentDbAccess = dbAccess as DocumentDbAccess;

            if (documentDbAccess == null)
            {
                throw new InvalidOperationException("Document db access interface is not a supported type");
            }

            return new VersionedDocumentStoreClient(documentDbAccess, config);
        }

        protected static IVersionedDocumentStoreClient CreateStoreLogic(
            IDocumentDbAccess dbAccess,
            DocumentStoreConfig config,
            IDocumentMetadataSource metadataSource)
        {
            var documentDbAccess = dbAccess as DocumentDbAccess;

            if (documentDbAccess == null)
            {
                throw new InvalidOperationException("Document db access interface is not a supported type");
            }

            return new VersionedDocumentStoreClient(documentDbAccess, config, metadataSource);
        }

        protected void ThrowTerminatingError(string message, Exception exception = null)
        {
            throw new NebulaStoreException(message, exception);
        }

        DocumentStoreConfig IDocumentStoreConfigSource.GetConfig()
        {
            return StoreConfig;
        }
    }
}