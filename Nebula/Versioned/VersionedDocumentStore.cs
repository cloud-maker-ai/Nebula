﻿using System;
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

        /// <summary>
        /// Creates a new instance of the <see cref="VersionedDocumentStore"/> class.
        /// </summary>
        /// <param name="dbAccessProvider">The db access provider.</param>
        protected VersionedDocumentStore(IDocumentDbAccessProvider dbAccessProvider)
            : this(dbAccessProvider, false)
        {
        }

        protected abstract DocumentStoreConfig StoreConfig { get; }

        protected abstract IVersionedDocumentStoreClient StoreClient { get; }

        protected virtual IVersionedDocumentStorePurgeClient PurgeClient
        {
            get { throw new InvalidOperationException("Document store not setup with purge client"); }
        }

        protected IDocumentDbAccess DbAccess { get; }

        protected static IVersionedDocumentStoreClient CreateStoreLogic(
            IDocumentDbAccess dbAccess,
            DocumentStoreConfig config)
        {
            return new VersionedDocumentStoreClient(GetDbAccessImplementation(dbAccess), config);
        }

        protected static IVersionedDocumentStoreClient CreateStoreLogic(
            IDocumentDbAccess dbAccess,
            DocumentStoreConfig config,
            IDocumentMetadataSource metadataSource)
        {
            return new VersionedDocumentStoreClient(GetDbAccessImplementation(dbAccess), config, metadataSource);
        }

        protected static IVersionedDocumentStorePurgeClient CreatePurgeLogic(
            IDocumentDbAccess dbAccess,
            DocumentStoreConfig config)
        {
            var dbAccessImpl = GetDbAccessImplementation(dbAccess);
            var queryClient = new VersionedDocumentQueryClient(dbAccessImpl, config);

            return new VersionedDocumentStorePurgeClient(dbAccessImpl, config, queryClient);
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