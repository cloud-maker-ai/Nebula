using System;
using System.Collections.Generic;
using System.Linq;
using Nebula.Config;
using Nebula.Utils;

namespace Nebula.Versioned
{
    /// <summary>
    /// A store client for query operations.
    /// </summary>
    internal class VersionedDocumentQueryClient : DocumentQueryClient<VersionedDocumentStoreClient.VersionedDbDocument>
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentQueryClient"/> class.
        /// </summary>
        /// <param name="dbAccess">The db access interface.</param>
        /// <param name="config">The store config.</param>
        public VersionedDocumentQueryClient(DocumentDbAccess dbAccess, DocumentStoreConfig config)
            : base(dbAccess, config, null)
        {
        }

        public IQueryable<VersionedDocumentStoreClient.VersionedDbDocument> CreateQueryById<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var idParameter = new DbParameter("id", id);
            var query = $"[x].{mapping.IdPropertyName} = @id";

            return CreateQueryByLatest(mapping, query, new[] { idParameter });
        }

        public IQueryable<VersionedDocumentStoreClient.VersionedDbDocument> CreateQueryById<TDocument>(
            string id,
            int version,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var idParameter = new DbParameter("id", id);
            var query = $"[x].{mapping.IdPropertyName} = @id";

            return CreateQueryByVersion(mapping, query, version, new[] { idParameter });
        }

        public IQueryable<VersionedDocumentStoreClient.VersionedDbDocument> CreateQueryAllVersionsById<TDocument>(
            string id,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var idParameter = new DbParameter("id", id);
            var query = $"[x].{mapping.IdPropertyName} = @id";

            return CreateQuery(mapping, query, new[] { idParameter });
        }

        public List<IQueryable<VersionedDocumentStoreClient.VersionedDbDocument>> CreateQueryByIds<TDocument>(
            ICollection<string> ids,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var batchSize = DbAccess.QueryPolicy.GetIdSearchLimit(ids);
            var batched = ids.Batch(batchSize);

            var result = new List<IQueryable<VersionedDocumentStoreClient.VersionedDbDocument>>();

            foreach (var batch in batched)
            {
                var query = CreateQueryByIdsImpl(batch.ToArray(), mapping);
                result.Add(query);
            }

            return result;
        }

        public IQueryable<VersionedDocumentStoreClient.VersionedDbDocument> CreateQueryAll<TDocument>(
            DocumentTypeMapping<TDocument> mapping)
        {
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            return CreateQueryByLatest(mapping, null);
        }

        public IQueryable<VersionedDocumentStoreClient.VersionedDbDocument> CreateQueryByLatest<TDocument>(
            DocumentTypeMapping<TDocument> mapping,
            string query,
            IEnumerable<DbParameter> parameters = null)
        {
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            if (query != null)
            {
                query = $"({query}) AND ";
            }

            // The first version is always fetched to get the creation time.
            query += "(c['@latest'] = true OR c['@version'] = 1)";

            return CreateQuery(mapping, query, parameters);
        }

        public IQueryable<VersionedDocumentStoreClient.VersionedDbDocument> CreateQueryByVersion<TDocument>(
            DocumentTypeMapping<TDocument> mapping,
            string query,
            int version,
            IEnumerable<DbParameter> parameters = null)
        {
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            if (query != null)
            {
                query = $"({query}) AND ";
            }

            // The first version is always fetched to get the creation time.
            query += $"(c['@version'] = {version} OR c['@version'] = 1)";

            return CreateQuery(mapping, query, parameters);
        }

        private IQueryable<VersionedDocumentStoreClient.VersionedDbDocument> CreateQueryByIdsImpl<TDocument>(
            ICollection<string> ids,
            DocumentTypeMapping<TDocument> mapping)
        {
            // Optimise queries for a single id to use EQUALS instead of IN.
            if (ids.Count == 1)
            {
                return CreateQueryById(ids.First(), mapping);
            }

            var inIds = "'" + string.Join("','", ids) + "'";

            var query = $"[x].{mapping.IdPropertyName} IN ({inIds})";

            return CreateQueryByLatest(mapping, query);
        }
    }
}