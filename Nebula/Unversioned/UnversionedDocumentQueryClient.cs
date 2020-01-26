using System;
using System.Collections.Generic;
using System.Linq;
using Nebula.Config;
using Nebula.Utils;

namespace Nebula.Unversioned
{
    /// <summary>
    /// A store client for un-versioned query operations.
    /// </summary>
    internal class UnversionedDocumentQueryClient : DocumentQueryClient<UnversionedDocumentStoreClient.UnversionedDbDocument>
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="UnversionedDocumentQueryClient"/> class.
        /// </summary>
        /// <param name="dbAccess">The db access interface.</param>
        /// <param name="config">The store config.</param>
        public UnversionedDocumentQueryClient(DocumentDbAccess dbAccess, DocumentStoreConfig config)
            : base(dbAccess, config, null)
        {
        }

        public IQueryable<UnversionedDocumentStoreClient.UnversionedDbDocument> CreateQueryById<TDocument>(
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

        public List<IQueryable<UnversionedDocumentStoreClient.UnversionedDbDocument>> CreateQueryByIds<TDocument>(
            ICollection<string> ids,
            DocumentTypeMapping<TDocument> mapping)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            var batchSize = DbAccess.QueryPolicy.GetIdSearchLimit(ids);
            var batched = ids.Batch(batchSize);

            var result = new List<IQueryable<UnversionedDocumentStoreClient.UnversionedDbDocument>>();

            foreach (var batch in batched)
            {
                var query = CreateQueryByIdsImpl(batch.ToArray(), mapping);
                result.Add(query);
            }

            return result;
        }

        public IQueryable<UnversionedDocumentStoreClient.UnversionedDbDocument> CreateQueryAll<TDocument>(
            DocumentTypeMapping<TDocument> mapping)
        {
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            return CreateQuery(mapping, null);
        }

        private IQueryable<UnversionedDocumentStoreClient.UnversionedDbDocument> CreateQueryByIdsImpl<TDocument>(
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

            return CreateQuery(mapping, query);
        }
    }
}