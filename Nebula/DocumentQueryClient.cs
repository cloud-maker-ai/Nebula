using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Nebula.Config;

namespace Nebula
{
    /// <summary>
    /// A base implementation for document query clients.
    /// </summary>
    /// <typeparam name="TDbDocument">Type type of DB document.</typeparam>
    internal abstract class DocumentQueryClient<TDbDocument> : DocumentStoreClient<TDbDocument>
        where TDbDocument : DocumentStoreClient<TDbDocument>.DbDocument
    {
        protected DocumentQueryClient(
            DocumentDbAccess dbAccess,
            DocumentStoreConfig config,
            IEnumerable<Type> storedProcedures) : base(dbAccess, config, storedProcedures)
        {
        }

        public IQueryable<TDbDocument> CreateQuery<TDocument>(
            DocumentTypeMapping<TDocument> mapping,
            string query,
            IEnumerable<DbParameter> parameters = null)
        {
            FeedOptions queryOptions = new FeedOptions
            {
                MaxItemCount = -1,
                EnableCrossPartitionQuery = true
            };

            var contentKey = CreateContentKey(mapping);

            string selectStatement = $"SELECT * FROM {DbAccess.DbConfig.CollectionName} as c WHERE is_defined(c.{contentKey})";

            if (query != null)
            {
                // Perform substitution on references to the internal document. '[x].' is replaced while excluding
                // occurrences in a string.
                string substitutedClause = Regex.Replace(query, "\\[x\\]\\.(?=[^']*(?:'[^']*'[^']*)*$)", $"c.{contentKey}.");

                selectStatement = $"{selectStatement} AND ({substitutedClause})";
            }

            if (!DbAccess.QueryPolicy.IsQueryValid(selectStatement))
            {
                throw new NebulaStoreException("Failed to create document query");
            }

            var querySpec = CreateQuerySpec(selectStatement, parameters);

            return MakeClientCall(
                () => DbAccess.DbClient.CreateDocumentQuery<TDbDocument>(CollectionUri, querySpec, queryOptions),
                "Failed to create document query");
        }

        protected SqlQuerySpec CreateQuerySpec(string queryText, IEnumerable<DbParameter> parameters)
        {
            var querySpec = new SqlQuerySpec { QueryText = queryText };

            if (parameters != null)
            {
                querySpec.Parameters = new SqlParameterCollection(
                    parameters.Select(p => new SqlParameter(p.Name, p.Value)));
            }

            return querySpec;
        }
    }
}