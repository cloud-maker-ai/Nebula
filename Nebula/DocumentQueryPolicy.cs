using System;
using System.Collections.Generic;

namespace Nebula
{
    /// <summary>
    /// An implementation of <see cref="IDocumentQueryPolicy"/> for CosmosDb.
    /// </summary>
    internal class DocumentQueryPolicy : IDocumentQueryPolicy
    {
        /// <inheritdoc />
        public int GetIdSearchLimit(ICollection<string> ids)
        {
            if (ids == null)
                throw new ArgumentNullException(nameof(ids));

            // The limit for GUID based ids is in the range of 700-800. This limit is conservative based on that.
            // A smarter implementation could take sample of id lengths and determine the limit. The logic here
            // is kept basic for now.
            return 500;
        }

        /// <inheritdoc />
        public bool IsQueryValid(string query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            // The absolute limit on query length in CosmosDb is known.
            return query.Length < 30720;
        }
    }
}