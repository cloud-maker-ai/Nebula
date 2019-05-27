using System.Collections.Generic;

namespace Nebula
{
    /// <summary>
    /// Defines the document query policies.
    /// </summary>
    public interface IDocumentQueryPolicy
    {
        /// <summary>
        /// Gets the maximum number of ids allowed in a single query.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns>The maximum number of ids allowed in a query.</returns>
        int GetIdSearchLimit(ICollection<string> ids);

        /// <summary>
        /// Checks if a document query is valid.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns><c>true</c> if the query is valid; otherwise <c>false</c>.</returns>
        bool IsQueryValid(string query);
    }
}