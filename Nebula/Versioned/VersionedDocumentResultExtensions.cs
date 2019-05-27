using System;

namespace Nebula.Versioned
{
    /// <summary>
    /// Extension methods for version document results.
    /// </summary>
    public static class VersionedDocumentResultExtensions
    {
        /// <summary>
        /// Creates a query result with converted documents.
        /// </summary>
        /// <typeparam name="TFrom">The type to convert from.</typeparam>
        /// <typeparam name="TTo">The type to convert to.</typeparam>
        /// <param name="result">The query result.</param>
        /// <param name="convertFunc">The function to convert to the target type.</param>
        /// <returns>The converted query result.</returns>
        public static VersionedDocumentQueryResult<TTo> WithType<TFrom, TTo>(
            this VersionedDocumentQueryResult<TFrom> result,
            Func<TFrom, TTo> convertFunc)
        {
            if (result == null)
                throw new ArgumentNullException(nameof(result));
            if (convertFunc == null)
                throw new ArgumentNullException(nameof(convertFunc));

            return VersionedResultUtils.ConvertType(result, convertFunc);
        }

        /// <summary>
        /// Creates a read result with a converted document.
        /// </summary>
        /// <typeparam name="TFrom">The type to convert from.</typeparam>
        /// <typeparam name="TTo">The type to convert to.</typeparam>
        /// <param name="result">The read result.</param>
        /// <param name="convertFunc">The function to convert to the target type.</param>
        /// <returns>The converted read result.</returns>
        public static VersionedDocumentReadResult<TTo> WithType<TFrom, TTo>(
            this VersionedDocumentReadResult<TFrom> result,
            Func<TFrom, TTo> convertFunc)
        {
            if (result == null)
                throw new ArgumentNullException(nameof(result));
            if (convertFunc == null)
                throw new ArgumentNullException(nameof(convertFunc));

            return VersionedResultUtils.ConvertType(result, convertFunc);
        }
    }
}