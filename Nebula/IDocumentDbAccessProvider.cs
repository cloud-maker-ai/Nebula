namespace Nebula
{
    /// <summary>
    /// Provides methods for getting the current document db access context.
    /// </summary>
    public interface IDocumentDbAccessProvider
    {
        /// <summary>
        /// Gets the current db access context.
        /// </summary>
        /// <returns>The db access context.</returns>
        IDocumentDbAccess GetDbAccess();
    }
}