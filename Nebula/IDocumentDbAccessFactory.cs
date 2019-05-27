namespace Nebula
{
    /// <summary>
    /// Provides methods for creating document db access contexts.
    /// </summary>
    public interface IDocumentDbAccessFactory
    {
        /// <summary>
        /// Creates a document database access context.
        /// </summary>
        /// <returns>The database access context.</returns>
        IDocumentDbAccess Create();
    }
}