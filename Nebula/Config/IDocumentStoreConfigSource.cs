namespace Nebula.Config
{
    /// <summary>
    /// Defines methods for accessing document store configuration.
    /// </summary>
    public interface IDocumentStoreConfigSource
    {
        /// <summary>
        /// Gets the store's configuration.
        /// </summary>
        /// <returns>The store's configuration.</returns>
        DocumentStoreConfig GetConfig();
    }
}