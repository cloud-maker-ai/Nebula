namespace Nebula.Config
{
    /// <summary>
    /// Defines methods for managing a service's configuration registry.
    /// </summary>
    /// <remarks>
    /// <para>The registry accepts configuration from different aspects of a service database. That includes store speicifc
    /// configuration but may also include global configuration that applies overall to the service.</para>
    /// </remarks>
    public interface IServiceDbConfigRegistry
    {
        /// <summary>
        /// Registers a document store configuration source.
        /// </summary>
        /// <param name="configSource">The store configuration source.</param>
        void RegisterStoreConfigSource(IDocumentStoreConfigSource configSource);

        /// <summary>
        /// Checks if a document store configuration source has been registered.
        /// </summary>
        /// <param name="configSource">The store configuration source.</param>
        /// <returns><c>true</c> if the store has been registered; otherwise <c>false</c>.</returns>
        bool IsStoreConfigRegistered(IDocumentStoreConfigSource configSource);
    }
}