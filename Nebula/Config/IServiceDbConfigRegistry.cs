namespace Nebula.Config
{
    /// <summary>
    /// Defines methods for managing a service's configuration registry.
    /// </summary>
    /// <remarks>
    /// <para>The registry accepts configuration from different aspects of a service database. That includes store specific
    /// configuration but may also include global configuration that applies overall to the service.</para>
    /// </remarks>
    public interface IServiceDbConfigRegistry
    {
        /// <summary>
        /// Registers a document store configuration source.
        /// </summary>
        /// <param name="configSource">The store configuration source.</param>
        void RegisterStoreConfigSource(IDocumentStoreConfigSource configSource);
    }
}