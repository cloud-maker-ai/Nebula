namespace Nebula.AspNetCore
{
    /// <summary>
    /// Defines Nebula config.
    /// </summary>
    internal class NebulaConfig
    {
        /// <summary>
        /// The CosmosDb service endpoint address.
        /// </summary>
        public string ServiceEndpoint { get; set; }

        /// <summary>
        /// The CosmosDb auth key.
        /// </summary>
        public string AuthKey { get; set; }

        /// <summary>
        /// The database id.
        /// </summary>
        public string DatabaseId { get; set; }

        /// <summary>
        /// The collection name.
        /// </summary>
        public string CollectionName { get; set; }

        /// <summary>
        /// The default collection RU allocation.
        /// </summary>
        public int? DefaultRus { get; set; }
    }
}
