using System;

namespace Nebula
{
    /// <summary>
    /// Document database configuration.
    /// </summary>
    public class DocumentDbConfig
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentDbConfig"/> class.
        /// </summary>
        /// <param name="serviceEndpoint">The service endpoint.</param>
        /// <param name="authKey">The service authorisation key.</param>
        /// <param name="databaseId">The database id.</param>
        /// <param name="collectionName">The database collection name.</param>
        /// <param name="defaultRus">The default RU value to use for new collections.</param>
        public DocumentDbConfig(
            string serviceEndpoint,
            string authKey,
            string databaseId,
            string collectionName,
            int defaultRus)
        {
            if (serviceEndpoint == null)
                throw new ArgumentNullException(nameof(serviceEndpoint));
            if (authKey == null)
                throw new ArgumentNullException(nameof(authKey));
            if (databaseId == null)
                throw new ArgumentNullException(nameof(databaseId));
            if (collectionName == null)
                throw new ArgumentNullException(nameof(collectionName));
            if (defaultRus < 400)
                throw new ArgumentOutOfRangeException(nameof(defaultRus));

            ServiceEndpoint = serviceEndpoint;
            AuthKey = authKey;
            DatabaseId = databaseId;
            CollectionName = collectionName;
            DefaultRus = defaultRus;
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentDbConfig"/> class.
        /// </summary>
        /// <param name="serviceEndpoint">The service endpoint.</param>
        /// <param name="authKey">The service authorisation key.</param>
        /// <param name="databaseId">The database id.</param>
        /// <param name="collectionName">The database collection name.</param>
        public DocumentDbConfig(string serviceEndpoint, string authKey, string databaseId, string collectionName)
            : this(serviceEndpoint, authKey, databaseId, collectionName, 400)
        {
        }

        /// <summary>
        /// Gets the service endpoint.
        /// </summary>
        public string ServiceEndpoint { get; }

        /// <summary>
        /// Gets the service authorisation key.
        /// </summary>
        public string AuthKey { get; }

        /// <summary>
        /// Gets the database id.
        /// </summary>
        public string DatabaseId { get; }

        /// <summary>
        /// Gets the collection name.
        /// </summary>
        public string CollectionName { get; }

        /// <summary>
        /// The default RU value to use for new collections.
        /// </summary>
        public int DefaultRus { get; }
    }
}