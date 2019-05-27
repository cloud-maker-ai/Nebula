using System;

namespace Nebula
{
    /// <summary>
    /// A builder for <see cref="DocumentDbConfig"/> classes.
    /// </summary>
    public class DocumentDbConfigBuilder
    {
        private readonly string _serviceEndpoint;
        private readonly string _authKey;

        private string _databaseId = "nebuladb";
        private string _collectionName = "nebula";
        private int _defaultRus = 400;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentDbConfig"/> class.
        /// </summary>
        /// <param name="serviceEndpoint">The service endpoint.</param>
        /// <param name="authKey">The service authorisation key.</param>
        public DocumentDbConfigBuilder(string serviceEndpoint, string authKey)
        {
            if (serviceEndpoint == null)
                throw new ArgumentNullException(nameof(serviceEndpoint));
            if (authKey == null)
                throw new ArgumentNullException(nameof(authKey));

            _serviceEndpoint = serviceEndpoint;
            _authKey = authKey;
        }

        /// <summary>
        /// Sets the database id.
        /// </summary>
        /// <param name="databaseId">The database id.</param>
        /// <returns>The builder.</returns>
        public DocumentDbConfigBuilder SetDatabaseId(string databaseId)
        {
            if (databaseId == null)
                throw new ArgumentNullException(nameof(databaseId));

            _databaseId = databaseId;
            return this;
        }

        /// <summary>
        /// Sets the document collection name.
        /// </summary>
        /// <param name="collectionName">The collection name.</param>
        /// <returns>The builder.</returns>
        public DocumentDbConfigBuilder SetCollectionName(string collectionName)
        {
            if (collectionName == null)
                throw new ArgumentNullException(nameof(collectionName));

            _collectionName = collectionName;
            return this;
        }

        /// <summary>
        /// Sets the default RU value.
        /// </summary>
        /// <param name="defaultRus">The default RU value to use for new collections.</param>
        /// <returns>The builder.</returns>
        public DocumentDbConfigBuilder SetDefaultRus(int defaultRus)
        {
            if (defaultRus < 400)
                throw new ArgumentOutOfRangeException(nameof(defaultRus));

            _defaultRus = defaultRus;
            return this;
        }

        /// <summary>
        /// Builds a <see cref="DocumentDbConfig"/> class.
        /// </summary>
        /// <returns>The document configuration.</returns>
        public DocumentDbConfig Finish()
        {
            return new DocumentDbConfig(
                _serviceEndpoint,
                _authKey,
                _databaseId,
                _collectionName,
                _defaultRus);
        }
    }
}