using System;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Nebula.Config;
using Nebula.Service;

namespace Nebula
{
    /// <inheritdoc />
    public sealed class DocumentDbAccess : IDocumentDbAccess
    {
        private readonly DocumentDbConfig _dbConfig;
        private readonly IDocumentDbService _dbService;
        private readonly IDocumentClient _client;
        private readonly ServiceDbConfigManager _configManager;
        private readonly IDocumentQueryPolicy _queryPolicy;

        private bool _started;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentDbAccess"/> class.
        /// </summary>
        /// <param name="dbConfig">The database config.</param>
        /// <param name="configManager">The document config manager.</param>
        public DocumentDbAccess(DocumentDbConfig dbConfig, ServiceDbConfigManager configManager)
        {
            if (dbConfig == null)
                throw new ArgumentNullException(nameof(dbConfig));
            if (configManager == null)
                throw new ArgumentNullException(nameof(configManager));

            _dbConfig = dbConfig;
            _configManager = configManager;
            _queryPolicy = new DocumentQueryPolicy();

            var dbService = new DocumentDbService(dbConfig);
            _dbService = dbService;
            _client = dbService.Client;
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentDbAccess"/> class for testing.
        /// </summary>
        /// <param name="dbConfig">The database config,</param>
        /// <param name="configManager">The document config manager.</param>
        /// <param name="documentClient">The document client.</param>
        /// <param name="dbService">The document db service.</param>
        /// <param name="queryPolicy">The document query policy.</param>
        /// <remarks>
        /// <para>This constructor should be used for internal testing only.</para>
        /// </remarks>
        internal DocumentDbAccess(
            DocumentDbConfig dbConfig,
            ServiceDbConfigManager configManager,
            IDocumentClient documentClient,
            IDocumentDbService dbService,
            IDocumentQueryPolicy queryPolicy)
        {
            if (dbConfig == null)
                throw new ArgumentNullException(nameof(dbConfig));
            if (configManager == null)
                throw new ArgumentNullException(nameof(configManager));
            if (documentClient == null)
                throw new ArgumentNullException(nameof(documentClient));
            if (dbService == null)
                throw new ArgumentNullException(nameof(dbService));
            if (queryPolicy == null)
                throw new ArgumentNullException(nameof(queryPolicy));

            _dbConfig = dbConfig;
            _configManager = configManager;
            _queryPolicy = queryPolicy;
            _client = documentClient;
            _dbService = dbService;
        }

        /// <inheritdoc />
        public DocumentDbConfig DbConfig
        {
            get { return _dbConfig; }
        }

        /// <inheritdoc />
        public IDocumentQueryPolicy QueryPolicy
        {
            get { return _queryPolicy; }
        }

        /// <inheritdoc />
        public IServiceDbConfigRegistry ConfigRegistry
        {
            get { return _configManager; }
        }

        /// <summary>
        /// Gets the configuration manager.
        /// </summary>
        internal ServiceDbConfigManager ConfigManager
        {
            get { return _configManager; }
        }

        /// <summary>
        /// Gets a boolean indicating if the database service has been started.
        /// </summary>
        public bool IsStarted
        {
            get { return _started; }
        }

        /// <summary>
        /// Gets the document client.
        /// </summary>
        /// <returns>The document client.</returns>
        /// <remarks>
        /// <para>This operation ensures that the database configuration is current before returning the client. This is
        /// an expensive operation if the configuration has changed by another process because the database must be
        /// updated to reflect the changes. In practice, configuration should not be changing if this class is reused
        /// for the life time of an application.
        /// </para>
        /// <para>The config update process is run synchronously.</para>
        /// </remarks>
        internal IDocumentClient GetClient()
        {
            if (!_started)
            {
                throw new NebulaServiceException("Document DB services have not been started");
            }

            _configManager.EnsureConfigCurrent(_client, _dbConfig);

            return _client;
        }

        /// <inheritdoc />
        public async Task Open()
        {
            if (_started)
            {
                return;
            }

            await _dbService.StartAsync();

            _started = true;

            // Do an initial config update run in case any stores have been registered already.
            _configManager.EnsureConfigCurrent(_client, _dbConfig);
        }
    }
}