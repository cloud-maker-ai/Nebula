using Nebula.Config;

namespace Nebula
{
    /// <summary>
    /// A base class for database access factories.
    /// </summary>
    public abstract class DocumentDbAccessFactory : IDocumentDbAccessFactory
    {
        /// <summary>
        /// Gets the service name.
        /// </summary>
        protected abstract string ServiceName { get; }

        /// <summary>
        /// Gets the database configuration.
        /// </summary>
        /// <returns></returns>
        protected abstract DocumentDbConfig GetConfig();

        /// <inheritdoc />
        public IDocumentDbAccess Create()
        {
            var dbConfig = GetConfig();

            return new DocumentDbAccess(dbConfig, new ServiceDbConfigManager(ServiceName));
        }
    }
}