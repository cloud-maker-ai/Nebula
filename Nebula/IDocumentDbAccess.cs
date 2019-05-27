using System.Threading.Tasks;
using Nebula.Config;

namespace Nebula
{
    /// <summary>
    /// Defines properties for accessing document database requirements.
    /// </summary>
    public interface IDocumentDbAccess
    {
        /// <summary>
        /// Gets the document config registry.
        /// </summary>
        IServiceDbConfigRegistry ConfigRegistry { get; }

        /// <summary>
        /// Gets the document database configuration.
        /// </summary>
        DocumentDbConfig DbConfig { get; }

        /// <summary>
        /// Gets the document query policy.
        /// </summary>
        IDocumentQueryPolicy QueryPolicy { get; }

        /// <summary>
        /// Initialises requirements for DB access.
        /// </summary>
        /// <returns>A task that represents the asynchronous start operation.</returns>
        /// <remarks>
        /// <para>The first request to CosmosDb has a higher latency because it has to fetch the address routing
        /// table and ensure that the configuration is current. To avoid this startup latency on the first request,
        /// the client is explicitly opened and the configuration is checked/updated.</para>
        /// </remarks>
        Task Open();
    }
}