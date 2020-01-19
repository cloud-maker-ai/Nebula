using System.Collections.Generic;
using System.Threading.Tasks;
using Nebula.Config;

namespace Nebula.Service
{
    /// <summary>
    /// Defines a document database service.
    /// </summary>
    public interface IDocumentDbService
    {
        /// <summary>
        /// Starts the service.
        /// </summary>
        /// <param name="storeConfigSources">The store config sources.</param>
        /// <returns>A task that represents the asynchronous start operation.</returns>
        Task StartAsync(IEnumerable<IDocumentStoreConfigSource> storeConfigSources);
    }
}