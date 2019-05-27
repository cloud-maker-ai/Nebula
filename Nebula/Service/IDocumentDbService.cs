using System.Threading.Tasks;

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
        /// <returns>A task that represents the asynchronous start operation.</returns>
        Task StartAsync();
    }
}