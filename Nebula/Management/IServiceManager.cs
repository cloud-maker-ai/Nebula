using System.Threading.Tasks;

namespace Nebula.Management
{
    /// <summary>
    /// Defines operations for directly interacting with a document service.
    /// </summary>
    public interface IServiceManager
    {
        /// <summary>
        /// Removes all documents stored for the service.
        /// </summary>
        /// <returns>A task that represents the asynchronous purge operation.</returns>
        /// <remarks>
        /// The service's configuration records are not removed by this operation.
        /// </remarks>
        Task PurgeDocumentsAsync();
    }
}