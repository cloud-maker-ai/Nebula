namespace Nebula
{
    /// <summary>
    /// Defines methods for retrieving document metadata.
    /// </summary>
    public interface IDocumentMetadataSource
    {
        /// <summary>
        /// Gets the current actor id.
        /// </summary>
        /// <returns>The actor id.</returns>
        string GetActorId();
    }
}