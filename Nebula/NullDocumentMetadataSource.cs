namespace Nebula
{
    /// <summary>
    /// A document metadata source that provides no data.
    /// </summary>
    internal class NullDocumentMetadataSource : IDocumentMetadataSource
    {
        /// <inheritdoc />
        public string GetActorId()
        {
            return null;
        }
    }
}