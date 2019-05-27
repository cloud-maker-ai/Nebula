namespace Nebula.Versioned
{
    /// <summary>
    /// Defines options supported for versioned document read operations.
    /// </summary>
    public class VersionedDocumentReadOptions
    {
        /// <summary>
        /// Gets or sets whether deleted documents should be included in the result.
        /// </summary>
        /// <remarks>
        /// <para>Deleted documents are excluded by default.</para>
        /// </remarks>
        public bool IncludeDeleted { get; set; }
    }
}