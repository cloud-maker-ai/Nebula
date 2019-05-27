namespace Nebula
{
    /// <summary>
    /// An enumeration of possible document read result types.
    /// </summary>
    public enum DocumentReadResultType
    {
        /// <summary>
        /// The document was read successfully.
        /// </summary>
        Loaded = 0,

        /// <summary>
        /// The document exists but could not be read.
        /// </summary>
        Failed = 1
    }
}