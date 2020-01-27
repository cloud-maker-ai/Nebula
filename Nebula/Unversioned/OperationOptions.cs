namespace Nebula.Unversioned
{
    /// <summary>
    /// Operation options to specify requirements for un-versioned Nebula create or update operations.
    /// </summary>
    public class OperationOptions
    {
        /// <summary>
        /// The time-to-live to apply to the document.
        /// </summary>
        /// <remarks>
        /// <para>If no TTL is specified then the document is never automatically deleted.</para>
        /// <para>If a TTL is specified then the document is deleted when the time between
        /// the last store and the current time exceeds the TTL.</para>
        /// </remarks>
        public int? Ttl { get; set; }
    }
}