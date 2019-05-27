namespace Nebula.Versioned
{
    /// <summary>
    /// Operation options to specify requirements for Nebula update and delete operations.
    /// </summary>
    public class OperationOptions
    {
        /// <summary>
        /// The version number used to enforce that the document has not been modified by another process.
        /// </summary>
        /// <remarks>
        /// <para>This option is used to enforce optimistic concurrency for store operations.</para>
        /// <para>If no check version is specified then data may override changes that have occurred by another process.</para>
        /// <para>If a check version is specified and the version does not match the value currently stored, then
        /// <see cref="NebulaStoreConcurrencyException"/> will be thrown by the operation.</para>
        /// </remarks>
        public int? CheckVersion { get; set; }
    }
}
