using System;

namespace Nebula
{
    /// <summary>
    /// An exception that is thrown when a Nebula store operation encounters a concurrency error.
    /// </summary>
    public class NebulaStoreConcurrencyException : NebulaStoreException
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="NebulaStoreConcurrencyException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public NebulaStoreConcurrencyException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="NebulaStoreConcurrencyException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception.</param>
        public NebulaStoreConcurrencyException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
