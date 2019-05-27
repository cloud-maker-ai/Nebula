using System;

namespace Nebula
{
    /// <summary>
    /// An exception that is thrown when a Nebula store operation encounters an error.
    /// </summary>
    public class NebulaStoreException : NebulaException
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="NebulaStoreException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public NebulaStoreException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="NebulaStoreException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception.</param>
        public NebulaStoreException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}