using System;

namespace Nebula
{
    /// <summary>
    /// An exception that is thrown when Nebula configuration operation encounters an error.
    /// </summary>
    public class NebulaConfigException : NebulaException
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="NebulaConfigException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public NebulaConfigException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="NebulaConfigException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception.</param>
        public NebulaConfigException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}