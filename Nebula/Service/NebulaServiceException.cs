using System;

namespace Nebula.Service
{
    /// <summary>
    /// An exception that is thrown when a Nebula service operation encounters an error.
    /// </summary>
    public class NebulaServiceException : NebulaException
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="NebulaServiceException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public NebulaServiceException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="NebulaServiceException"/> class.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception.</param>
        public NebulaServiceException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}