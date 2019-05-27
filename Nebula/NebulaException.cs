using System;

namespace Nebula
{
    /// <summary>
    /// An exception that is thrown when a Nebula operation encounters an error.
    /// </summary>
    public abstract class NebulaException : Exception
    {
        protected NebulaException(string message)
            : base(message)
        {

        }

        protected NebulaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}