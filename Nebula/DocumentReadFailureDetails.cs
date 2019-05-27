using System;

namespace Nebula
{
    /// <summary>
    /// Document read failure details.
    /// </summary>
    public class DocumentReadFailureDetails
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentReadFailureDetails"/> class.
        /// </summary>
        /// <param name="message">The failure message.</param>
        /// <param name="detail">The failure detail message.</param>
        public DocumentReadFailureDetails(string message, string detail)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            Message = message;
            Detail = detail;
        }

        /// <summary>
        /// Gets the failure message.
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// Gets the failure detail.
        /// </summary>
        public string Detail { get; }
    }
}