using System;

namespace Nebula.Config
{
    /// <summary>
    /// Defines the configuration of an attachment.
    /// </summary>
    public class AttachmentConfig
    {
        private readonly string _attachmentName;

        /// <summary>
        /// Initialises a new instance of the <see cref="AttachmentConfig"/> class.
        /// </summary>
        /// <param name="attachmentName">The attachment name.</param>
        internal AttachmentConfig(string attachmentName)
        {
            if (attachmentName == null)
                throw new ArgumentNullException(nameof(attachmentName));

            _attachmentName = attachmentName;
        }

        /// <summary>
        /// Gets the attachment name.
        /// </summary>
        public string AttachmentName
        {
            get { return _attachmentName; }
        }
    }
}