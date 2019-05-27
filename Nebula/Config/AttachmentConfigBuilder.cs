using System;

namespace Nebula.Config
{
    /// <summary>
    /// A builder for <see cref="AttachmentConfig"/> classes.
    /// </summary>
    public class AttachmentConfigBuilder
    {
        private readonly string _name;

        /// <summary>
        /// Initialises a new instance of the <see cref="AttachmentConfigBuilder"/> class.
        /// </summary>
        /// <param name="name"></param>
        internal AttachmentConfigBuilder(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _name = name;
        }

        /// <summary>
        /// Gets the attachment name.
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Builds a <see cref="AttachmentConfig"/> class.
        /// </summary>
        /// <returns>The attachment configuration.</returns>
        public AttachmentConfig Finish()
        {
            return new AttachmentConfig(_name);
        }
    }
}