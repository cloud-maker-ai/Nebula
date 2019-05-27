using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.Azure.Documents;

namespace Nebula.Config
{
    /// <summary>
    /// A builder for <see cref="DocumentConfig"/> classes.
    /// </summary>
    public class DocumentConfigBuilder
    {
        private readonly string _name;
        private readonly Dictionary<string, AttachmentConfigBuilder> _attachments;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentConfigBuilder"/> class.
        /// </summary>
        /// <param name="name">The document name.</param>
        internal DocumentConfigBuilder(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            _name = name;

            _attachments = new Dictionary<string, AttachmentConfigBuilder>();
        }

        /// <summary>
        /// Gets the document name.
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Adds document attachment configuration.
        /// </summary>
        /// <param name="name">The attachment name.</param>
        /// <returns>The attachment configuration builder.</returns>
        public AttachmentConfigBuilder AddAttachment(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (_attachments.ContainsKey(name))
                throw new ArgumentException("Document attachment already added", nameof(name));

            var builder = new AttachmentConfigBuilder(name);

            _attachments[name] = builder;

            return builder;
        }

        /// <summary>
        /// Creates a new attachment mapping.
        /// </summary>
        /// <typeparam name="TDocument">The type of document.</typeparam>
        /// <typeparam name="TAttachment">The type of attachment.</typeparam>
        /// <param name="documentMapping">The document type mapping.</param>
        /// <param name="attachmentName">The attachment name.</param>
        /// <returns></returns>
        public AttachmentTypeMappingBuilder<TDocument, TAttachment> AddAttachmentMapping<TDocument, TAttachment>(
            DocumentTypeMapping<TDocument> documentMapping,
            string attachmentName)
        {
            if (documentMapping == null)
                throw new ArgumentNullException(nameof(documentMapping));
            if (attachmentName == null)
                throw new ArgumentNullException(nameof(attachmentName));

            if (!_attachments.ContainsKey(attachmentName))
                throw new ArgumentException("Document attachment not defined", nameof(attachmentName));

            return new AttachmentTypeMappingBuilder<TDocument, TAttachment>(attachmentName, documentMapping);
        }

        /// <summary>
        /// Builds a <see cref="DocumentConfig"/> class.
        /// </summary>
        /// <returns>The document configuration.</returns>
        public DocumentConfig Finish()
        {
            var path = new IncludedPath
            {
                Path = "/*"
            };

            path.Indexes.Add(new HashIndex(DataType.String));

            var attachments = _attachments.Values
                .Select(a => a.Finish())
                .ToImmutableList();

            return new DocumentConfig(_name, attachments, ImmutableList.Create(path), null);
        }
    }
}