using System;
using System.IO;

namespace Nebula
{
    /// <summary>
    /// Defines details and mappings required for reading and writing document attachments to storage.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    /// <typeparam name="TAttachment">The type of attachment.</typeparam>
    public class AttachmentTypeMapping<TDocument, TAttachment>
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="AttachmentTypeMapping{TDocument,TAttachment}" /> class.
        /// </summary>
        /// <param name="attachmentName">The attachment name.</param>
        /// <param name="documentMapping">The document mapping.</param>
        /// <param name="reader">The attachment reader.</param>
        /// <param name="writer">The attachment writer.</param>
        internal AttachmentTypeMapping(
            string attachmentName,
            DocumentTypeMapping<TDocument> documentMapping,
            Func<Stream, TAttachment> reader,
            Func<TAttachment, Stream> writer)
        {
            if (attachmentName == null)
                throw new ArgumentNullException(nameof(attachmentName));
            if (documentMapping == null)
                throw new ArgumentNullException(nameof(documentMapping));
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            AttachmentName = attachmentName;
            DocumentMapping = documentMapping;
            Reader = reader;
            Writer = writer;
        }

        /// <summary>
        /// Gets the attachment name.
        /// </summary>
        public string AttachmentName { get; }

        /// <summary>
        /// Gets the document mapping.
        /// </summary>
        public DocumentTypeMapping<TDocument> DocumentMapping { get; }

        /// <summary>
        /// Gets the attachment reader.
        /// </summary>
        public Func<Stream, TAttachment> Reader { get; }

        /// <summary>
        /// Gets the attachment writer.
        /// </summary>
        public Func<TAttachment, Stream> Writer { get; }
    }
}