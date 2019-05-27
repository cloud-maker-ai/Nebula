using System;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace Nebula
{
    /// <summary>
    /// A builder for <see cref="AttachmentTypeMapping{TDocument,TAttachment}"/> classes.
    /// </summary>
    /// <typeparam name="TDocument">The type of document.</typeparam>
    /// <typeparam name="TAttachment">The type of attachment.</typeparam>
    public class AttachmentTypeMappingBuilder<TDocument, TAttachment>
    {
        private readonly string _attachmentName;
        private readonly DocumentTypeMapping<TDocument> _documentMapping;

        private Func<Stream, TAttachment> _readerFunc;
        private Func<TAttachment, Stream> _writerFunc;

        /// <summary>
        /// Initialises a new instance of the <see cref="AttachmentTypeMappingBuilder{TDocument,TAttachment}"/> class.
        /// </summary>
        /// <param name="attachmentName">The attachment name.</param>
        /// <param name="documentMapping">The document type mapping.</param>
        internal AttachmentTypeMappingBuilder(string attachmentName, DocumentTypeMapping<TDocument> documentMapping)
        {
            if (attachmentName == null)
                throw new ArgumentNullException(nameof(attachmentName));
            if (documentMapping == null)
                throw new ArgumentNullException(nameof(documentMapping));

            _attachmentName = attachmentName;
            _documentMapping = documentMapping;
        }

        /// <summary>
        /// Sets the attachment reader.
        /// </summary>
        /// <param name="readerFunc">The attachment reader.</param>
        /// <returns>The builder.</returns>
        /// <remarks>If no reader is set then a default JSON serialiser is used.</remarks>
        public AttachmentTypeMappingBuilder<TDocument, TAttachment> SetReader(Func<Stream, TAttachment> readerFunc)
        {
            if (readerFunc == null)
                throw new ArgumentNullException(nameof(readerFunc));

            _readerFunc = readerFunc;
            return this;
        }

        /// <summary>
        /// Sets the attachment writer.
        /// </summary>
        /// <param name="writerFunc">The attachment writer.</param>
        /// <returns>The builder.</returns>
        /// <remarks>If no writer is set then a default JSON serialiser is used.</remarks>
        public AttachmentTypeMappingBuilder<TDocument, TAttachment> SetWriter(Func<TAttachment, Stream> writerFunc)
        {
            if (writerFunc == null)
                throw new ArgumentNullException(nameof(writerFunc));

            _writerFunc = writerFunc;
            return this;
        }

        /// <summary>
        /// Builds a <see cref="AttachmentTypeMapping{TDocument,TAttachment}"/> class.
        /// </summary>
        /// <returns>The attachment type mapping.</returns>
        public AttachmentTypeMapping<TDocument, TAttachment> Finish()
        {
            if (_readerFunc == null)
            {
                _readerFunc = ReadJsonAttachment;
            }

            if (_writerFunc == null)
            {
                _writerFunc = WriteJsonAttachment;
            }

            return new AttachmentTypeMapping<TDocument, TAttachment>(_attachmentName, _documentMapping, _readerFunc, _writerFunc);
        }

        private static Stream WriteJsonAttachment(TAttachment attachment)
        {
            var stream = new MemoryStream();

            using (var writer = new StreamWriter(stream, Encoding.UTF8, 1024, true))
            {
                using (var jsonWriter = new JsonTextWriter(writer))
                {
                    var serialiser = new JsonSerializer();
                    serialiser.Serialize(jsonWriter, attachment);
                    jsonWriter.Flush();
                }
            }

            stream.Position = 0;
            return stream;
        }

        private static TAttachment ReadJsonAttachment(Stream stream)
        {
            var serializer = new JsonSerializer();

            using (var streamReader = new StreamReader(stream))
            {
                using (var jsonTextReader = new JsonTextReader(streamReader))
                {
                    return serializer.Deserialize<TAttachment>(jsonTextReader);
                }
            }
        }
    }
}