using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.Azure.Documents;

namespace Nebula.Config
{
    /// <summary>
    /// Defines the configuration of a document.
    /// </summary>
    public class DocumentConfig
    {
        private readonly string _documentName;
        private readonly ImmutableList<IncludedPath> _inclusionIndexes;
        private readonly ImmutableList<ExcludedPath> _exclusionIndexes;
        private readonly ImmutableList<AttachmentConfig> _attachmentConfig;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentConfig"/> class.
        /// </summary>
        /// <param name="documentName">The document type name.</param>
        /// <param name="attachmentConfig">The attachment configurations.</param>
        /// <param name="inclusionIndexes">The inclusion indexes.</param>
        /// <param name="exclusionIndexes">The exclusion indexes.</param>
        internal DocumentConfig(
            string documentName,
            ImmutableList<AttachmentConfig> attachmentConfig,
            ImmutableList<IncludedPath> inclusionIndexes,
            ImmutableList<ExcludedPath> exclusionIndexes)
        {
            if (documentName == null)
                throw new ArgumentNullException(nameof(documentName));
            if (inclusionIndexes == null)
                throw new ArgumentNullException(nameof(inclusionIndexes));

            _documentName = documentName;
            _inclusionIndexes = inclusionIndexes;

            // excludedIndexes and attachmentConfig may be null.
            _exclusionIndexes = exclusionIndexes;
            _attachmentConfig = attachmentConfig ?? ImmutableList<AttachmentConfig>.Empty;
        }

        /// <summary>
        /// Gets the document type name.
        /// </summary>
        public string DocumentName
        {
            get { return _documentName; }
        }

        /// <summary>
        /// Gets the attachment configuration.
        /// </summary>
        public ImmutableList<AttachmentConfig> Attachments
        {
            get { return _attachmentConfig; }
        }

        /// <summary>
        /// Gets the inclusion indexes.
        /// </summary>
        /// <remarks>
        /// <para>An inclusion index is a mapping of a JSON property path to a rule. The rule defines what indexing rules
        /// apply to the JSON content.</para>
        /// </remarks>
        public IList<IncludedPath> InclusionIndexes
        {
            get { return _inclusionIndexes; }
        }

        /// <summary>
        /// Gets the exclusion indexes.
        /// </summary>
        /// <remarks>
        /// <para>An exlusion index is a JSON property path that defines content that should be exluded from index rules.</para>
        /// </remarks>
        public IList<ExcludedPath> ExclusionIndexes
        {
            get { return _exclusionIndexes ?? ImmutableList<ExcludedPath>.Empty; }
        }
    }
}