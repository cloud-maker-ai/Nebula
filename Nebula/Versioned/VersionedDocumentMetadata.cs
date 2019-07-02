using System;

namespace Nebula.Versioned
{
    /// <summary>
    /// Defines metadata associated with a versioned document.
    /// </summary>
    public class VersionedDocumentMetadata
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentMetadata"/> class.
        /// </summary>
        /// <param name="version">The document version</param>
        /// <param name="isDeleted">A <c>boolean</c> indicating whether the document is deleted.</param>
        /// <param name="createdTime">The created time.</param>
        /// <param name="modifiedTime">The modified time.</param>
        public VersionedDocumentMetadata(int version, bool isDeleted, DateTime createdTime, DateTime modifiedTime)
            : this(version, isDeleted, createdTime, modifiedTime, null)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="VersionedDocumentMetadata"/> class.
        /// </summary>
        /// <param name="version">The document version</param>
        /// <param name="isDeleted">A <c>boolean</c> indicating whether the document is deleted.</param>
        /// <param name="createdTime">The created time.</param>
        /// <param name="modifiedTime">The modified time.</param>
        /// <param name="actorId">The id of the actor associated with the change.</param>
        public VersionedDocumentMetadata(int version, bool isDeleted, DateTime createdTime, DateTime modifiedTime, string actorId)
        {
            if (version <= 0)
                throw new ArgumentOutOfRangeException(nameof(version), "Version must be positive integer");
            if (createdTime == DateTime.MinValue)
                throw new ArgumentException("Created time is required", nameof(createdTime));
            if (modifiedTime == DateTime.MinValue)
                throw new ArgumentException("Modified time is required", nameof(modifiedTime));
            if (createdTime.Kind != DateTimeKind.Utc)
                throw new ArgumentException("UTC time is required", nameof(createdTime));
            if (modifiedTime.Kind != DateTimeKind.Utc)
                throw new ArgumentException("UTC time is required", nameof(modifiedTime));

            // actorId may be null.

            Version = version;
            IsDeleted = isDeleted;
            CreatedTime = createdTime;
            ModifiedTime = modifiedTime;
            ActorId = actorId;
        }

        /// <summary>
        /// Gets the document version.
        /// </summary>
        public int Version { get; }

        /// <summary>
        /// Gets a <c>boolean</c> indicating whether the document is deleted.
        /// </summary>
        public bool IsDeleted { get; }

        /// <summary>
        /// Gets the document created time in UTC.
        /// </summary>
        public DateTime CreatedTime { get; }

        /// <summary>
        /// Gets the document last modified time in UTC.
        /// </summary>
        public DateTime ModifiedTime { get; }
        
        /// <summary>
        /// Gets the id of the actor associated with the change.
        /// </summary>
        public string ActorId { get; }
    }
}