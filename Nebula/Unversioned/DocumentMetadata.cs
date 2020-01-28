using System;

namespace Nebula.Unversioned
{
    /// <summary>
    /// Defines metadata associated with an un-versioned document.
    /// </summary>
    public class DocumentMetadata
    {
        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentMetadata"/> class.
        /// </summary>
        /// <param name="createdTime">The created time.</param>
        /// <param name="modifiedTime">The modified time.</param>
        public DocumentMetadata(DateTime createdTime, DateTime modifiedTime)
            : this(createdTime, modifiedTime, null)
        {
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentMetadata"/> class.
        /// </summary>
        /// <param name="createdTime">The created time.</param>
        /// <param name="modifiedTime">The modified time.</param>
        /// <param name="actorId">The id of the actor associated with the change.</param>
        public DocumentMetadata(DateTime createdTime, DateTime modifiedTime, string actorId)
        {
            if (createdTime == DateTime.MinValue)
                throw new ArgumentException("Created time is required", nameof(createdTime));
            if (modifiedTime == DateTime.MinValue)
                throw new ArgumentException("Modified time is required", nameof(modifiedTime));
            if (createdTime.Kind != DateTimeKind.Utc)
                throw new ArgumentException("UTC time is required", nameof(createdTime));
            if (modifiedTime.Kind != DateTimeKind.Utc)
                throw new ArgumentException("UTC time is required", nameof(modifiedTime));

            // actorId may be null.

            CreatedTime = createdTime;
            ModifiedTime = modifiedTime;
            ActorId = actorId;
        }

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