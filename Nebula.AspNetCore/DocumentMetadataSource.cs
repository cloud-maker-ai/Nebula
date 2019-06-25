using System;
using System.Linq;
using System.Security.Claims;
using Microsoft.AspNetCore.Http;

namespace Nebula.AspNetCore
{
    /// <summary>
    /// A standard ASP.NET Core document metadata source.
    /// </summary>
    public class DocumentMetadataSource : IDocumentMetadataSource
    {
        private readonly IHttpContextAccessor _httpContextAccessor;

        /// <summary>
        /// Initialises a new instance of the <see cref="DocumentMetadataSource"/> class.
        /// </summary>
        /// <param name="httpContextAccessor">The http context accessor.</param>
        public DocumentMetadataSource(IHttpContextAccessor httpContextAccessor)
        {
            if (httpContextAccessor == null)
                throw new ArgumentNullException(nameof(httpContextAccessor));

            _httpContextAccessor = httpContextAccessor;
        }

        /// <inheritdoc />
        public string GetActorId()
        {
            var claims = _httpContextAccessor.HttpContext.User.Claims.ToArray();

            string actorId = null;

            var subClaim =
                claims.SingleOrDefault(c => c.Type == ClaimTypes.NameIdentifier)
                ?? claims.SingleOrDefault(c => c.Type == "sub");

            if (subClaim != null)
            {
                actorId = subClaim.Value;
            }

            return actorId;
        }
    }
}