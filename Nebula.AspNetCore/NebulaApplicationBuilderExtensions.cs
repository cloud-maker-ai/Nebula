using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Nebula.AspNetCore
{
    /// <summary>
    /// Extension methods for <see cref="T:Microsoft.AspNetCore.Builder.IApplicationBuilder" /> to start Nebula services.
    /// </summary>
    public static class NebulaApplicationBuilderExtensions
    {
        /// <summary>
        /// Starts Nebula services.
        /// </summary>
        /// <param name="app">The <see cref="T:Microsoft.AspNetCore.Builder.IApplicationBuilder" />.</param>
        /// <returns>A reference to this instance after the operation has completed.</returns>
        public static IApplicationBuilder UseNebula(this IApplicationBuilder app)
        {
            if (app == null)
                throw new ArgumentNullException(nameof(app));

            var dbAccessProvider = app.ApplicationServices.GetService<IDocumentDbAccessProvider>();

            var dbAccess = dbAccessProvider.GetDbAccess();

            dbAccess
                .Open()
                .Wait();

            return app;
        }
    }
}