using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Nebula.Config;

namespace Nebula.AspNetCore
{
    /// <summary>
    /// Extension methods for setting up Nebula services in an <see cref="T:Microsoft.Extensions.DependencyInjection.IServiceCollection" />.
    /// </summary>
    public static class NebulaServiceCollectionExtensions
    {
        /// <summary>
        /// Adds Nebula services to the specified <see cref="T:Microsoft.Extensions.DependencyInjection.IServiceCollection" />.
        /// </summary>
        /// <param name="services">The <see cref="T:Microsoft.Extensions.DependencyInjection.IServiceCollection" /> to add services to.</param>
        /// <param name="serviceName">The service name.</param>
        /// <param name="config">The document config.</param>
        /// <returns>The service collection.</returns>
        public static IServiceCollection AddNebula(this IServiceCollection services, string serviceName, DocumentDbConfig config)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));
            if (serviceName == null)
                throw new ArgumentNullException(nameof(serviceName));
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            services.TryAdd(ServiceDescriptor.Singleton<IDocumentDbAccessFactory, StandardDbAccessFactory>(
                provider => new StandardDbAccessFactory(serviceName, config)));

            services.TryAddSingleton<IDocumentDbAccessProvider, DocumentDbAccessProvider>();

            services.AddHttpContextAccessor();

            services.TryAddScoped<IDocumentMetadataSource, DocumentMetadataSource>();

            return services;
        }

        /// <summary>
        /// Adds Nebula services to the specified <see cref="T:Microsoft.Extensions.DependencyInjection.IServiceCollection" />.
        /// </summary>
        /// <param name="services">The <see cref="T:Microsoft.Extensions.DependencyInjection.IServiceCollection" /> to add services to.</param>
        /// <param name="serviceName">The service name.</param>
        /// <param name="config">The config.</param>
        /// <returns>The service collection.</returns>
        /// <remarks>
        /// <para>The config is bound from the 'Nebula' configuration path.</para>
        /// </remarks>
        public static IServiceCollection AddNebula(this IServiceCollection services, string serviceName, IConfiguration config)
        {
            return services.AddNebula(serviceName, config, "Nebula");
        }

        /// <summary>
        /// Adds Nebula services to the specified <see cref="T:Microsoft.Extensions.DependencyInjection.IServiceCollection" />.
        /// </summary>
        /// <param name="services">The <see cref="T:Microsoft.Extensions.DependencyInjection.IServiceCollection" /> to add services to.</param>
        /// <param name="serviceName">The service name.</param>
        /// <param name="config">The config.</param>
        /// <param name="configPath">The Nebula config path.</param>
        /// <returns>The service collection.</returns>
        /// <remarks>
        /// <para>The config is bound from the path specified by <paramref name="configPath"/>.</para>
        /// </remarks>
        public static IServiceCollection AddNebula(this IServiceCollection services, string serviceName, IConfiguration config, string configPath)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));
            if (serviceName == null)
                throw new ArgumentNullException(nameof(serviceName));
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (configPath == null)
                throw new ArgumentNullException(nameof(configPath));

            services.AddOptions();
            services.Configure<NebulaConfig>(config.GetSection(configPath));

            services.TryAdd(ServiceDescriptor.Singleton<IDocumentDbAccessFactory, ConfigDbAccessFactory>(
                provider => new ConfigDbAccessFactory(serviceName, provider.GetRequiredService<IOptions<NebulaConfig>>())));

            services.TryAddSingleton<IDocumentDbAccessProvider, DocumentDbAccessProvider>();

            services.AddHttpContextAccessor();

            services.TryAddScoped<IDocumentMetadataSource, DocumentMetadataSource>();
            return services;
        }

        /// <summary>
        /// Adds a Nebula store to the specified <see cref="T:Microsoft.Extensions.DependencyInjection.IServiceCollection" />.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <returns>The service collection.</returns>
        public static IServiceCollection AddNebulaStore<TStore>(this IServiceCollection services)
            where TStore : class, IDocumentStoreConfigSource
        {
            services.AddTransient<IDocumentStoreConfigSource, TStore>();

            return services;
        }
    }
}