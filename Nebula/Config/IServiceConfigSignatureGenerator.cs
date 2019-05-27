using System.Collections.Generic;

namespace Nebula.Config
{
    /// <summary>
    /// Defines methods for creating service configuration signatures.
    /// </summary>
    internal interface IServiceConfigSignatureGenerator
    {
        /// <summary>
        /// Creates a service configuration signature.
        /// </summary>
        /// <param name="config">The store configurations.</param>
        /// <returns>The service configuration signature.</returns>
        /// <remarks>
        /// <para>The generated signature will be equal for two sets of configuration if they only differ by order. If the
        /// configurations differ not by ordering then the signature will be different.</para>
        /// </remarks>
        string CreateSignature(IList<DocumentStoreConfig> config);
    }
}