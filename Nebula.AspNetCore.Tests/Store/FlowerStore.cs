using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Nebula.Config;
using Nebula.Versioned;

namespace Nebula.AspNetCore.Tests.Store
{
    public class FlowerStore : VersionedDocumentStore
    {
        private readonly DocumentTypeMapping<Daisy> _daisyMapping;
        private readonly DocumentTypeMapping<Garden> _gardenMapping;

        private readonly DocumentStoreConfig _config;
        private readonly IVersionedDocumentStoreClient _client;

        public FlowerStore(
            IDocumentDbAccessProvider dbAccessProvider,
            IDocumentMetadataSource metadataSource) : base(dbAccessProvider, false)
        {
            var config = new DocumentStoreConfigBuilder("Flowers");

            var daisyDocumentType = config.AddDocument("Daisy").Finish();
            var roseDocumentType = config.AddDocument("Rose").Finish();

            _daisyMapping = config.AddDocumentMapping<Daisy>(daisyDocumentType.DocumentName)
                .SetIdMapper(x => x.Id.ToString())
                .SetPartitionMapper(x => x.Id.ToString())
                .Finish();

            _gardenMapping = config.AddDocumentMapping<Garden>(roseDocumentType.DocumentName)
                .SetIdMapper(x => x.Id.ToString())
                .SetPartitionMapper(x => x.Id.ToString())
                .Finish();

            _config = config.Finish();
            _client = CreateStoreLogic(DbAccess, _config, metadataSource);
        }

        protected override DocumentStoreConfig StoreConfig
        {
            get { return _config; }
        }

        protected override IVersionedDocumentStoreClient StoreClient
        {
            get { return _client; }
        }

        /// <summary>
        /// Creates or updates a garden.
        /// </summary>
        /// <param name="garden">The garden.</param>
        /// <param name="checkVersion">The version if a consistency check is required.</param>
        /// <returns>A task representing the result of the asynchronous operation.</returns>
        public async Task UpsertGarden(Garden garden, int? checkVersion = null)
        {
            await StoreClient.UpsertDocumentAsync(
                garden,
                _gardenMapping,
                new OperationOptions
                {
                    CheckVersion = checkVersion
                });
        }

        /// <summary>
        /// Deletes a garden.
        /// </summary>
        /// <param name="id">The garden id.</param>
        /// <param name="checkVersion">The version if a consistency check is required.</param>
        /// <returns>A task representing the result of the asynchronous operation.</returns>
        public async Task DeleteGarden(Guid id, int? checkVersion = null)
        {
            await StoreClient.DeleteDocumentAsync(
                id.ToString(),
                _gardenMapping,
                new OperationOptions
                {
                    CheckVersion = checkVersion
                });
        }

        /// <summary>
        /// Gets daisies in a garden.
        /// </summary>
        /// <param name="gardenId">The garden id.</param>
        /// <returns>The daisies contained in the garden.</returns>
        public async Task<List<Daisy>> GetDaisiesInGarden(Guid gardenId)
        {
            var gardenIdParameter = new DbParameter("gardenId", gardenId.ToString());

            var result = await StoreClient.GetDocumentsAsync(
                "[x].GardenId = @gardenId",
                new[] { gardenIdParameter },
                _daisyMapping,
                null);

            return result.Loaded
                .Select(x => x.Document)
                .ToList();
        }

        /// <summary>
        ///  Gets a daisy by id.
        /// </summary>
        /// <param name="id">The daisy id.</param>
        /// <returns>The daisy if found; otherwise null.</returns>
        public async Task<Daisy> GetDaisyById(Guid id)
        {
            var result = await StoreClient.GetDocumentAsync(
                id.ToString(),
                _daisyMapping,
                null);

            return result.Document;
        }

        /// <summary>
        /// Gets a particular daisy version.
        /// </summary>
        /// <param name="id">The daisy id.</param>
        /// <param name="version">The daisy version.</param>
        /// <returns>The daisy if one exists with the id and version; otherwise null.</returns>
        public async Task<Daisy> GetDaisyById(Guid id, int version)
        {
            var result = await StoreClient.GetDocumentAsync(
                id.ToString(),
                version,
                _daisyMapping);

            return result.Document;
        }

        /// <summary>
        /// Gets all daisies in all gardens.
        /// </summary>
        /// <returns>All daisies in the world.</returns>
        public async Task<List<Daisy>> GetAllDaisies()
        {
            var result = await StoreClient.GetDocumentsAsync(
                _daisyMapping,
                null);

            return result
                .Select(x => x.Document)
                .ToList();
        }

        /// <summary>
        /// Gets daisies by ids.
        /// </summary>
        /// <param name="ids">The ids of the daisies.</param>
        /// <returns>A list of daisies.</returns>
        public async Task<List<Daisy>> GetDaisiesById(IEnumerable<Guid> ids)
        {
            var result = await StoreClient.GetDocumentsAsync(
                ids.Select(x => x.ToString()),
                _daisyMapping,
                null);

            return result.Loaded
                .Select(x => x.Document)
                .ToList();
        }

        /// <summary>
        /// Deletes a daisy.
        /// </summary>
        /// <param name="id">The daisy id.</param>
        /// <param name="checkVersion">The version if a consistency check is required.</param>
        /// <returns>A task representing the result of the asynchronous operation.</returns>
        public async Task DeleteDaisy(Guid id, int? checkVersion = null)
        {
            await StoreClient.DeleteDocumentAsync(
                id.ToString(),
                _daisyMapping,
                new OperationOptions
                {
                    CheckVersion = checkVersion
                });
        }

        /// <summary>
        /// Creates or updates a daisy.
        /// </summary>
        /// <param name="daisy">The daisy.</param>
        /// <param name="checkVersion">The version if a consistency check is required.</param>
        /// <returns>A task representing the result of the asynchronous operation.</returns>
        public async Task UpsertDaisy(Daisy daisy, int? checkVersion = null)
        {
            await StoreClient.UpsertDocumentAsync(
                daisy,
                _daisyMapping,
                new OperationOptions
                {
                    CheckVersion = checkVersion
                });
        }
    }
}