using System;
using System.Threading.Tasks;
using Nebula.Config;
using Nebula.Versioned;

namespace Nebula.AspNetCore.Tests.Store
{
    public class OrderStore : VersionedDocumentStore
    {
        private readonly DocumentTypeMapping<Order> _orderMapping;

        private readonly DocumentStoreConfig _config;
        private readonly IVersionedDocumentStoreClient _client;

        public OrderStore(
            IDocumentDbAccessProvider dbAccessProvider,
            IDocumentMetadataSource metadataSource) : base(dbAccessProvider)
        {
            var config = new DocumentStoreConfigBuilder("Orders");

            var orderDocumentType = config.AddDocument("Order").Finish();

            _orderMapping = config.AddDocumentMapping<Order>(orderDocumentType.DocumentName)
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
        /// Stores an order.
        /// </summary>
        /// <param name="order">The order.</param>
        /// <returns>A task representing the result of the asynchronous operation.</returns>
        public async Task StoreOrder(Order order)
        {
            await StoreClient.UpsertDocumentAsync(
                order,
                _orderMapping,
                null);
        }

        /// <summary>
        /// Gets an order by id.
        /// </summary>
        /// <param name="orderId">The order id.</param>
        /// <returns>A task representing the result of the asynchronous operation.</returns>
        public async Task<Order> GetOrderById(Guid orderId)
        {
            var orderResult = await StoreClient.GetDocumentAsync(
                orderId.ToString(),
                _orderMapping,
                null);

            return orderResult?.Document;
        }
    }
}