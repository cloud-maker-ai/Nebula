using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Nebula.Config;
using Xunit;

namespace Nebula.Tests.Config
{
    public class ServiceConfigSignatureGeneratorTests
    {
        private readonly Random _random;

        public ServiceConfigSignatureGeneratorTests()
        {
            _random = new Random(1);
        }

        [Fact]
        public void CreateSignatureForEqualConfigsWithEmptyInput()
        {
            DoSignatureTest(new DocumentStoreConfig[0]);
        }

        [Fact]
        public void CreateSignatureForEqualConfigsWithSingleInput()
        {
            DocumentStoreConfigBuilder storeABuilder = new DocumentStoreConfigBuilder("StoreA");
            storeABuilder.AddDocument("DocA");
            storeABuilder.AddDocument("DocB");
            storeABuilder.AddDocument("DocC");

            DoSignatureTest(new[] { storeABuilder.Finish() });
        }

        [Fact]
        public void CreateSignatureForEqualConfigsWithMultipleInputs()
        {
            DocumentStoreConfigBuilder builderA = new DocumentStoreConfigBuilder("StoreA");
            builderA.AddDocument("DocA");
            builderA.AddDocument("DocB");
            builderA.AddDocument("DocC");

            DocumentStoreConfigBuilder builderB = new DocumentStoreConfigBuilder("StoreB");
            builderB.AddDocument("DocA");
            builderB.AddDocument("DocB");

            DocumentStoreConfigBuilder builderC = new DocumentStoreConfigBuilder("StoreC");
            builderB.AddDocument("DocD");

            DoSignatureTest(new[] { builderA, builderB, builderC }.Select(x => x.Finish()).ToArray());
        }

        [Fact]
        public void CreateSignatureForInequalConfigs()
        {
            DocumentStoreConfigBuilder builder = new DocumentStoreConfigBuilder("StoreA");
            builder.AddDocument("DocA");

            DocumentStoreConfigBuilder builderChangedStoreName = new DocumentStoreConfigBuilder("StoreB");
            builderChangedStoreName.AddDocument("DocA");

            DocumentStoreConfigBuilder builderChangedDocName = new DocumentStoreConfigBuilder("StoreA");
            builderChangedDocName.AddDocument("DocC");

            var configA = new[] { builder.Finish() };
            var configB = new[] { builderChangedStoreName.Finish() };
            var configC= new[] { builderChangedDocName.Finish() };

            DoSignatureInequalTest(new IList<DocumentStoreConfig>[] { configA, configB, configC });
        }

        private void DoSignatureTest(IList<DocumentStoreConfig> config)
        {
            var generator = new ServiceConfigSignatureGenerator("Test");

            var expectedSig = generator.CreateSignature(config);

            for (int i = 0; i < 100; i++)
            {
                var fuzzed = FuzzConfig(config, _random);
                var sig = generator.CreateSignature(fuzzed);

                Assert.Equal(expectedSig, sig);
            }
        }

        private void DoSignatureInequalTest(IList<DocumentStoreConfig> configA, IList<DocumentStoreConfig> configB)
        {
            var generator = new ServiceConfigSignatureGenerator("Test");

            var expectedSig = generator.CreateSignature(configA);

            for (int i = 0; i < 100; i++)
            {
                var fuzzed = FuzzConfig(configB, _random);
                var sig = generator.CreateSignature(fuzzed);

                Assert.NotEqual(expectedSig, sig);
            }
        }

        private void DoSignatureInequalTest(IList<IList<DocumentStoreConfig>> items)
        {
            for (int i = 0; i < items.Count; i++)
            {
                for (int j = 0; j < items.Count; j++)
                {
                    if (i == j)
                    {
                        continue;
                    }

                    DoSignatureInequalTest(items[i], items[j]);
                }
            }
        }

        private IList<DocumentStoreConfig> FuzzConfig(IList<DocumentStoreConfig> config, Random random)
        {
            // Change store config order.
            List<DocumentStoreConfig> result = new List<DocumentStoreConfig>();

            foreach (var storeConfig in Shuffle(config, random))
            {
                // Change document config order.
                List<DocumentConfig> documents = new List<DocumentConfig>();

                foreach (var documentConfig in Shuffle(storeConfig.Documents, random))
                {
                    // Change index order.
                    documents.Add(new DocumentConfig(
                        documentConfig.DocumentName,
                        null,
                        ImmutableList.CreateRange(Shuffle(documentConfig.InclusionIndexes, random)),
                        null));
                }

                result.Add(new DocumentStoreConfig(storeConfig.StoreName, ImmutableList.CreateRange(documents)));
            }

            return result;
        }

        private static IList<TItem> Shuffle<TItem>(IList<TItem> list, Random random)
        {
            return list.OrderBy(x => random.Next()).ToList();
        }
    }
}