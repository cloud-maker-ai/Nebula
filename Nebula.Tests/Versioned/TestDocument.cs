using System;
using Newtonsoft.Json;

namespace Nebula.Tests.Versioned
{
    public class TestDocument
    {
        public TestDocument()
        {
            Id = Guid.NewGuid();
            Name = "v1";
        }

        [JsonConstructor]
        public TestDocument(Guid id, string name)
        {
            Id = id;
            Name = name;
        }

        public Guid Id { get; }

        public string Name { get; }
    }
}