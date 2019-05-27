using System;
using Newtonsoft.Json;

namespace Nebula.AspNetCore.Tests.Store
{
    public class Daisy
    {
        public Guid Id { get; set; }

        [JsonProperty(Required = Required.Always)]
        public string Colour { get; set; }
    }
}