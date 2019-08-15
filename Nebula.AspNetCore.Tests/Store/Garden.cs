using System;
using Newtonsoft.Json;

namespace Nebula.AspNetCore.Tests.Store
{
    public class Garden
    {
        public Guid Id { get; set; }

        [JsonProperty(Required = Required.Always)]
        public string Address { get; set; }
    }
}