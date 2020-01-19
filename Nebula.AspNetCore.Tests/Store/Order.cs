using System;
using System.Collections.Generic;

namespace Nebula.AspNetCore.Tests.Store
{
    public class Order
    {
        public Guid Id { get; set; }

        public IList<Guid> FlowerIds { get; set; }

        public string Address { get; set; }
    }
}