using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Nebula.AspNetCore.Tests.Store;

namespace Nebula.AspNetCore.Tests.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ValuesController : ControllerBase
    {
        private readonly FlowerStore _flowerStore;

        public ValuesController(FlowerStore flowerStore)
        {
            _flowerStore = flowerStore;
        }

        // GET api/values
        [HttpGet]
        public async Task<ActionResult<IEnumerable<string>>> Get()
        {
            var daisy = new Daisy
            {
                Id = Guid.NewGuid(),
                Colour = "Red"
            };

            await _flowerStore.Upsert(daisy);

            var result = await _flowerStore.GetById(daisy.Id);

            return new[] { result.Colour };
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public ActionResult<string> Get(int id)
        {
            return "value";
        }

        // POST api/values
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
