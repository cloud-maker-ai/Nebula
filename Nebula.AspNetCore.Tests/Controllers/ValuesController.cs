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
        public async Task<ActionResult<IEnumerable<Daisy>>> Get()
        {
            var garden = new Garden
            {
                Id = Guid.NewGuid(),
                Address = "1 Park Lane"
            };

            var daisy1 = new Daisy
            {
                Id = Guid.NewGuid(),
                GardenId = garden.Id,
                Colour = "Red"
            };

            var daisy2 = new Daisy
            {
                Id = Guid.NewGuid(),
                GardenId = garden.Id,
                Colour = "White"
            };

            await _flowerStore.UpsertGarden(garden);

            await _flowerStore.UpsertDaisy(daisy1);
            await _flowerStore.UpsertDaisy(daisy2);

            var daisies = await _flowerStore.GetDaisiesInGarden(garden.Id);

            return daisies;
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
