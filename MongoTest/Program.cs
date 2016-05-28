using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoTest
{
    class Program
    {
        private static MongoCollection<Restaurant> _coll;

        static void Main(string[] args)
        {
            MainAsync(args).Wait();
            Console.WriteLine("Press Enter");
            Console.ReadLine();
        }

        static async Task MainAsync(string[] args)
        {
            var connectionString = "mongodb://localhost:27017";
            var client = new MongoClient(connectionString);

            var db = client.GetServer().GetDatabase("test");
            //var collection = db.GetCollection<Restaurant>("restaurants");

            //Builders<Restaurant>.Filter.Where(x => x.Borough == "Queens")

            // Test first
            // Require latest driver
            /* 
            var aggregate = collection.Aggregate().Group(new BsonDocument { { "_id", "$borough" }, { "count", new BsonDocument("$sum", 1) } });
            var results = await aggregate.ToListAsync();
            */


            // Second test
            // Require latest driver
            /*
            var aggregate = collection.Aggregate()
                .Match(new BsonDocument { { "borough", "Queens" }, { "cuisine", "Brazilian" } })
                .Group(new BsonDocument { { "_id", "$address.zipcode" }, { "count", new BsonDocument("$sum", 1) } });
            var results = await aggregate.ToListAsync();

            foreach(var result in results)
            {
                Console.WriteLine(result);
            }*/

            // Require latest driver
            /* 
            var lll = await collection.Aggregate()
                .Match(x => x.Borough == "Queens" && x.Cuisine == "Brazilian")
                .Group(i => i.Address.ZipCode, g => new { Count = g.Count() }).ToListAsync();

            foreach (var result in lll)
            {
                Console.WriteLine(result);
            }*/


            // Require driver 1.8.1
            _coll = db.GetCollection<Restaurant>("restaurants");
            var match = new BsonDocument {
                                            { "$match",   new BsonDocument {{ "borough", "Queens" }, { "cuisine", "Brazilian" } } }
                                         };

            var group = new BsonDocument{
                                            {
                                                "$group", new BsonDocument{
                                                    {"_id", "$address.zipcode"},
                                                    {"count", new BsonDocument{{ "$sum", 1}}} }
                                            }
                                        };

            var pipeline = new[] { match, group, };
            var result = Aggregate<ResultType>(pipeline);

        }

        static List<T> Aggregate<T>(IEnumerable<BsonDocument> pipeline)
        {
            var result = _coll.Aggregate(pipeline);

            List<T> returnValues = new List<T>();
            returnValues.AddRange(result.ResultDocuments.Select(x => BsonSerializer.Deserialize<T>(x)));

            return returnValues;
        }
    }

    public class ResultType
    {
        [BsonElement("_id")]
        public string ZipCode { get; set; }
        [BsonElement("count")]
        public int Count { get; set; }
    }

    public class Restaurant
    {
        public ObjectId Id { get; set; }
        [BsonElement("borough")]
        public string Borough { get; set; }
        [BsonElement("cuisine")]
        public string Cuisine { get; set; }
        [BsonElement("name")]
        public string Name { get; set; }
        [BsonElement("restaurant_id")]
        public string RestaurantId { get; set; }
        [BsonElement("address")]
        public Address Address { get; set; }
        [BsonElement("grades")]
        public List<Grade> Grades { get; set; }
    }

    public class Address
    {
        [BsonElement("building")]
        public string Building { get; set; }
        [BsonElement("coord")]
        public List<double> Coord { get; set; }
        [BsonElement("street")]
        public string Street { get; set; }
        [BsonElement("zipcode")]
        public string ZipCode { get; set; }
    }

    public class Grade
    {
        [BsonElement("date")]
        public DateTime Date { get; set; }
        [BsonElement("grade")]
        public string GradeName { get; set; }
        [BsonElement("score")]
        public double? Score { get; set; }
    }
}
