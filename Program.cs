using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Driver;

namespace mongo_id_perf
{
    class OidDocument
    {
        public ObjectId Id { get; set; }
    }

    class GuidDocument
    {
        public Guid Id { get; set; }
    }

    class Program
    {
        const int numInsertions = 1_000_000;
        const int numRetrievals = 10_000;

        const string connectionString = "mongodb://localhost:27117";
        const string collectionName = "documents";

        static async Task Main(string[] args)
        {
            await RunOidTest();
            await RunGuidTest();
        }

        static async Task<IMongoCollection<TDocument>> SetupCollection<TDocument>()
        {
            var dbName = $"id-perf-{Guid.NewGuid()}";
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase(dbName);
            await db.CreateCollectionAsync(collectionName);
            var collection = db.GetCollection<TDocument>(collectionName);
            return collection;
        }

        static async Task RunOidTest()
        {
            var collection = await SetupCollection<OidDocument>();

            var documents = Enumerable.Range(0, numInsertions)
                .Select(_ => new OidDocument())
                .ToArray();
            await collection.InsertManyAsync(documents);
            var documentIds = documents.Select(x => x.Id).ToArray();

            Console.WriteLine($"Inserted {documentIds.Length} documents");

            var rng = new Random();
            var idsToRetrieve = Enumerable.Range(0, numRetrievals)
                .Select(_ => documentIds[rng.Next(0, documentIds.Length)])
                .ToArray();

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            foreach (var id in idsToRetrieve)
            {
                await collection.Find(x => x.Id == id).FirstOrDefaultAsync();
            }

            stopwatch.Stop();
            var timeSpent = stopwatch.Elapsed;

            Console.WriteLine($"Retrievel of {numRetrievals} OID documents: {timeSpent}");
        }

        static async Task RunGuidTest()
        {
            var collection = await SetupCollection<GuidDocument>();

            var documents = Enumerable.Range(0, numInsertions)
                .Select(_ => new GuidDocument())
                .ToArray();
            await collection.InsertManyAsync(documents);
            var documentIds = documents.Select(x => x.Id).ToArray();

            Console.WriteLine($"Inserted {documentIds.Length} documents");

            var rng = new Random();
            var idsToRetrieve = Enumerable.Range(0, numRetrievals)
                .Select(_ => documentIds[rng.Next(0, documentIds.Length)])
                .ToArray();

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            foreach (var id in idsToRetrieve)
            {
                await collection.Find(x => x.Id == id).FirstOrDefaultAsync();
            }

            stopwatch.Stop();
            var timeSpent = stopwatch.Elapsed;

            Console.WriteLine($"Retrievel of {numRetrievals} GUID documents: {timeSpent}");
        }
    }
}
