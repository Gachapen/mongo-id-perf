using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using MongoDB.Bson;
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
        const int numInsertions = 1_000;
        static readonly int numRetrievals = numInsertions;

        const string dbName = "id-perf";
        const string collectionName = "documents";

        static async Task Main(string[] args)
        {
            var targets = new (string, string)[] {
                ("mongodb local", "mongodb://localhost:27117"),
                ("cosmosdb az", "REDACTED"),
                ("cosmosdb az geo", "REDACTED"),
            };

            var results = new List<(string, List<double>)>(targets.Length);

            foreach (var (targetName, targetConnectionString) in targets) {
                var oidResults = await RunOidTest(targetName, targetConnectionString);
                var guidResults = await RunGuidTest(targetName, targetConnectionString);

                results.Add(($"{targetName} OID", oidResults));
                results.Add(($"{targetName} GUID", guidResults));
            }

            using var writer = new StreamWriter("./results.csv");
            using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            foreach (var (name, values) in results) {
                csv.WriteField(name);
                foreach (var time in values) {
                    csv.WriteField(time);
                }
                csv.NextRecord();
            }
            csv.Flush();
        }

        static async Task<IMongoCollection<TDocument>> SetupCollection<TDocument>(string targetConnectionString)
        {
            var client = new MongoClient(targetConnectionString);
            var db = client.GetDatabase(dbName);
            var collection = db.GetCollection<TDocument>(collectionName);
            await collection.DeleteManyAsync(x => true);
            return collection;
        }

        static async Task<List<double>> RunOidTest(string targetName, string targetConnectionString)
        {
            Console.WriteLine($"Running OID test against {targetName}");

            var collection = await SetupCollection<OidDocument>(targetConnectionString);

            Console.WriteLine("Inserting...");

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

            var results = new List<double>(idsToRetrieve.Length);

            var stopwatch = new Stopwatch();
            var nanosPerTick = (1000L*1000L*1000L) / Stopwatch.Frequency;
            Console.WriteLine($"High resolution swopwatch: {Stopwatch.IsHighResolution} ({nanosPerTick})");

            foreach (var id in idsToRetrieve)
            {
                stopwatch.Restart();
                await collection.Find(x => x.Id == id).FirstOrDefaultAsync();
                stopwatch.Stop();
                results.Add((double)stopwatch.ElapsedTicks * (double)nanosPerTick);
            }

            return results;
        }

        static async Task<List<double>> RunGuidTest(string targetName, string targetConnectionString)
        {
            Console.WriteLine($"Running GUID test against {targetName}");

            var collection = await SetupCollection<GuidDocument>(targetConnectionString);

            Console.WriteLine("Inserting...");

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

            var results = new List<double>(idsToRetrieve.Length);

            var stopwatch = new Stopwatch();
            var nanosPerTick = (1000L*1000L*1000L) / Stopwatch.Frequency;
            Console.WriteLine($"High resolution swopwatch: {Stopwatch.IsHighResolution} ({nanosPerTick})");

            foreach (var id in idsToRetrieve)
            {
                stopwatch.Restart();
                await collection.Find(x => x.Id == id).FirstOrDefaultAsync();
                stopwatch.Stop();
                results.Add((double)stopwatch.ElapsedTicks * (double)nanosPerTick);
            }

            return results;
        }
    }
}
