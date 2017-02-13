using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using System.IO;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;
using System.Threading.Tasks;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphViewMarvelTest2
    {
        [TestMethod]
        public void SelectMarvelQuery1()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");

            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.g().V().Has("weapon", "shield").As("character").Out("appeared").As("comicbook").Select("character").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery1b()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");

            GraphViewCommand cmd = new GraphViewCommand(connection);
            cmd.CommandText =
                "g.V().has('weapon','shield').as('character').out('appeared').as('comicbook').select('character').next()";
            cmd.OutputFormat = OutputFormat.GraphSON;
            var results = cmd.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery1c()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");

            GraphViewCommand cmd = new GraphViewCommand(connection);
            cmd.CommandText =
                "g.V().has('weapon','shield').as('character').outE('appeared').next()";
            cmd.OutputFormat = OutputFormat.GraphSON;
            var results = cmd.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery2()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);

            var results =
                graph.g()
                    .V()
                    .Has("weapon", "lasso")
                    .As("character")
                    .Out("appeared")
                    .As("comicbook")
                    .Select("comicbook")
                    .Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery2b()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('weapon', 'lasso').as('character').out('appeared').as('comicbook').select('comicbook').next()";
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.Execute();

            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery3()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results = graph.g().V().Has("name", "AVF 4").In("appeared").Values("name").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery3b()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('name', 'AVF 4').in('appeared').values('name').next()";
            var results = graph.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery4()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results = graph.g().V().Has("name", "AVF 4").In("appeared").Has("weapon", "shield").Values("name").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery4b()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('name', 'AVF 4').in('appeared').has('weapon', 'shield').values('name').next()";
            var results = graph.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        /// <summary>
        /// Print the characters and the comic-books they appeared in where the characters had a weapon that was a shield or claws.
        /// </summary>
        [TestMethod]
        public void SelectMarvelQueryNativeAPI1()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results =
                graph.g().V()
                    .As("character")
                    .Has("weapon", Predicate.within("shield", "claws"))
                    .Out("appeared")
                    .As("comicbook")
                    .Select("character");

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQueryNativeAPI2()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results =
                graph.g().V()
                    .As("CharacterNode")
                    .Values("name")
                    .As("character")
                    .Select("CharacterNode")
                    .Has("weapon", Predicate.without("shield", "claws"))
                    .Out("appeared")
                    .Values("name")
                    .As("comicbook")
                    .Select("comicbook")
                    .Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void GraphViewMarvelInsertDeleteTest()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            connection.ResetCollection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("character").Property("name", "VENUS II").Property("weapon", "shield").Next();
            graph.g().AddV("comicbook").Property("name", "AVF 4").Next();
            graph.g().V().Has("name", "VENUS II").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
            graph.g().AddV("character").Property("name", "HAWK").Property("weapon", "claws").Next();
            graph.g().V().As("v").Has("name", "HAWK").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
            graph.g().AddV("character").Property("name", "WOODGOD").Property("weapon", "lasso").Next();
            graph.g().V().As("v").Has("name", "WOODGOD").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
        }

        [TestMethod]
        public void GraphViewMarvelInsertTest2()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            connection.ResetCollection();
            GraphViewCommand cmd = new GraphViewCommand(connection);

            cmd.CommandText = "g.addV('character').property('name', 'VENUS II').property('weapon', 'shield').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('comicbook').property('name', 'AVF 4').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().has('name', 'VENUS II').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('character').property('name', 'HAWK').property('weapon', 'claws').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().as('v').has('name', 'HAWK').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('character').property('name', 'WOODGOD').property('weapon', 'lasso').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().as('v').has('name', 'WOODGOD').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
        }
        [TestMethod]
        public void GraphViewMarvelInsertTest()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "TransactionTest");
            connection.ResetCollection();
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("character" + DateTime.Now).Property("name", "VENUS II").Property("weapon", "shield").Next();
            graph.g().AddV("comicbook" + DateTime.Now).Property("name", "AVF 4").Next();
            graph.g().V().Has("name", "VENUS II").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
        }
        //[TestMethod]
        //public async void storedProcedureTest()
        //{
        //    connection connection = new connection("https://graphview.documents.azure.com:443/",
        //       "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //       "GroupMatch", "TransactionTest");
        //    var markAntiquesSproc = new StoredProcedure
        //    {
        //        Id = "ValidateDocumentAge",
        //        Body = @"
        //    function(docToCreate, antiqueYear) {
        //        var collection = getContext().getCollection();    
        //        var response = getContext().getResponse();    

        //        if(docToCreate.Year != undefined && docToCreate.Year < antiqueYear){
        //            docToCreate.antique = true;
        //        }

        //        collection.createDocument(collection.getSelfLink(), docToCreate, {}, 
        //            function(err, docCreated, options) { 
        //                if(err) throw new Error('Error while creating document: ' + err.message);                              
        //                if(options.maxCollectionSizeInMb == 0) throw 'max collection size not found'; 
        //                response.setBody(docCreated);
        //        });
        // }"
        //    };

        //    // register stored procedure
        //    StoredProcedure createdStoredProcedure = await connection.DocDBclient.CreateStoredProcedureAsync(UriFactory.CreateDocumentCollectionUri("db", "coll"), markAntiquesSproc);
        //    dynamic document = new Document() { Id = "Borges_112" };
        //    //document.Title = "Aleph";
        //    //document.Year = 1949;

        //    // execute stored procedure
        //    Document createdDocument = await connection.DocDBclient.ExecuteStoredProcedureAsync<Document>(UriFactory.CreateStoredProcedureUri("db", "coll", "sproc"), document, 1920);
        //}
        [TestMethod]
        public void storedProcedureUpdateDocTest()
        {
            // (1) create procedure
            string collectionLink = "dbs/" + "GroupMatch" + "/colls/" + "TransactionTest";

            // Each batch size is determined by maxJsonSize.
            // maxJsonSize should be so that:
            // -- it fits into one request (MAX request size is ???).
            // -- it doesn't cause the script to time out, so the batch number can be minimzed.
            const int maxJsonSize = 50000;

            // Prepare the BulkInsert stored procedure
            string jsBody = File.ReadAllText(@"..\..\..\GraphView\GraphViewExecutionRuntime\transaction\update2.js");
            StoredProcedure sproc = new StoredProcedure
            {
                Id = "UpdateEdge" + DateTime.Now.ToLongTimeString(),
                Body = jsBody,
            };

            connection connection = new connection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "TransactionTest");
            var bulkInsertCommand = new GraphViewCommand(connection);
            //Create the BulkInsert stored procedure if it doesn't exist
            //var spTask = connection.DocDBclient.CreateStoredProcedureAsync(collectionLink, sproc);

            Task<StoredProcedure> spTask = bulkInsertCommand.TryCreatedStoredProcedureAsync(collectionLink, sproc);
            spTask.Wait();
            sproc = spTask.Result;
            var sprocLink = sproc.SelfLink;
            // (2) Update source vertex
            var srcId = "69f53465-ac7d-45d4-bddb-031dcea0b0c8";
            //var edgeObject = "\"test Edge\"";
            //var json_arr_src = generateInsertEdgeObjectString(srcId, edgeObject);
            //var objs_src = new dynamic[] { Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic[]>(json_arr_src) };
            // Execute the batch
            var id = srcId;
            var jsonDocArr = new StringBuilder();
            //jsonDocArr.Append("{\"$push\": { \"_edge\":  ");
            //jsonDocArr.Append("{\"testEdge\"}");
            //jsonDocArr.Append("}");
            jsonDocArr.Append("{$inc:{\"_nextEdgeOffset\":1}}");
            var objs_src = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(jsonDocArr.ToString()) };
            var array = new dynamic[] { id, objs_src[0]};
            //var array = new dynamic[] { };
            Task<StoredProcedureResponse<JObject>> result= connection.DocDBclient.ExecuteStoredProcedureAsync<JObject>(sprocLink, array);
            result.Wait();
            //result.RunSynchronously();
            Console.WriteLine("Finish the StoredProcedure " + result.Result.Response);
            //Task<int> insertTask_src = bulkInsertCommand.BulkInsertAsync(sprocLink, array);
            //insertTask_src.Wait();
        }
        public string generateInsertEdgeObjectString(string vertexId, string edgeObject)
        {
            var jsonDocArr = new StringBuilder();
            jsonDocArr.Append("[\"" + vertexId + "\", {\"$addToSet\": { \"_edge\":  ");
            jsonDocArr.Append(edgeObject.ToString());
            jsonDocArr.Append("}}]");
            //jsonDocArr.Append(GraphViewJsonCommand.ConstructNodeJsonString(nodes[currentIndex]));

            //while (jsonDocArr.Length < maxJsonSize && ++currentIndex < nodes.Count)
            //    jsonDocArr.Append(", " + GraphViewJsonCommand.ConstructNodeJsonString(nodes[currentIndex]));

            //jsonDocArr.Append("]");
            return jsonDocArr.ToString();
        }
    }
}
