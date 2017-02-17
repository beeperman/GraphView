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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

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
            //connection.ResetCollection();
            connection.launchTransactionCheck = true;
            GraphViewCommand graph = new GraphViewCommand(connection);
            var t = DateTime.Now;
            graph.g().AddV("character" + t).Property("name", "VENUS II").Property("weapon", "shield").Next();
            graph.g().AddV("comicbook" + t).Property("name", "AVF 4").Next();
            //graph.g().AddV("comicbook2" + t + 1).Property("name", "AVF 4").Next();
            graph.g().V().Has("name", "VENUS II").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
            graph.g().V().Has("name", "VENUS II").AddE("appeared2").To(graph.g().V().Has("name", "AVF 4")).Next();
            graph.g().V().Has("name", "VENUS II").AddE("appeared3").To(graph.g().V().Has("name", "AVF 4")).Next();
        }
        [TestMethod]
        public void ConnectionCheckTransactionThreadTest()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", "TransactionTest");
            //connection.ResetCollection();
            connection.launchTransactionCheck = true;
            while(true)
            {
                //Console.WriteLine();
                Thread.Sleep(3000);
            }
        }

        [TestMethod]
        public void ResetTheCollection()
        {
            connection connection = new connection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", "TransactionTest");
            connection.ResetCollection();
        }
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
            string jsBody = File.ReadAllText(@"..\..\..\GraphView\GraphViewExecutionRuntime\transaction\update.js");
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
            // Execute the batch
            var id = srcId;
            var jsonDocArr = new StringBuilder();
            jsonDocArr.Append("{$inc:{\"_nextEdgeOffset\":1}}");
            var objs_src = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(jsonDocArr.ToString()) };
            var array = new dynamic[] { id, objs_src[0]};
            Task<StoredProcedureResponse<JObject>> result= connection.DocDBclient.ExecuteStoredProcedureAsync<JObject>(sprocLink, array);
            result.Wait();
            //result.RunSynchronously();
            Console.WriteLine("Finish the StoredProcedure " + result.Result.Response);
        }
        public string generateInsertEdgeObjectString(string vertexId, string edgeObject)
        {
            var jsonDocArr = new StringBuilder();
            jsonDocArr.Append("[\"" + vertexId + "\", {\"$addToSet\": { \"_edge\":  ");
            jsonDocArr.Append(edgeObject.ToString());
            jsonDocArr.Append("}}]");
            return jsonDocArr.ToString();
        }
        [TestMethod]
        public void DaemonCheckTheadTest()
        {
            Thread thread = new Thread(() => {
                for (;;)
                {
                    Console.WriteLine("Backend is running" + DateTime.Now);
                    Thread.Sleep(100);
                }
            });

            thread.Name = "My new thread";// Asigning name to the thread
            thread.IsBackground = false;// Made the thread forground
            thread.Priority = ThreadPriority.AboveNormal;// Setting thread priority
            thread.Start();// Start D
            for (int i = 0; i < 1000000000; i ++){ };
        }
        [TestMethod]
        public void DocumentDBQueryAndClearReverseEdgeTest()
        {
            try
            {
                var collectionName = "TransactionTest";
                var queryDocID = "5e07961e-3384-4690-94ff-326d3f72e177";
                connection connection = new connection("https://graphview.documents.azure.com:443/",
                  "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                  "GroupMatch", collectionName);
                FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
                //Fetch the Document to be updated
                foreach (var doc in connection.DocDBclient.CreateDocumentQuery<Document>(UriFactory.CreateDocumentCollectionUri("GroupMatch", collectionName), queryOptions))
                {
                    doc.SetPropertyValue("_reverse_edge", JsonConvert.DeserializeObject("[]"));
                    doc.SetPropertyValue("_nextReverseEdgeOffset", JsonConvert.DeserializeObject("0"));
                    connection.DocDBclient.ReplaceDocumentAsync(doc).Wait();
                }
            }   catch(Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            //Update some properties on the found resource
            //Now persist these changes to the database by replacing the original resource
            Console.WriteLine("Press any key to continue ...");
        }
        [TestMethod] 
        public void DocumentDBQueryAndUpdateTest()
        {
            var collectionName = "MarvelTest";
            var queryDocID = "5e07961e-3384-4690-94ff-326d3f72e177";
            connection connection = new connection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", collectionName);
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            //Fetch the Document to be updated
            Document doc = connection.DocDBclient.CreateDocumentQuery<Document>(UriFactory.CreateDocumentCollectionUri("GroupMatch", collectionName))
                                        .Where(r => r.Id == queryDocID)
                                        .AsEnumerable()
                                        .SingleOrDefault();
            
            //Update some properties on the found resource
            doc.SetPropertyValue("name", "AVF 4 + 1");
            //Now persist these changes to the database by replacing the original resource
            Task<ResourceResponse<Document>> updated = connection.DocDBclient.ReplaceDocumentAsync(doc);
            updated.Wait();
            Console.WriteLine("Press any key to continue ...");
        }
        public string RetrieveDocument(connection Connection, string script)
        {
            //var script = string.Format("SELECT * FROM Node WHERE Node.id = '{0}'", id);
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            List<dynamic> result = Connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(Connection.DocDB_DatabaseId, Connection.DocDB_CollectionId),
                script, queryOptions).ToList();

            if (result.Count == 0) return null;

            return ((JObject)result[0]).ToString();
        }
        [TestMethod]
        public void DaemonTransactionCheckTest()
        {
            var databaseID = "GroupMatch";
            var collectionName = "TransactionTest";
            connection connection = new connection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              databaseID, collectionName);

            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.g().V().Next();
            
            Dictionary<string, HashSet<string>> edgeHash = new Dictionary<string, HashSet<string>>();
            Dictionary<string, HashSet<string>> reverseEdgeHash = new Dictionary<string, HashSet<string>>();

            // (1) iterate all the vertex, remember the edge
            foreach (var result in results)
            {
                // get the in edge
                var doc = JsonConvert.DeserializeObject<JObject>(result);
                var srcID = doc["id"].ToString();
                var outE = doc["outE"];
                if (doc["outE"] != null && outE.First != null)
                {
                    var iterOut = outE.First.First.First;
                    while (iterOut != null)
                    {
                        var inVID = iterOut["inV"].ToString();
                        if (!edgeHash.ContainsKey(srcID))
                        {
                            edgeHash.Add(srcID, new HashSet<string>());
                        }
                        edgeHash[srcID].Add(inVID);
                        iterOut = iterOut.Next;
                    }
                }
                // get the out edge
                var inE = doc["inE"];
                if (doc["inE"] != null && inE.First != null)
                {
                    var iterIn = inE.First.First.First;
                    while (iterIn != null)
                    {
                        var outVID = iterIn["outV"].ToString();
                        if (!reverseEdgeHash.ContainsKey(srcID))
                        {
                            reverseEdgeHash.Add(srcID, new HashSet<string>());
                        }
                        reverseEdgeHash[srcID].Add(outVID);
                        iterIn = iterIn.Next;
                    }
                }
                Console.WriteLine(doc);
            }
            Console.WriteLine("Finish the edge parse");
            // (2) check the edge
            Dictionary<string, HashSet<string>> needRepairEdgeHash = new Dictionary<string, HashSet<string>>();
            foreach (var srcEdge in edgeHash)
            {
                foreach(var desV in srcEdge.Value)
                {
                    if(reverseEdgeHash.ContainsKey(desV) && reverseEdgeHash[desV].Contains(srcEdge.Key))
                    {

                    } else
                    {
                        if(!needRepairEdgeHash.ContainsKey(desV))
                        {
                            needRepairEdgeHash.Add(desV, new HashSet<string>());
                        }
                        needRepairEdgeHash[desV].Add(srcEdge.Key);
                    }
                }
            }
            Console.WriteLine("Finish check the edge");
            // (3) query the doc and updat the edge
            // create stored procedure
            // (1) create procedure
            string collectionLink = "dbs/" + databaseID + "/colls/" + collectionName;

            // Each batch size is determined by maxJsonSize.
            // maxJsonSize should be so that:
            // -- it fits into one request (MAX request size is ???).
            // -- it doesn't cause the script to time out, so the batch number can be minimzed.
            const int maxJsonSize = 50000;

            // Prepare the BulkInsert stored procedure
            string jsBody = File.ReadAllText(@"..\..\..\GraphView\GraphViewExecutionRuntime\transaction\update.js");
            StoredProcedure sproc = new StoredProcedure
            {
                Id = "UpdateEdge",
                Body = jsBody,
            };

            var bulkInsertCommand = new GraphViewCommand(connection);
            //Create the BulkInsert stored procedure if it doesn't exist
            Task<StoredProcedure> spTask = bulkInsertCommand.TryCreatedStoredProcedureAsync(collectionLink, sproc);
            spTask.Wait();
            sproc = spTask.Result;
            var sprocLink = sproc.SelfLink;
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            
            foreach (var desID in needRepairEdgeHash)
            {
                foreach(var srcID in desID.Value)
                {
                    // Here need to use the documentDB API to get the doc
                    string querySrcSQL = "SELECT * FROM  " + collectionName + " WHERE " + collectionName + ".id = \"" + srcID + "\"";
                    string queryDesSQL = "SELECT * FROM  " + collectionName + " WHERE " + collectionName + ".id = \"" + desID.Key + "\"";
                    // replave the old des doc
                    string srcDocStr = RetrieveDocument(connection, querySrcSQL);
                    JObject _srcDoc = JsonConvert.DeserializeObject<JObject>(srcDocStr);
                    Console.WriteLine("Running direct SQL query...");
                    JObject revEdgeObject = null;
                    //JObject _desDoc = desDoc.First<JObject>();
                    string desDocStr = RetrieveDocument(connection, queryDesSQL);
                    JObject _desDoc = JsonConvert.DeserializeObject<JObject>(srcDocStr);

                    if(_srcDoc != null && _desDoc != null)
                    {
                        // Create the reverse edge
                        var _edge = _srcDoc["_edge"].First();
                        var iter = _edge;
                        while(iter != null)
                        {
                            if (iter["_sink"].ToString() == desID.Key)
                            {
                                break;
                            }
                            else
                            {
                                iter = iter.Next;
                            }
                        }
                        // update the edge properties
                        revEdgeObject = iter.Value<JObject>();
                        revEdgeObject["_ID"] = 0;
                        revEdgeObject["_reverse_ID"] = 0;
                        revEdgeObject["_sink"] = srcID;
                        revEdgeObject["_sinkLabel"] = _srcDoc["label"];

                        // Update the des doc edge property to fix the edge lose
                        var id_des = desID.Key;
                        var jsonDocArr_des = new StringBuilder();
                        jsonDocArr_des.Append("{\"$addToSet\": { \"_reverse_edge\":  ");
                        jsonDocArr_des.Append(revEdgeObject.ToString());
                        jsonDocArr_des.Append("}}");
                        var objs_des = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(jsonDocArr_des.ToString()) };

                        var incRevOffset = new StringBuilder();
                        incRevOffset.Append("{$inc:{\"_nextReverseEdgeOffset\":1}}");
                        var incOffsetRevDynamic = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(incRevOffset.ToString()) };

                        var array_des = new dynamic[] { id_des, incOffsetRevDynamic[0], objs_des[0] };
                        // Execute the batch
                        var insertTask_des = connection.DocDBclient.ExecuteStoredProcedureAsync<JObject>(sprocLink, array_des);
                        insertTask_des.Wait();
                        // insert the reverse edge to the des vertex doc
                    }
                }
            }
        }
    }
}
