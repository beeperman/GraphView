// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Text;
// For debugging

namespace GraphView
{
    /// <summary>
    ///     Connector to a graph database. The class inherits most functions of SqlConnection,
    ///     and provides a number of GraphView-specific functions.
    /// </summary>
    public partial class connection : IDisposable
    {
        private bool _disposed;
        public DocumentCollection DocDB_Collection;
        public string DocDB_CollectionId;
        public Database DocDB_Database;
        public string DocDB_DatabaseId;
        public bool DocDB_finish;
        public string DocDB_PrimaryKey;

        public string DocDB_Url;
        public DocumentClient DocDBclient;

        internal VertexObjectCache VertexCache { get; private set; }

        public Thread TransactionCheckThread;
        public Boolean launchTransactionCheck = true;
        /// <summary>
        ///     Initializes a new connection to DocDB.
        ///     Contains four string,
        ///     Url , Key , Database's name , Collection's name
        /// </summary>
        /// <param name="docdb_EndpointUrl">The Url</param>
        /// <param name="docdb_AuthorizationKey">The Key</param>
        /// <param name="docdb_DatabaseID">Database's name</param>
        /// <param name="docdb_CollectionID">Collection's name</param>
        public connection(string docdb_EndpointUrl, string docdb_AuthorizationKey, string docdb_DatabaseID,
            string docdb_CollectionID)
        {
            DocDB_Url = docdb_EndpointUrl;
            DocDB_PrimaryKey = docdb_AuthorizationKey;
            DocDB_DatabaseId = docdb_DatabaseID;
            DocDB_CollectionId = docdb_CollectionID;
            DocDBclient = new DocumentClient(new Uri(DocDB_Url), DocDB_PrimaryKey, 
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                });

            DocDBclient.OpenAsync();

            VertexCache = VertexObjectCache.Instance;
            // set up the transaction check thread
            TransactionCheckThread = new Thread(() => {
                while(launchTransactionCheck)
                {
                    Console.WriteLine("Backend TransactionCheck Thread is running" + DateTime.Now);
                    DaemonTransactionCheck(); // do check the failed transaction insertion
                    Thread.Sleep(1000); // the param could be exposed to an pubic config parameter
                }
            });

            TransactionCheckThread.Name = "TransactionCheckThread";// Asigning name to the thread
            TransactionCheckThread.IsBackground = true;// Made the thread background
            TransactionCheckThread.Priority = ThreadPriority.Lowest;// Setting thread priority to low
            TransactionCheckThread.Start();// Start the thread
        }

        internal DbPortal CreateDatabasePortal()
        {
            return new DocumentDbPortal(this);
        }

        /// <summary>
        ///     Releases all resources used by GraphViewConnection.
        /// </summary>
        public void Dispose()
        {
            launchTransactionCheck = false; 
        }

        public void ResetCollection()
        {
            DocDB_Database =
                DocDBclient.CreateDatabaseQuery().Where(db => db.Id == DocDB_DatabaseId).AsEnumerable().FirstOrDefault();
            
            // If the database does not exist, create one
            if (DocDB_Database == null)
                CreateDatabaseAsync().Wait();

            DocDB_Collection =
                DocDBclient.CreateDocumentCollectionQuery("dbs/" + DocDB_Database.Id)
                    .Where(c => c.Id == DocDB_CollectionId)
                    .AsEnumerable()
                    .FirstOrDefault();

            // Delete the collection if it exists
            if (DocDB_Collection != null)
                DeleteCollectionAsync().Wait();

            CreateCollectionAsync().Wait();

            Console.Write("Collection " + DocDB_CollectionId + " has been reset.");
        }

        private async Task CreateDatabaseAsync()
        {
            DocDB_Database = await DocDBclient.CreateDatabaseAsync(new Database { Id = DocDB_DatabaseId })
                                    .ConfigureAwait(continueOnCapturedContext: false);
        }

        private async Task CreateCollectionAsync()
        {
            DocDB_Collection = await DocDBclient.CreateDocumentCollectionAsync("dbs/" + DocDB_Database.Id,
                                        new DocumentCollection {Id = DocDB_CollectionId},
                                        new RequestOptions {OfferType = "S3"})
                                            .ConfigureAwait(continueOnCapturedContext: false);
        }

        public async Task DeleteCollectionAsync()
        {
            await
                DocDBclient.DeleteDocumentCollectionAsync(DocDB_Collection.SelfLink)
                    .ConfigureAwait(continueOnCapturedContext: false);
        }

        public void BulkInsertNodes(List<string> nodes)
        {
            if (!nodes.Any()) return;

            string collectionLink = "dbs/" + DocDB_DatabaseId + "/colls/" + DocDB_CollectionId;

            // Each batch size is determined by maxJsonSize.
            // maxJsonSize should be so that:
            // -- it fits into one request (MAX request size is ???).
            // -- it doesn't cause the script to time out, so the batch number can be minimzed.
            const int maxJsonSize = 50000;

            // Prepare the BulkInsert stored procedure
            string jsBody = File.ReadAllText(@"..\..\BulkInsert.js");
            StoredProcedure sproc = new StoredProcedure
            {
                Id = "BulkInsert",
                Body = jsBody,
            };

            var bulkInsertCommand = new GraphViewCommand(this);
            //Create the BulkInsert stored procedure if it doesn't exist
            Task<StoredProcedure> spTask = bulkInsertCommand.TryCreatedStoredProcedureAsync(collectionLink, sproc);
            spTask.Wait();
            sproc = spTask.Result;
            var sprocLink = sproc.SelfLink;

            // If you are sure that the proc already exist on the server side, 
            // you can comment out the TryCreatedStoredProcude code above and use the URI directly instead
            //var sprocLink = "dbs/" + DocDB_DatabaseId + "/colls/" + DocDB_CollectionId + "/sprocs/" + sproc.Id;

            int currentCount = 0;
            while (currentCount < nodes.Count)
            {
                // Get the batch json string whose size won't exceed the maxJsonSize
                string json_arr = GraphViewCommand.GenerateNodesJsonString(nodes, currentCount, maxJsonSize);
                var objs = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(json_arr) };

                // Execute the batch
                Task<int> insertTask = bulkInsertCommand.BulkInsertAsync(sprocLink, objs);
                insertTask.Wait();

                // Prepare for next batch
                currentCount += insertTask.Result;
                Console.WriteLine(insertTask.Result + " nodes has already been inserted.");
            }
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

        public void DaemonTransactionCheck()
        {
            //var databaseID = "GroupMatch";
            //var collectionName = "TransactionTest";
            //connection connection = new connection("https://graphview.documents.azure.com:443/",
            //  "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //  databaseID, collectionName);

            GraphViewCommand graph = new GraphViewCommand(this);
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
                foreach (var desV in srcEdge.Value)
                {
                    if (reverseEdgeHash.ContainsKey(desV) && reverseEdgeHash[desV].Contains(srcEdge.Key))
                    {

                    }
                    else
                    {
                        if (!needRepairEdgeHash.ContainsKey(desV))
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
            string collectionLink = "dbs/" + this.DocDB_DatabaseId + "/colls/" + this.DocDB_CollectionId;

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

            //Create the BulkInsert stored procedure if it doesn't exist
            Task<StoredProcedure> spTask = graph.TryCreatedStoredProcedureAsync(collectionLink, sproc);
            spTask.Wait();
            sproc = spTask.Result;
            var sprocLink = sproc.SelfLink;
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            string collectionName = this.DocDB_CollectionId;
    
            foreach (var desID in needRepairEdgeHash)
            {
                foreach (var srcID in desID.Value)
                {
                    // Here need to use the documentDB API to get the doc
                    string querySrcSQL = "SELECT * FROM  " + collectionName + " WHERE " + collectionName + ".id = \"" + srcID + "\"";
                    string queryDesSQL = "SELECT * FROM  " + collectionName + " WHERE " + collectionName + ".id = \"" + desID.Key + "\"";
                    // replave the old des doc
                    string srcDocStr = RetrieveDocument(this, querySrcSQL);
                    JObject _srcDoc = JsonConvert.DeserializeObject<JObject>(srcDocStr);
                    Console.WriteLine("Running direct SQL query...");
                    JObject revEdgeObject = null;
                    //JObject _desDoc = desDoc.First<JObject>();
                    string desDocStr = RetrieveDocument(this, queryDesSQL);
                    JObject _desDoc = JsonConvert.DeserializeObject<JObject>(srcDocStr);

                    if (_srcDoc != null && _desDoc != null)
                    {
                        // Create the reverse edge
                        var _edge = _srcDoc["_edge"].First();
                        var iter = _edge;
                        while (iter != null)
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
                        var insertTask_des = DocDBclient.ExecuteStoredProcedureAsync<JObject>(sprocLink, array_des);
                        insertTask_des.Wait();
                        // insert the reverse edge to the des vertex doc
                    }
                }
            }
        }

    }

    internal sealed class VertexObjectCache
    {
        private static volatile VertexObjectCache instance;
        private static Dictionary<string, VertexField> cachedVertexCollection;
        private static ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        private VertexObjectCache()
        {
            cachedVertexCollection = new Dictionary<string, VertexField>();
        }

        public static VertexObjectCache Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (_lock)
                    {
                        if (instance == null)
                        {
                            instance = new VertexObjectCache();
                        }
                    }
                }

                return instance;
            }
        }

        public VertexField GetVertexField(string vertexId, string vertexJson)
        {
            _lock.EnterUpgradeableReadLock();
            try
            {
                VertexField vertexObject = null;
                if (cachedVertexCollection.TryGetValue(vertexId, out vertexObject))
                {
                    return vertexObject;
                }
                else
                {
                    _lock.EnterWriteLock();
                    try
                    {
                        JObject jsonObject = JObject.Parse(vertexJson);
                        vertexObject = FieldObject.GetVertexField(jsonObject);
                        cachedVertexCollection.Add(vertexId, vertexObject);
                    }
                    finally
                    {
                        _lock.ExitWriteLock();
                    }
                    return vertexObject;
                }
            }
            finally
            {
                _lock.ExitUpgradeableReadLock();
            }
        }
    }
}