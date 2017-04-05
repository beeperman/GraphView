using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static GraphView.GraphViewKeywords;


namespace GraphView
{
    internal abstract class BulkOperation
    {
        public JObject OperationJObject { get; } = new JObject();

        public Dictionary<string, string> KnownEtags { get; } = new Dictionary<string, string>();


        protected BulkOperation(string op)
        {
            this.OperationJObject["op"] = op;
        }

        public abstract void Callback(BulkResponse response, JObject content);
    }


    internal class BulkOperationAddVertex : BulkOperation
    {
        /* Parameter schema: 
            {
                "op": "AddVertex",
                "vertexObject": { ... }
            }
           Response content:
            { }
        */

        private readonly JObject _vertexObject;

        public BulkOperationAddVertex(JObject vertexObject)
            : base("AddVertex")
        {
            this._vertexObject = vertexObject;

            // OperationJObject
            this.OperationJObject["vertexObject"] = vertexObject;

            // KnownEtags:
            // Does need add anything
        }

        public override void Callback(BulkResponse response, JObject content)
        {
            this._vertexObject[KW_DOC_ETAG] = response.Etags[(string)this._vertexObject[KW_DOC_ID]];
            Debug.Assert(this._vertexObject[KW_DOC_ETAG] != null);
        }
    }
    
    internal class BulkOperationAddEdge : BulkOperation
    {
        public string FirstSpillEdgeDocId { get; private set; }
        public string NewEdgeDocId { get; private set; }


        private readonly GraphViewConnection _connection;
        private readonly JObject _edgeObject;
        private readonly string _vertexId;
        private readonly JObject _vertexObject;
        private readonly bool _isReverse;


        public BulkOperationAddEdge(
            GraphViewConnection connection,
            JObject srcVertexObject, JObject sinkVertexObject,
            bool isReverse, int? spillThreshold,
            JObject edgeObject)
            : base("AddEdge")
        {
            /* Parameter schema: 
                {
                    "op": "AddEdge",
                    "srcV": ...,
                    "sinkV": ...,
                    "isReverse", true/false
                    "spillThreshold", null/0/>0     // Can be null
                    "edgeObject", {
                        "id": ...,
                        "label": ...,
                        "_srcV"/"_sinkV": "...",
                        "_srcVLabel"/"_sinkVLabel": "...",
                        ... (Other properties)
                    }
                }
               Response content:
                {
                    "firstSpillEdgeDocId": "..."    // Not null when spilling the _edge/_reverse_edge the first time
                    "newEdgeDocId": "..."           // Which document is this edge added to? (Can be null)
                }
            */
            string srcVId = (string)srcVertexObject[KW_DOC_ID];
            string sinkVId = (string)sinkVertexObject[KW_DOC_ID];

            this._isReverse = isReverse;
            this._vertexId = isReverse ? sinkVId : srcVId;
            this._vertexObject = isReverse ? sinkVertexObject : srcVertexObject;
            this._connection = connection;
            this._edgeObject = edgeObject;

#if DEBUG
            string srcVLabel = (string)srcVertexObject[KW_VERTEX_LABEL];
            string sinkVLabel = (string)sinkVertexObject[KW_VERTEX_LABEL];
            if (isReverse)
            {
                Debug.Assert((string)edgeObject[KW_EDGE_SRCV] == srcVId);
                Debug.Assert((string)edgeObject[KW_EDGE_SRCV_LABEL] == srcVLabel);
            }
            else
            {
                Debug.Assert((string)edgeObject[KW_EDGE_SINKV] == sinkVId);
                Debug.Assert((string)edgeObject[KW_EDGE_SINKV_LABEL] == sinkVLabel);
            }
            Debug.Assert(!string.IsNullOrEmpty((string)edgeObject[KW_DOC_ID]));
            Debug.Assert(edgeObject[KW_EDGE_LABEL] != null);
#endif

            JProperty vertexIsSpilled = this._vertexObject.Property(isReverse ? KW_VERTEX_REVEDGE_SPILLED : KW_VERTEX_EDGE_SPILLED);
            JArray edgeContainer = (JArray)this._vertexObject[isReverse ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE];

            // Prepare etag dictionary
            this.KnownEtags[this._vertexId] = connection.VertexCache.GetCurrentEtag(this._vertexId);
            if ((bool)vertexIsSpilled.Value)
            {
                string latestEdgeDocId = (string)edgeContainer[0][KW_DOC_ID];

                // etag of latest edge document may not exist: since this document might have not been accessed before
                string etag = connection.VertexCache.TryGetCurrentEtag(latestEdgeDocId);
                this.KnownEtags[latestEdgeDocId] = etag;  // Can be null
            }

            this.OperationJObject["srcV"] = srcVId;
            this.OperationJObject["sinkV"] = sinkVId;
            this.OperationJObject["isReverse"] = isReverse;
            this.OperationJObject["spillThreshold"] = spillThreshold;
            this.OperationJObject["edgeObject"] = edgeObject;
        }

        public override void Callback(BulkResponse response, JObject content)
        {
            this.FirstSpillEdgeDocId = (string)content["firstSpillEdgeDocId"];
            this.NewEdgeDocId = (string)content["newEdgeDocId"];


            //
            // Actually, there might be at most three documents whose etag are upserted
            //  - The vertex document: when nonspilled->nonspilled or nonspilled->spilled or spilled-but-create-new-edge-document
            //  - The edge document (firstSpillEdgeDocId): when nonspilled->spilled, the existing edges are spilled into this document
            //  - The edge document (newEdgeDocId): when spilled, the new edge is always stored here
            //

            //
            // Update vertex JObject's etag (if necessary)
            //
            if (response.Etags.ContainsKey(this._vertexId))
            {
                // The vertex is updated, either because it is changed from non-spilled to spilled, or
                // because its latest spilled edge document is updated
                Debug.Assert(response.Etags[this._vertexId] != null);
                this._vertexObject[KW_DOC_ETAG] = response.Etags[this._vertexId];
            }

            //
            // Update vertex edgeContainer's content (if necessary)
            //
            JProperty vertexIsSpilled = this._vertexObject.Property(this._isReverse ? KW_VERTEX_REVEDGE_SPILLED : KW_VERTEX_EDGE_SPILLED);
            JArray edgeContainer = (JArray)this._vertexObject[this._isReverse ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE];
            if ((bool)vertexIsSpilled.Value)
            {
                // The edges are originally spilled (now it is still spilled)
                Debug.Assert(this.FirstSpillEdgeDocId == null);

                if (this.NewEdgeDocId == (string)edgeContainer[0][KW_DOC_ID])
                {
                    // Now the newly added edge is added to the latest edge document (not too large)
                    // Do nothing
                    // The vertex object should not be updated (etag not changed)
                    Debug.Assert(response.Etags[this._vertexId] == this._connection.VertexCache.GetCurrentEtag(this._vertexId));
                }
                else
                {
                    // Now the newly added edge is stored in a new edge document
                    // The original latest edge document is too small to store the new edge
                    // Update the vertex object's latest edge document id
                    Debug.Assert(response.Etags.ContainsKey(this._vertexId));
                    edgeContainer[0][KW_DOC_ID] = this.NewEdgeDocId;
                }
            }
            else
            {
                // The vertex's edges are originally not spilled
                Debug.Assert(response.Etags.ContainsKey(this._vertexId));

                if (this.FirstSpillEdgeDocId != null)
                {
                    // Now the vertex is changed from not-spilled to spilled
                    Debug.Assert(this.NewEdgeDocId != null);
                    vertexIsSpilled.Value = true;
                    edgeContainer.Clear();
                    edgeContainer.Add(new JObject
                    {
                        [KW_DOC_ID] = this.NewEdgeDocId
                    });
                }
                else
                {
                    // Now the vertex is still not spilled
                    Debug.Assert(this.NewEdgeDocId == null);

                    edgeContainer.Add(this._edgeObject);
                }
            }

        }
    }
    
    internal class BulkOperationDropVertexProperty : BulkOperation
    {
        public bool Found { get; private set; }

        public BulkOperationDropVertexProperty(GraphViewConnection connection, string vertexId, string propertyName)
            : base("DropVertexProperty")
        {
            /* Parameter schema: 
                {
                    "op": "DropVertexProperty",
                    "vertexId": ...,
                    "propertyName": ...,
                }
               Response content:
                {
                    "found": true/false
                }
            */

            this.OperationJObject["vertexId"] = vertexId;
            this.OperationJObject["propertyName"] = propertyName;

            this.KnownEtags[vertexId] = connection.VertexCache.GetCurrentEtag(vertexId);  // Not null
        }

        public override void Callback(BulkResponse response, JObject content)
        {
            this.Found = (bool)content["found"];
        }
    }
    
    internal class BulkOperationDropVertexSingleProperty : BulkOperation
    {
        public bool Found { get; private set; }

        public BulkOperationDropVertexSingleProperty(GraphViewConnection connection, string vertexId, string propertyName, string singlePropertyId)
            : base("DropVertexSingleProperty")
        {
            /* Parameter schema: 
                {
                    "op": "DropVertexProperty",
                    "vertexId": ...,
                    "propertyName": ...,
                    "singlePropertyId": ...,
                }
               Response content:
                {
                    "found": true/false
                }
            */

            this.OperationJObject["vertexId"] = vertexId;
            this.OperationJObject["propertyName"] = propertyName;
            this.OperationJObject["singlePropertyId"] = singlePropertyId;

            this.KnownEtags[vertexId] = connection.VertexCache.GetCurrentEtag(vertexId);  // Not null
        }

        public override void Callback(BulkResponse response, JObject content)
        {
            this.Found = (bool)content["found"];
        }
    }

    internal class BulkOperationDropVertexSinglePropertyMetaProperty : BulkOperation
    {
        public bool Found { get; private set; }

        public BulkOperationDropVertexSinglePropertyMetaProperty(GraphViewConnection connection, string vertexId, string propertyName, string singlePropertyId, string metaName)
            : base("DropVertexSinglePropertyMetaProperty")
        {
            /* Parameter schema: 
                {
                    "op": "DropVertexProperty",
                    "vertexId": ...,
                    "propertyName": ...,
                    "singlePropertyId": ...,
                    "metaName": ...,
                }
               Response content:
                {
                    "found": true/false
                }
            */

            this.OperationJObject["vertexId"] = vertexId;
            this.OperationJObject["propertyName"] = propertyName;
            this.OperationJObject["singlePropertyId"] = singlePropertyId;
            this.OperationJObject["metaName"] = metaName;

            this.KnownEtags[vertexId] = connection.VertexCache.GetCurrentEtag(vertexId);  // Not null
        }

        public override void Callback(BulkResponse response, JObject content)
        {
            this.Found = (bool)content["found"];
        }
    }




    internal enum BulkStatus : int
    {
        Success = 0,

        DBError = -1,
        NotAccepted = -2, // usually timeout
        AssertionFailed = -3,
        InternalError = -4,
    }


    internal class BulkResponse
    {
        [JsonProperty("Status", Required = Required.Always)]
        public BulkStatus Status { get; private set; }

        [JsonProperty("Message", Required = Required.Always)]
        public string Message { get; private set; }

        [JsonProperty("DocDBErrorCode", Required = Required.AllowNull)]
        public HttpStatusCode? DocDBErrorCode { get; private set; }

        [JsonProperty("DocDBErrorMessage", Required = Required.AllowNull)]
        public string DocDBErrorMessage { get; private set; }

        [JsonProperty("Content", Required = Required.Always)]
        public JArray Content { get; private set; }

        [JsonProperty("Etags", Required = Required.Always)]
        public Dictionary<string, string> Etags { get; private set; }

        [JsonConstructor]
        private BulkResponse() { }
    }
    

    internal class Bulk
    {
        public GraphViewConnection Connection { get; }

        public Bulk(GraphViewConnection connection)
        {
            this.Connection = connection;
        }


        private BulkResponse BulkCallInternal(JArray opArray, Dictionary<string, string> knownEtags)
        {
            string responseBody = this.Connection.ExecuteSProcBulkOperation(opArray, knownEtags);

            BulkResponse response = JsonConvert.DeserializeObject<BulkResponse>(responseBody);
            if (response.Status != BulkStatus.Success)
            {
                throw new Exception($"BulkCall failed: {response.Message}");
            }

            //
            // Update etags in cache
            //
            foreach (KeyValuePair<string, string> pair in response.Etags)
            {
                string docId = pair.Key;
                string etag = pair.Value;
                if (etag != null)
                {
                    // If (etag == null), it means the etag is unknown
                    // TODO: Check whether this situation will happen?
                    this.Connection.VertexCache.UpdateCurrentEtag(docId, etag);
                }
            }

            return response;
        }


        public void BulkCall(params BulkOperation[] operations)
        {
            JArray opArray = new JArray();
            Dictionary<string, string> knownEtags = new Dictionary<string, string>();

            // Prepare opArray
            foreach (BulkOperation operation in operations) {
                opArray.Add(operation.OperationJObject);
                foreach (KeyValuePair<string, string> pair in operation.KnownEtags) {
                    string docId = pair.Key;
                    string etag = pair.Value;
#if DEBUG
                    if (knownEtags.ContainsKey(docId)) {
                        Debug.Assert(knownEtags[docId] == etag);
                    }
#endif
                    knownEtags[docId] = etag;
                }
            }

            // Do the call!
            BulkResponse response = BulkCallInternal(opArray, knownEtags);

            // Invoke the callbacks
            for (int index = 0; index < operations.Length; ++index) {
                JToken content = response.Content[index];
                if ((content as JValue)?.Type == JTokenType.Null) {
                    operations[index].Callback(response, null);
                }
                else {
                    Debug.Assert(content is JObject);
                    operations[index].Callback(response, (JObject)content);
                }
            }
        }

    }
}
