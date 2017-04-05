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


namespace GraphView.GraphViewExecutionRuntime.Bulking
{

    public enum BulkOperationStatus : int
    {
        Success = 0,

        DBError = -1,
        NotAccepted = -2, // usually timeout
        AssertionFailed = -3,
        InternalError = -4,
    }


    public class BulkOperationResponse
    {
        [JsonProperty("Status", Required = Required.Always)]
        public BulkOperationStatus Status { get; private set; }

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
        private BulkOperationResponse() { }
    }
    

    internal class BulkOperation
    {
        public GraphViewConnection Connection { get; }

        public BulkOperation(GraphViewConnection connection)
        {
            this.Connection = connection;
        }



        private BulkOperationResponse BulkOperationCall(JObject operation, Dictionary<string, string> etags)
        {
            JArray opArray = new JArray { operation };
            string responseBody = this.Connection.ExecuteSProcBulkOperation(opArray, etags);

            BulkOperationResponse response = JsonConvert.DeserializeObject<BulkOperationResponse>(responseBody);
            if (response.Status != BulkOperationStatus.Success)
            {
                throw new Exception($"BulkOperationCall failed: {response.Message}");
            }
            return response;
        }


        public void AddVertex(JObject vertexObject)
        {
            /* Parameter schema: 
                {
                    "op": "AddVertex",
                    "vertexObject": { ... }
                }
               Response content:
                { }
            */

            JObject operation = new JObject
            {
                ["op"] = "AddVertex",
                ["vertexObject"] = vertexObject,
            };
            BulkOperationResponse response = BulkOperationCall(operation, new Dictionary<string, string>());

            vertexObject[KW_DOC_ETAG] = response.Etags[(string)vertexObject[KW_DOC_ID]];
            Debug.Assert(vertexObject[KW_DOC_ETAG] != null);
        }


        public void AddEdge(
            JObject srcVertexObject, JObject sinkVertexObject,
            bool isReverse, int? spillThreshold,
            JObject edgeObject,
            out string firstSpillEdgeDocId, out string newEdgeDocId)
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

            // Prepare etag dictionary
            string vertexId = isReverse ? sinkVId : srcVId;
            JObject vertexObject = isReverse ? sinkVertexObject : srcVertexObject;
            JProperty vertexIsSpilled = vertexObject.Property(isReverse ? KW_VERTEX_REVEDGE_SPILLED : KW_VERTEX_EDGE_SPILLED);
            JArray edgeContainer = (JArray)vertexObject[isReverse ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE];

            Dictionary<string, string> etags = new Dictionary<string, string>();
            etags[vertexId] = this.Connection.VertexCache.GetCurrentEtag(vertexId);
            if ((bool)vertexIsSpilled.Value)
            {
                string latestEdgeDocId = (string)edgeContainer[0][KW_DOC_ID];

                // etag of latest edge document may not exist: since this document might have not been accessed before
                string etag = this.Connection.VertexCache.TryGetCurrentEtag(latestEdgeDocId);
                if (etag != null)
                {
                    etags[latestEdgeDocId] = etag;
                }
            }

            JObject operation = new JObject
            {
                ["op"] = "AddEdge",
                ["srcV"] = srcVId,
                ["sinkV"] = sinkVId,
                ["isReverse"] = isReverse,
                ["spillThreshold"] = spillThreshold,
                ["edgeObject"] = edgeObject,
            };
            BulkOperationResponse response = BulkOperationCall(operation, etags);

            firstSpillEdgeDocId = (string)response.Content[0]["firstSpillEdgeDocId"];
            newEdgeDocId = (string)response.Content[0]["newEdgeDocId"];


            //
            // Actually, there might be at most three documents whose etag are upserted
            //  - The vertex document: when nonspilled->nonspilled or nonspilled->spilled or spilled-but-create-new-edge-document
            //  - The edge document (firstSpillEdgeDocId): when nonspilled->spilled, the existing edges are spilled into this document
            //  - The edge document (newEdgeDocId): when spilled, the new edge is always stored here
            //

            //
            // Update vertex JObject's etag (if necessary)
            //
            if (response.Etags.ContainsKey(vertexId))
            {
                // The vertex is updated, either because it is changed from non-spilled to spilled, or
                // because its latest spilled edge document is updated
                Debug.Assert(response.Etags[vertexId] != null);
                vertexObject[KW_DOC_ETAG] = response.Etags[vertexId];
            }

            //
            // Update vertex edgeContainer's content (if necessary)
            //
            if ((bool)vertexIsSpilled.Value)
            {
                // The edges are originally spilled (now it is still spilled)
                Debug.Assert(firstSpillEdgeDocId == null);

                if (newEdgeDocId == (string)edgeContainer[0][KW_DOC_ID])
                {
                    // Now the newly added edge is added to the latest edge document (not too large)
                    // Do nothing
                    // The vertex object should not be updated (etag not changed)
                    Debug.Assert(response.Etags[vertexId] == this.Connection.VertexCache.GetCurrentEtag(vertexId));
                }
                else
                {
                    // Now the newly added edge is stored in a new edge document
                    // The original latest edge document is too small to store the new edge
                    // Update the vertex object's latest edge document id
                    Debug.Assert(response.Etags.ContainsKey(vertexId));
                    edgeContainer[0][KW_DOC_ID] = newEdgeDocId;
                }
            }
            else
            {
                // The vertex's edges are originally not spilled
                Debug.Assert(response.Etags.ContainsKey(vertexId));

                if (firstSpillEdgeDocId != null)
                {
                    // Now the vertex is changed from not-spilled to spilled
                    Debug.Assert(newEdgeDocId != null);
                    vertexIsSpilled.Value = true;
                    edgeContainer.Clear();
                    edgeContainer.Add(new JObject
                    {
                        [KW_DOC_ID] = newEdgeDocId
                    });
                }
                else
                {
                    // Now the vertex is still not spilled
                    Debug.Assert(newEdgeDocId == null);

                    edgeContainer.Add(edgeObject);
                }
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
        }

    }
}
