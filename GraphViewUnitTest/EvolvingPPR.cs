using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using GraphView;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;
using System.Text;
using System.Threading.Tasks;



namespace GraphViewUnitTest
{
    [TestClass]
    public class EvolvingPPR
    {
        GraphViewCommand graph;
        LRUDictionary<string, CacheVertex> cache = new LRUDictionary<string, CacheVertex>(int.MaxValue);
        Dictionary<string, double> notConvergedV = new Dictionary<string, double>();
        double alpha = 0.85, beta = 1, bound = 0.01;

        double DefaultPageRank(string id)
        {
            return beta * (1 - alpha);
        }

        CacheVertex Touch(string id)
        {
            CacheVertex vertex;
            if (!cache.TryGetValue(id, out vertex))
            {
                vertex = new CacheVertex(
                    GetJTokenFromId(id), GetOutVerticesJArray(id));
            }
            cache.Add(id, vertex);
            return vertex;
        }

        public double GetPageRank(string id)
        {
            CacheVertex vertex = Touch(id);
            return vertex.pageRank;
        }
        static public double GetPageRank(JToken from)
        {
            return from["properties"]["PageRank"].First["value"].ToObject<double>();
        }

        public double GetResidue(string id)
        {
            try
            {
                return notConvergedV[id];
            }
            catch
            {
                CacheVertex vertex = Touch(id);
                return vertex.residue;
            } 
        }
        static public double GetResidue(JToken from)
        {
                return from["properties"]["Residue"].First["value"].ToObject<double>();
        }

        public List<string> GetOutVertices(string id)
        {
            CacheVertex vertex = Touch(id);
            return vertex.outVertices;
        }
        public List<string> GetOutVertices(JToken from)
        {
            return GetOutVertices(from["id"].ToString());
        }
        public JArray GetOutVerticesJArray(string id)
        {
            return JsonConvert.DeserializeObject<JArray>(
                graph.g().V().HasId(id).Out().Next().FirstOrDefault());
        }

        // the to exists in cache!
        public void UpdateResidue(double newResidue, string to)
        {
            if (Math.Abs(newResidue) > bound)
            {
                notConvergedV[to] = newResidue;
            }
            else
            {
                CacheVertex vertex;
                cache.TryGetValue(to, out vertex);
                cache.Remove(to);
                vertex.residue = newResidue;
                cache.Add(to, vertex);
            }
        }

        public JToken GetJTokenFromId(string id)
        {
            return JsonConvert.DeserializeObject<JArray>(
                graph.g().V().HasId(id).Next().FirstOrDefault())[0];
        }

        // Not converged yet
        public bool NotConverged()
        {
            return notConvergedV.Count != 0;
        }

        public string GetNotConverged()
        {
            var key = notConvergedV.Take(1).First().Key;
            return key;
        }

        
        // id exists in cache!
        public void UpdatePageRankAndResidue(double newPageRank, double newResidue, string id)
        {
            CacheVertex vertex;
            cache.TryGetValue(id, out vertex);
            cache.Remove(id);
            vertex.pageRank = newPageRank;
            vertex.residue = newResidue;
            cache.Add(id, vertex);
            /*
            graph.g().V().HasId(id).
                Property("PageRank", newPageRank).Property("Residue", newResidue).Next();
            */
        }
        public void UpdatePageRankAndResidue(double newPageRank, double newResidue, JToken to)
        {
            UpdatePageRankAndResidue(newPageRank, newResidue, to["id"].ToString());
        }
        public void UpdatePageRankAndResidueV(double newPageRank, double newResidue, string id)
        {
            graph.g().V().HasId(id).
                Property("PageRank", newPageRank).Property("Residue", newResidue).Next();
        }

        public void DoneWith(string v)
        {
            notConvergedV.Remove(v);
        }

        public void FinishAndWriteBack()
        {
            string key;
            while(cache.TryGetLast(out key))
            {
                CacheVertex vertex;
                cache.TryGetValue(key, out vertex);
                cache.Remove(key);
                graph.g().V().HasId(key).
                    Property("PageRank", vertex.pageRank).
                    Property("Residue", vertex.residue).Next();
            }
        }

        public void RunGSMethod()
        {
            while (NotConverged())
            {
                var v = GetNotConverged();
                // Update i-th PageRank and Residue
                var oldPageRank = GetPageRank(v);
                var oldResidue = GetResidue(v);
                var newPageRank = oldPageRank + oldResidue;
                UpdatePageRankAndResidue(newPageRank, 0d, v);
                // Update r
                var to = GetOutVertices(v);
                foreach (var t in to)
                {
                    double oldResidueOft = GetResidue(t);
                    double newResidueOft = oldResidueOft + alpha / to.Count * oldResidue;
                    UpdateResidue(newResidueOft, t);
                }
                DoneWith(v);
            }
        }

        public void AddE(string fromId, string toId)
        {
            graph.OutputFormat = OutputFormat.GraphSON;
            {
                var oldPageRank = GetPageRank(fromId);
                var to = GetOutVertices(fromId);

                // Update residue
                foreach (var t in to)
                {
                    double newResidue = GetResidue(t);
                    if (toId.Equals(t))
                    {
                        newResidue += alpha / to.Count * oldPageRank;
                    }
                    else
                    {
                        newResidue -= alpha / (to.Count * (to.Count - 1)) * oldPageRank;
                    }
                    UpdateResidue(newResidue, t);
                }

                RunGSMethod();

                FinishAndWriteBack();
            }
            graph.OutputFormat = OutputFormat.Regular;
        }

        public void AddV(string id)
        {
            UpdatePageRankAndResidueV(DefaultPageRank(id), 0d, id);
        }

        [TestMethod]
        public void LoadDataAndComputePageRank()
        {
            // Azure DocumentDB configuration
            string DOCDB_URL = "https://localhost:8081/";
            string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            string DOCDB_DATABASE = "PPR";
            string DOCDB_COLLECTION = "STAR";
            // create collection
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            connection.ResetCollection();
            graph = new GraphViewCommand(connection);

            // Froming a star-like graph
            for (int i = 1; i <= 3; i++)
            {
                string id, fromId;
                id = graph.g().AddV("nexus").Property("number", i).Values("id").Next()[0];
                AddV(id);
                // Froming stars
                for (int j = 1; j <= 3; j++)
                {
                    fromId = graph.g().AddV("node").Property("number", 10 * i + j).Values("id").Next()[0];
                    AddV(fromId);
                    graph.g().V().HasId(fromId).AddE("points_to").To(graph.g().V().HasId(id)).OutV().Values("id").Next();
                    AddE(fromId, id);
                }
            }

            for (int i = 1; i <= 3; i++)
            {
                string toId0, toId1, fromId;
                toId0 = graph.g().V().Has("number", i - 1 < 1 ? 3 : i - 1).Values("id").Next()[0];
                toId1 = graph.g().V().Has("number", i + 1 > 3 ? 1 : i + 1).Values("id").Next()[0];
                fromId = graph.g().V().Has("number", i).AddE("points_to").To(graph.g().V().HasId(toId0)).OutV().Values("id").Next()[0];
                AddE(fromId, toId0);
                graph.g().V().Has("number", i).AddE("points_to").To(graph.g().V().HasId(toId1)).Next();
                AddE(fromId, toId1);
            }
            Console.WriteLine("Finished!");
        }

        public class CacheVertex
        {
            public List<string> outVertices = new List<string>();
            public double pageRank;
            public double residue;
            public CacheVertex(JToken t, JArray outV)
            {
                pageRank = GetPageRank(t);
                residue = GetResidue(t);
                foreach (var o in outV)
                {
                    outVertices.Add(o["id"].ToString());
                }
            }
        }
    }

    public class LRUDictionary<TKey, TValue>
    {
        private Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>> _dict = 
            new Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>>();

        private LinkedList<KeyValuePair<TKey, TValue>> _list =
            new LinkedList<KeyValuePair<TKey, TValue>>();

        public int Max_Size { get; set; }

        public LRUDictionary(int maxsize)
        {
            Max_Size = maxsize;
        }

        public void Add(TKey key, TValue value)
        {
            lock (_dict)
            {
                LinkedListNode<KeyValuePair<TKey, TValue>> node;

                if (_dict.TryGetValue(key, out node))
                {
                    _list.Remove(node);
                    _list.AddFirst(node);
                }
                else
                {
                    node = new LinkedListNode<KeyValuePair<TKey, TValue>>(
                    new KeyValuePair<TKey, TValue>(key, value));

                    _dict.Add(key, node);
                    _list.AddFirst(node);

                }

                if (_dict.Count > Max_Size)
                {
                    var nodetoremove = _list.Last;
                    if (nodetoremove != null)
                        Remove(nodetoremove.Value.Key);
                }
            }

        }
        public bool Remove(TKey key)
        {
            lock (_dict)
            {
                LinkedListNode<KeyValuePair<TKey, TValue>> removednode;
                if (_dict.TryGetValue(key, out removednode))
                {
                    _dict.Remove(key);
                    _list.Remove(removednode);
                    return true;
                }

                else
                    return false;
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            LinkedListNode<KeyValuePair<TKey, TValue>> node;

            bool result = false;
            lock (_dict)
                result = _dict.TryGetValue(key, out node);

            if (node != null)
                value = node.Value.Value;
            else
                value = default(TValue);

            return result;
        }

        public bool TryGetLast(out TKey key)
        {
            key = default(TKey);

            if (_list.Count == 0)
                return false;

            key = _list.Last.Value.Key;
            return true;
        }
        
        public int Count()
        {
            return _list.Count;
        }
    }
}
