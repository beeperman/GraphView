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
        Dictionary<String, Double> notConvergedV = new Dictionary<string, double>();
        double alpha = 0.85, beta = 1, bound = 0.01;

        double DefaultPageRank(string id)
        {
            return beta * (1 - alpha);
        }

        public double GetPageRank(string id)
        {
            return GetPageRank(GetJTokenFromId(id));
        }
        public double GetPageRank(JToken from)
        {
            return from["properties"]["PageRank"].First["value"].ToObject<double>();
        }

        public void UpdatePageRankAndResidue(double newPageRank, double newResidue, string id)
        {
            graph.g().V().HasId(id).
                Property("PageRank", newPageRank).Property("Residue", newResidue).Next();
        }
        public void UpdatePageRankAndResidue(double newPageRank, double newResidue, JToken to)
        {
            UpdatePageRankAndResidue(newPageRank, newResidue, to["id"].ToString());
        }

        public double GetResidue(JToken from)
        {
            try
            {
                return notConvergedV[from["id"].ToString()];
            }
            catch
            {
                return from["properties"]["Residue"].First["value"].ToObject<double>();
            }
        }

        public void UpdateResidue(double newResidue, JToken to)
        {
            if (Math.Abs(newResidue) > bound)
            {
                notConvergedV[to["id"].ToString()] = newResidue;
            }
            else
            {
                graph.g().V().HasId(to["id"].ToString()).Property("Residue", newResidue).Next();
            }
        }

        public JToken GetJTokenFromId(string id)
        {
            return JsonConvert.DeserializeObject<JArray>(graph.g().V().HasId(id).Next().FirstOrDefault())[0];
        }

        // Not converged yet
        public bool NotConverged()
        {
            return notConvergedV.Count != 0;
        }

        public JToken GetNotConverged()
        {
            var key = notConvergedV.Take(1).First().Key;
            return GetJTokenFromId(key);
        }

        public JArray GetOutVertices(string id)
        {
            return JsonConvert.DeserializeObject<JArray>(
                graph.g().V().HasId(id).Out().Next().FirstOrDefault());
        }
        public JArray GetOutVertices(JToken from)
        {
            return GetOutVertices(from["id"].ToString());
        }

        public void DoneWith(JToken v)
        {
            notConvergedV.Remove(v["id"].ToString());
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
                JArray to = GetOutVertices(fromId);

                // Update residue
                foreach (var t in to)
                {
                    double newResidue = GetResidue(t);
                    if (toId.Equals(t["id"].ToString()))
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

            }
            graph.OutputFormat = OutputFormat.Regular;
        }

        public void AddV(string id)
        {
            UpdatePageRankAndResidue(DefaultPageRank(id), 0d, id);
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
    }
}
