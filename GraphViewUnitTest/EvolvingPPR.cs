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
    public class InaccurateV
    {
        public string id = "";
        public double residue = 0;
    }
    [TestClass]
    public class EvolvingPPR
    {
        GraphViewCommand graph;
        Dictionary<String, Double> notConvergedV = new Dictionary<string, double>();
        double alpha = 0.85, beta = 1, bound = 0.00001;
        double DefaultPR(string id)
        {
            return beta * (1 - alpha);
        }
        public void runGSMethod()
        {
            while (notConvergedV.Count != 0)
            {
                var key = notConvergedV.Take(1).First().Key;
                var v = JsonConvert.DeserializeObject<JArray>(graph.g().V().HasId(key).Next().FirstOrDefault())[0];
                // Update xi and ri
                var oldPageRank = v["properties"]["PageRank"].First["value"].ToObject<double>();
                double oldResidue;
                try
                {
                    oldResidue = notConvergedV[v["id"].ToString()];
                }
                catch
                {
                    oldResidue = v["properties"]["Residue"].First["value"].ToObject<double>();
                }

                double newPageRank = oldPageRank + oldResidue;
                graph.g().V().HasId(key).Property("PageRank", newPageRank).Property("Residue", 0d).Next();
                // Update r
                // valus id ??
                var to = JsonConvert.DeserializeObject<JArray>(graph.g().V().HasId(key).Out().Next().FirstOrDefault());
                foreach (var t in to)
                {
                    double oldResidueOft;
                    try
                    {
                        oldResidueOft = notConvergedV[t["id"].ToString()];
                    }
                    catch
                    {
                        oldResidueOft = t["properties"]["Residue"].First["value"].ToObject<double>();
                    }

                    double newResidueOft = oldResidueOft + alpha / to.Count * oldResidue;
                    if (Math.Abs(newResidueOft) > bound)
                    {
                        notConvergedV[t["id"].ToString()] = newResidueOft;
                    }
                    else
                    {
                        graph.g().V().HasId(t["id"].ToString()).Property("Residue", newResidueOft).Next();
                    }
                }
                notConvergedV.Remove(key);
            }
        }
        public void AddV(string id)
        {
            graph.g().V().HasId(id).Property("PageRank", DefaultPR(id)).Property("Residue", 0d).Next();
        }
        public void AddE(string fromId, string toId)
        {
            graph.OutputFormat = OutputFormat.GraphSON;
            var from = JsonConvert.DeserializeObject<JArray>(graph.g().V().HasId(fromId).Next().FirstOrDefault())[0];
            var xi = from["properties"]["PageRank"].First["value"].ToObject<double>();
            
            JArray to = JsonConvert.DeserializeObject<JArray>(graph.g().V().HasId(fromId).Out().Next().FirstOrDefault());
            
            // Update residue
            foreach (var t in to)
            {
                double newResidue;
                try
                {
                    newResidue = notConvergedV[t["id"].ToString()];
                }
                catch
                {
                    newResidue = t["properties"]["Residue"].First["value"].ToObject<double>();
                }
                if (toId.Equals(t["id"].ToString()))
                {
                    newResidue += alpha / to.Count * xi;
                }
                else
                {
                    newResidue -= alpha / (to.Count * (to.Count - 1)) * xi;
                }
                if (Math.Abs(newResidue) > bound)
                {
                    notConvergedV[t["id"].ToString()] = newResidue;
                }
                else
                {
                    graph.g().V().HasId(t["id"].ToString()).Property("Residue", newResidue).Next();
                }
            }
            runGSMethod();
            graph.OutputFormat = OutputFormat.Regular;
        }
        public void AddE(string fromId, string[] toId)
        {
            foreach(var id in toId)
            {
                AddE(fromId, id);
            }
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
            //var toId = graph.g().V().Has("number", 0).Values("id").Next()[0];
            //var fromId = graph.g().V().HasId(toId).AddE("points_to").To(graph.g().V().HasId(toId)).InV().Values("id").Next()[0];
            //AddE(fromId, toId);
        }
    }
}
