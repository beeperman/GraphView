using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphViewUnitTest
{
    [TestClass]
    public class PriorityFirstSearch
    {
        [TestMethod]
        public void LoadClassicGraphDataPFS()
        {
            /*
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "MarvelTest");
            */
            // Azure DocumentDB configuration
            string DOCDB_URL = "https://localhost:8081/";
            string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            string DOCDB_DATABASE = "GroupMatch";
            string DOCDB_COLLECTION = "MarvelTest";

            // create collection
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            connection.ResetCollection();
            GraphViewCommand graphCommand = new GraphViewCommand(connection);

            graphCommand.g().AddV("person").Property("name", "marko").Property("age", 29).Next();
            graphCommand.g().AddV("person").Property("name", "vadas").Property("age", 27).Next();
            graphCommand.g().AddV("software").Property("name", "lop").Property("lang", "java").Next();
            graphCommand.g().AddV("person").Property("name", "josh").Property("age", 32).Next();
            graphCommand.g().AddV("software").Property("name", "ripple").Property("lang", "java").Next();
            graphCommand.g().AddV("person").Property("name", "peter").Property("age", 35).Next();
            graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 0.5d).To(graphCommand.g().V().Has("name", "vadas")).Next();
            graphCommand.g().V().Has("name", "marko").AddE("knows").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "josh")).Next();
            graphCommand.g().V().Has("name", "marko").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 1.0d).To(graphCommand.g().V().Has("name", "ripple")).Next();
            graphCommand.g().V().Has("name", "josh").AddE("created").Property("weight", 0.4d).To(graphCommand.g().V().Has("name", "lop")).Next();
            graphCommand.g().V().Has("name", "peter").AddE("created").Property("weight", 0.2d).To(graphCommand.g().V().Has("name", "lop")).Next();

            // Dispose not used connection
            graphCommand.Dispose();
            connection.Dispose();
        }

        [TestMethod]
        public void PriorityFirstShortest()
        {
            /*
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "MarvelTest");
            */
            // Azure DocumentDB configuration
            string DOCDB_URL = "https://localhost:8081/";
            string DOCDB_AUTHKEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            string DOCDB_DATABASE = "GroupMatch";
            string DOCDB_COLLECTION = "MarvelTest";

            // create collection
            GraphViewConnection connection = new GraphViewConnection(DOCDB_URL, DOCDB_AUTHKEY, DOCDB_DATABASE, DOCDB_COLLECTION);
            GraphViewCommand graph = new GraphViewCommand(connection);

            var src1 = graph.g().V().HasLabel("person").Has("name", Predicate.eq("marko")).Values("id").Next()[0];
            var des1 = graph.g().V().HasLabel("software").Has("name", Predicate.eq("lop")).Values("id").Next()[0];
            int result1 = GetShortestPath(src1, des1, graph);
            Assert.AreEqual(result1, 1);

            var src2 = graph.g().V().HasLabel("person").Has("name", Predicate.eq("marko")).Values("id").Next()[0];
            var des2 = graph.g().V().HasLabel("software").Has("name", Predicate.eq("ripple")).Values("id").Next()[0];
            var result2 = GetShortestPath(src2, des2, graph);
            Assert.AreEqual(result2, 2);

            var src3 = graph.g().V().HasLabel("person").Has("name", Predicate.eq("peter")).Values("id").Next()[0];
            var des3 = graph.g().V().HasLabel("software").Has("name", Predicate.eq("lop")).Values("id").Next()[0];
            var result3 = GetShortestPath(src3, des3, graph);
            Assert.AreEqual(result3, 1);
        }
        public void UpdatePriority(String src, String v, GraphViewCommand graph) // priority = depth
        {
            var srcPriority = Int32.Parse(graph.g().V().HasId(src).Values("priority").Next()[0]);
            // Get results for further analysis
            var res = graph.g().V().HasId(v).Values("priority").Next();
            if (res.Count > 0 && Int32.Parse(res[0]) < srcPriority + 1)
            {
                // No need to update (this way longer)
                return;
            }
            var vPriority = srcPriority + 1;
            if (res.Count > 0)
            {
                // Drop previous priority ie. depth
                graph.g().V().HasId(v).Properties("priority").Drop().Next();
            }
            // Update priority
            graph.g().V().HasId(v).Property("priority", vPriority).Next();
        }
        public String LowestPriority(HashSet<String> visited, GraphViewCommand graph)
        {
            var vertices = graph.g().V().Values("id").Next();
            int lowest = int.MaxValue;
            String lowestV = "";
            foreach(var v in vertices)
            {
                if (visited.Contains(v))
                {
                    // Omit visited ones
                    continue;
                }
                var res = graph.g().V().HasId(v).Values("priority").Next();
                if (res.Count > 0 && Int32.Parse(res[0]) < lowest)
                {
                    lowest = Int32.Parse(res[0]);
                    lowestV = v;
                }
            }
            return lowestV;
        }
        public void PFS(String src, GraphViewCommand graph)
        {
            // Init
            graph.g().V().HasId(src).Property("priority", 0).Next(); // A 0 for source
            HashSet<String> visited = new HashSet<string>();
            visited.Add(src); // visit the source
            // Loop
            while (true)
            {
                var vIds = graph.g().V().HasId(src).Out().Values("id").Next();
                // Update Priority
                foreach (var vId in vIds)
                {
                    UpdatePriority(src, vId, graph);
                }
                // Get next
                String v;
                if (!(v = LowestPriority(visited, graph)).Equals(""))
                {
                    src = v;
                }
                if (visited.Contains(src))
                {
                    // End
                    break;
                }
                // Mark as visited and loop
                visited.Add(src);
            }
        }
        public int GetShortestPath(String src, String des, GraphViewCommand graph)
        {
            PFS(src, graph);
            int depth = 0;
            var res = graph.g().V().HasId(des).Values("priority").Next();
            if (res.Count > 0)
            {
                depth = Int32.Parse(res[0]);
            }
            return depth;
            /*
            Queue<String> vertexIdQ1 = new Queue<String>();
            Queue<String> vertexIdQ2 = new Queue<String>();
            HashSet<String> historyVertex = new HashSet<string>();

            Boolean reachDes = false;
            int depth = 1;
            vertexIdQ1.Enqueue(src);

            while (!reachDes && vertexIdQ1.Count != 0)
            {
                var id = vertexIdQ1.Dequeue();
                var tempVertexIds = graph.g().V().HasId(id).Out().Values("id").Next();

                foreach (var vertexId in tempVertexIds)
                {
                    if (historyVertex.Contains(vertexId))
                    {
                        continue;
                    }
                    else
                    {
                        historyVertex.Add(vertexId);
                    }
                    if (vertexId == des)
                    {
                        reachDes = true;
                        break;
                    }
                    else if (vertexId != src)
                    {
                        vertexIdQ2.Enqueue(vertexId);
                    }
                }
                // the uppper level queue become empty, move to next level of graph
                if (vertexIdQ1.Count == 0 && !reachDes)
                {
                    var swap = vertexIdQ1;
                    vertexIdQ1 = vertexIdQ2;
                    vertexIdQ2 = swap;
                    depth++;
                }
            }

            if (reachDes)
            {
                Console.WriteLine("Shortest Path from {0} to {1}, depth is {2}", src, des, depth);
            }
            else
            {
                Console.WriteLine("No path from {0} to {1}, depth is {2}", src, des, 0);
            }
            return depth;
            */
        }
    }
}
