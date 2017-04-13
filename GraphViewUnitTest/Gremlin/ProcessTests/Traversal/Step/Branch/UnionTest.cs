﻿using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using GraphView;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphViewUnitTest.Gremlin.ProcessTests.Traversal.Step.Branch
{
    /// <summary>
    /// Tests for the Union Step.
    /// </summary>
    [TestClass]
    public class UnionTest : AbstractGremlinTest
    {
        /// <summary>
        /// Port of the g_V_unionXout__inX_name() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V.union(__.out, __.in).name"
        /// </summary>
        [TestMethod]
        public void UnionOutAndInVertices()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Union(
                    GraphTraversal2.__().Out(),
                    GraphTraversal2.__().In()).Values("name");

                var result = traversal.Next();
                var expectedResult = new List<string>() { "marko", "marko", "marko", "lop", "lop", "lop", "peter", "ripple", "josh", "josh", "josh", "vadas" };
                AbstractGremlinTest.CheckUnOrderedResults<string>(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_VX1X_unionXrepeatXoutX_timesX2X__outX_name() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V(v1Id).union(repeat(__.out).times(2), __.out).name", "v1Id", v1Id
        /// </summary>
        /// <remarks>
        /// It appears that the Repeat Times 2 steps aren't working, as this the result is only returing marko's immediate Out vertices.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36529
        /// </remarks>
        [TestMethod]
        public void HasVertexIdUnionOutAndOutRepeatTimes2()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                string vertexId = this.ConvertToVertexId(GraphViewCommand, "marko");

                var traversal = GraphViewCommand.g().V().HasId(vertexId)
                    .Union(
                        GraphTraversal2.__().Repeat(GraphTraversal2.__().Out()).Times(2),
                        GraphTraversal2.__().Out())
                    .Values("name");

                var result = traversal.Next();
                var expectedResult = new List<string>() { "lop", "lop", "ripple", "josh", "vadas" };
                AbstractGremlinTest.CheckUnOrderedResults<string>(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V.choose(__.label.is('person'), union(__.out.lang, __.out.name), __.in.label)"
        /// </summary>
        [TestMethod]
        public void ChooseIfPersonThenUnionOutLangOutAndOutNameElseInLabel()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                var traversal = GraphViewCommand.g().V().Choose(
                    GraphTraversal2.__().Label().Is("person"),
                    GraphTraversal2.__().Union(
                            GraphTraversal2.__().Out().Values("lang"),
                            GraphTraversal2.__().Out().Values("name")),
                    GraphTraversal2.__().In().Label());

                var result = traversal.Next();
                var expectedResult = new List<string>() { "lop", "lop", "lop", "ripple", "java", "java", "java", "java", "josh", "vadas", "person", "person", "person", "person" };
                AbstractGremlinTest.CheckUnOrderedResults<string>(expectedResult, result);
            }
        }

        /// <summary>
        /// Port of the g_V_chooseXlabel_eq_person__unionX__out_lang__out_nameX__in_labelX_groupCount() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V.choose(__.label.is('person'), union(__.out.lang, __.out.name), __.in.label).groupCount"
        /// </summary>
        /// <remarks>
        /// </remarks>
        [TestMethod]
        public void ChooseIfPersonThenUnionOutLangOutAndOutNameElseInLabelGroupCount()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V().Choose(
                    GraphTraversal2.__().Label().Is("person"),
                    GraphTraversal2.__().Union(
                        GraphTraversal2.__().Out().Values("lang"),
                        GraphTraversal2.__().Out().Values("name")),
                    GraphTraversal2.__().In().Label())
                    .GroupCount();

                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());
                
                Assert.AreEqual(4, (int)result[0]["java"]);
                Assert.AreEqual(1, (int)result[0]["ripple"]);
                Assert.AreEqual(4, (int)result[0]["person"]);
                Assert.AreEqual(1, (int)result[0]["vadas"]);
                Assert.AreEqual(1, (int)result[0]["josh"]);
                Assert.AreEqual(3, (int)result[0]["lop"]);
            }
        }

        /// <summary>
        /// Port of the g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V.union(repeat(union(out('created'), __.in('created'))).times(2), repeat(union(__.in('created'), out('created'))).times(2)).label.groupCount()"
        /// </summary>

        [TestMethod]
        public void UnionRepeatUnionOutCreatedInCreatedTimes2RepeatUnionInCreatedOutCreatedTimes2LabelGroupCount()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                var traversal = GraphViewCommand.g().V().Union(
                    GraphTraversal2.__().Repeat(
                        GraphTraversal2.__().Union(
                            GraphTraversal2.__().Out("created"),
                            GraphTraversal2.__().In("created"))).Times(2),
                    GraphTraversal2.__().Repeat(
                        GraphTraversal2.__().Union(
                            GraphTraversal2.__().In("created"),
                            GraphTraversal2.__().Out("created"))).Times(2))
                    .Label().GroupCount();

                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.Next().FirstOrDefault());

                Assert.AreEqual(12, (int)result[0]["software"]);
                Assert.AreEqual(20, (int)result[0]["person"]);
            }
        }

        /// <summary>
        /// Port of the g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX() UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V(v1Id, v2Id).local(union(outE().count, inE().count, outE().weight.sum))", "v1Id", v1Id, "v2Id", v2Id
        /// </summary>
        /// <remarks>
        /// The traversal fails because GremlinSumOp.GetContext() throws a NotImplementedException.
        /// WorkItem to track this: https://msdata.visualstudio.com/DocumentDB/_workitems/edit/36732
        /// </remarks>
        [TestMethod]
        public void HasVId1VId2LocalUnionOutECountInECountOutEWeightSum()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                string vertexId1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(GraphViewCommand, "vadas");

                var traversal = GraphViewCommand.g().V(vertexId1, vertexId2)
                    .Local(
                        GraphTraversal2.__().Union(
                            GraphTraversal2.__().OutE().Count(),
                            GraphTraversal2.__().InE().Count(),
                            GraphTraversal2.__().OutE().Values("weight").Sum()));
                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.FirstOrDefault());

                // Assertions missing, revisit this once we actually get the above traversal to work.
                CheckUnOrderedResults(new double[] { 0d, 0d, 0d, 3d, 1d, 1.9d}, ((JArray)result).Select(j => j.ToObject<double>()).ToList());
            }
        }

        /// <summary>
        /// Port of the g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX UT from org/apache/tinkerpop/gremlin/process/traversal/step/branch/UnionTest.java.
        /// Equivalent gremlin: "g.V(v1Id, v2Id).union(outE().count, inE().count, outE().weight.sum)", "v1Id", v1Id, "v2Id", v2Id
        /// </summary>
        [TestMethod]
        public void HasVId1VId2UnionOutECountInECountOutEWeightSum()
        {
            using (GraphViewCommand GraphViewCommand = new GraphViewCommand(graphConnection))
            {
                GraphViewCommand.OutputFormat = OutputFormat.GraphSON;
                string vertexId1 = this.ConvertToVertexId(GraphViewCommand, "marko");
                string vertexId2 = this.ConvertToVertexId(GraphViewCommand, "vadas");

                var traversal = GraphViewCommand.g().V(vertexId1, vertexId2)
                    .Union(
                        GraphTraversal2.__().OutE().Count(),
                        GraphTraversal2.__().InE().Count(),
                        GraphTraversal2.__().OutE().Values("weight").Sum());

                dynamic result = JsonConvert.DeserializeObject<dynamic>(traversal.FirstOrDefault());

                CheckUnOrderedResults(new double[] { 3d, 1.9d, 1d }, ((JArray)result).Select(j => j.ToObject<double>()).ToList());
            }
        }
    }
}

