﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.IO;

namespace GremlinTranslationOperator.Tests
{
    [TestClass()]
    public class GremlinTranslationOperator
    {
        [TestMethod]
        public void test()
        {
            const string q2 = @"select null as number from n_0";

            var sr = new StringReader(q2);
            var parser = new GraphViewParser();
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;

            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);
        }

        [TestMethod]
        public void TestModernGraph()
        {
            GraphViewConnection connection = null;
            //GraphTraversal2.SetGraphViewConnection(connection);
            //GraphTraversal2.g().addV("person").property("age", "27").property("name", "vadas").next();
            //GraphTraversal2.g().addV("person").property("age", "29").property("name", "marko").next();
            //GraphTraversal2.g().addV("person").property("age", "35").property("name", "peter").next();
            //GraphTraversal2.g().addV("person").property("age", "32").property("name", "josh").next();
            //GraphTraversal2.g().addV("software").property("lang", "java").property("name", "lop").next();
            //GraphTraversal2.g().addV("software").property("lang", "java").property("name", "ripple").next();

            //GraphTraversal2.g().V().Has("name", "marko").AddE("knows").To(GraphTraversal2.g().V().Has("name", "vadas")).next();
            //GraphTraversal2.g().V().has("name", "marko").addE("knows").to(GraphTraversal2.g().V().has("name", "josh")).next();
            //GraphTraversal2.g().V().has("name", "marko").addE("knows").to(GraphTraversal2.g().V().has("name", "lop")).next();
            //GraphTraversal2.g().V().has("name", "peter").addE("created").to(GraphTraversal2.g().V().has("name", "lop")).next();
            //GraphTraversal2.g().V().has("name", "josh").addE("created").to(GraphTraversal2.g().V().has("name", "lop")).next();
            //GraphTraversal2.g().V().has("name", "josh").addE("created").to(GraphTraversal2.g().V().has("name", "ripple")).next();

            // v("lop")
            // v("lop")
            // v("lop")
            // v("vadas")
            // v("josh")
            // v("ripple")
            //GraphTraversal2.g().V().Out().next();

        }

        [TestMethod]
        public void TestStep()
        {

            //GraphTraversal2.g()
            //    .V().Out("jinjin").Optional(GraphTraversal2.__().Out("mdl").OutE().InV()).next();

            //GraphTraversal2.g().V().Local(GraphTraversal2.__().OutE()).Properties("name", "age").Key().next();

            //GraphTraversal2.g().V()
            //    .Project("vertex", "parents", "references", "model")
            //    .By(GraphTraversal2.__().Emit().Repeat(GraphTraversal2.__().OutE("_val").As("_").InV()).Tree())
            //    .By(GraphTraversal2.__().OutE().Label().Dedup().Fold())
            //    .By(GraphTraversal2.__().As("@v")
            //        .FlatMap(GraphTraversal2.__().Out("mdl").OutE("ref"))
            //        .Repeat(GraphTraversal2.__().As("@e")
            //            .FlatMap(GraphTraversal2.__().InV()
            //                .As("mdl")
            //                .Select(GremlinKeyword.Pop.last, "@v")
            //                .Both()
            //                .Where(GraphTraversal2.__().Out("mdl")
            //                    .Where(Predicate.eq("mdl"))))
            //            .As("@v")
            //            .Optional(GraphTraversal2.__().FlatMap(
            //                GraphTraversal2.__().Select(GremlinKeyword.Pop.last, "@e")
            //                    .Values("_ref")
            //                    .As("key")
            //                    .Select(GremlinKeyword.Pop.last, "@v")
            //                    .Out("mdl")
            //                    .OutE("ref")
            //                    .Where(GraphTraversal2.__().Values("_key")
            //                        .Where(Predicate.eq("key"))))))
            //        .Until(GraphTraversal2.__().FlatMap(
            //            GraphTraversal2.__().As("res").Select(GremlinKeyword.Pop.last, "@v").Where(Predicate.eq("res"))))
            //        .Union(GraphTraversal2.__().Dedup()
            //                .Emit()
            //                .Repeat(GraphTraversal2.__().OutE("_val").As("_").InV())
            //                .Tree(),
            //            GraphTraversal2.__().Project("id", "key", "ref")
            //                .By(GraphTraversal2.__().Id())
            //                .By(GraphTraversal2.__().Select(GremlinKeyword.Pop.first, "@e").Values("_key"))
            //                .By(GraphTraversal2.__().Select(GremlinKeyword.Pop.last, "@e").Values("_ref"))
            //                .Fold())
            //        .Fold())
            //    .By(GraphTraversal2.__().Out("mdl").Project("vertex").By(GraphTraversal2.__().Tree())).next();


            //GraphTraversal2.g().V()
            //    .has("_app", "test-app")
            //    .has("_id", "product:soda-machine:shop-2")
            //    .hasLabel("product")
            //    .flatMap(GraphTraversal2.__().As("src")
            //        .flatMap(GraphTraversal2.g().V()
            //            .has("_app", "test-app")
            //            .has("_id", "device:soda-mixer:shop-1")
            //            .hasLabel("device"))
            //        .As("tgt")
            //        .select("src")
            //        .coalesce(GraphTraversal2.__().inE("device-product"),
            //                     GraphTraversal2.__().inE("device-product")
            //            //GraphTraversal2.__().addE("device-product").from("tgt")
            //            )
            //    )
            //    .count()
            //    .next();


            
        }
    }
}