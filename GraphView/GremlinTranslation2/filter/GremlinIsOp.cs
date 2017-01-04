﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinIsOp: GremlinTranslationOperator
    {
        public object Value { get; set; }
        public Predicate Predicate { get; set; }

        public GremlinIsOp(object value)
        {
            Value = value;
        }

        public GremlinIsOp(Predicate predicate)
        {
            Predicate = predicate;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (Value != null)
            {
                inputContext.PivotVariable.Is(inputContext, Value);
            }
            else
            {
                inputContext.PivotVariable.Is(inputContext, Predicate);
            }

            return inputContext;
        }
    }
}
