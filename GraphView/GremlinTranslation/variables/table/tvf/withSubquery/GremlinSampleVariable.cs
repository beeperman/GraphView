using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSampleVariable : GremlinTableVariable
    {
        public int AmountToSample { get; set; }
        public GremlinToSqlContext ProbabilityContext { get; set; }

        public GremlinSampleVariable(int amountToSample, GremlinToSqlContext probabilityContext)
            : base(GremlinVariableType.Table)
        {
            this.AmountToSample = amountToSample;
            this.ProbabilityContext = probabilityContext;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.ProbabilityContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.ProbabilityContext.FetchAllTableVars());
            return variableList;
        }
    }

    internal class GremlinSampleGlobalVariable : GremlinSampleVariable
    {
        public GremlinSampleGlobalVariable(int amountToSample, GremlinToSqlContext probabilityContext)
            : base(amountToSample, probabilityContext)
        {
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression> {SqlUtil.GetValueExpr(this.AmountToSample)};
            if (this.ProbabilityContext != null)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(this.ProbabilityContext.ToSelectQueryBlock()));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SampleGlobal, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinSampleLocalVariable : GremlinSampleVariable
    {
        public GremlinSampleLocalVariable(int amountToSample, GremlinToSqlContext probabilityContext)
            : base(amountToSample, probabilityContext)
        {
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression> {SqlUtil.GetValueExpr(this.AmountToSample)};
            if (this.ProbabilityContext != null)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(this.ProbabilityContext.ToSelectQueryBlock()));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SampleLocal, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
