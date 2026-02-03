from __future__ import annotations

from typing import Collection

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet
from metricflow_semantics.sql.sql_exprs import (
    SqlAddTimeExpression,
    SqlCastToTimestampExpression,
    SqlDateTruncExpression,
    SqlExtractExpression,
    SqlGenerateUuidExpression,
    SqlPercentileExpression,
    SqlPercentileFunctionType,
    SqlSubtractTimeIntervalExpression,
)
from typing_extensions import override

from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.render.expr_renderer import (
    DefaultSqlExpressionRenderer,
    SqlExpressionRenderer,
    SqlExpressionRenderResult,
)
from metricflow.sql.render.sql_plan_renderer import DefaultSqlPlanRenderer
from metricflow.sql.sql_plan import SqlSelectColumn


class ClickhouseSqlExpressionRenderer(DefaultSqlExpressionRenderer):
    """Expression renderer for the ClickHouse engine."""

    sql_engine = SqlEngine.CLICKHOUSE

    @property
    @override
    def double_data_type(self) -> str:
        """Custom double data type for ClickHouse engine."""
        return "Nullable(DOUBLE PRECISION)"

    @property
    @override
    def timestamp_data_type(self) -> str:
        """Custom timestamp type for ClickHouse engine."""
        return "Nullable(TIMESTAMP)"

    @property
    @override
    def supported_percentile_function_types(self) -> Collection[SqlPercentileFunctionType]:
        return {
            SqlPercentileFunctionType.CONTINUOUS,
            SqlPercentileFunctionType.DISCRETE,
            SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS,
            SqlPercentileFunctionType.APPROXIMATE_DISCRETE,
        }

    @override
    def render_group_by_expr(self, group_by_column: SqlSelectColumn) -> SqlExpressionRenderResult:
        """Custom rendering of group by column expressions.

        ClickHouse requires group bys to be referenced by alias, rather than duplicating the expression from the SELECT.
        This is similar to BigQuery behavior.

        e.g.,
          SELECT COALESCE(x, y) AS x_or_y, SUM(1)
          FROM source_table
          GROUP BY x_or_y

        By default we would render GROUP BY COALESCE(x, y) on that last line, and ClickHouse will throw an exception.
        """
        return SqlExpressionRenderResult(
            sql=group_by_column.column_alias,
            bind_parameter_set=group_by_column.expr.bind_parameter_set,
        )

    @override
    def visit_date_trunc_expr(self, node: SqlDateTruncExpression) -> SqlExpressionRenderResult:
        """Render DATE_TRUNC for ClickHouse using toStartOf* functions."""
        self._validate_granularity_for_engine(node.time_granularity)

        arg_rendered = self.render_sql_expr(node.arg)

        # ClickHouse uses toStartOf* functions instead of DATE_TRUNC
        granularity_to_function = {
            TimeGranularity.DAY: "toStartOfDay",
            TimeGranularity.WEEK: "toStartOfWeek",
            TimeGranularity.MONTH: "toStartOfMonth",
            TimeGranularity.QUARTER: "toStartOfQuarter",
            TimeGranularity.YEAR: "toStartOfYear",
            TimeGranularity.HOUR: "toStartOfHour",
            TimeGranularity.MINUTE: "toStartOfMinute",
            TimeGranularity.SECOND: "toStartOfSecond",
            TimeGranularity.MILLISECOND: "toStartOfMillisecond",
        }

        func_name = granularity_to_function.get(node.time_granularity)
        if func_name is None:
            # Fall back to DATE_TRUNC for unsupported granularities
            return SqlExpressionRenderResult(
                sql=f"DATE_TRUNC('{node.time_granularity.value}', {arg_rendered.sql})",
                bind_parameter_set=arg_rendered.bind_parameter_set,
            )

        # toStartOfWeek takes an optional mode parameter (0 = Sunday, 1 = Monday)
        # Use mode 1 for ISO week (Monday start)
        if node.time_granularity == TimeGranularity.WEEK:
            return SqlExpressionRenderResult(
                sql=f"{func_name}({arg_rendered.sql}, 1)",
                bind_parameter_set=arg_rendered.bind_parameter_set,
            )

        # For millisecond precision, we need to cast to DateTime64
        if node.time_granularity == TimeGranularity.MILLISECOND:
            return SqlExpressionRenderResult(
                sql=f"{func_name}(CAST({arg_rendered.sql} AS DateTime64(3)))",
                bind_parameter_set=arg_rendered.bind_parameter_set,
            )

        return SqlExpressionRenderResult(
            sql=f"{func_name}({arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_extract_expr(self, node: SqlExtractExpression) -> SqlExpressionRenderResult:
        """Render EXTRACT for ClickHouse using specific functions."""
        arg_rendered = self.render_sql_expr(node.arg)

        # ClickHouse uses specific functions for date part extraction
        date_part_to_function = {
            DatePart.YEAR: "toYear",
            DatePart.QUARTER: "toQuarter",
            DatePart.MONTH: "toMonth",
            DatePart.DAY: "toDayOfMonth",
            DatePart.DOW: "toDayOfWeek",
            DatePart.DOY: "toDayOfYear",
        }

        func_name = date_part_to_function.get(node.date_part)
        if func_name is None:
            # Fall back to EXTRACT for unsupported date parts
            return SqlExpressionRenderResult(
                sql=f"EXTRACT({self.render_date_part(node.date_part)} FROM {arg_rendered.sql})",
                bind_parameter_set=arg_rendered.bind_parameter_set,
            )

        return SqlExpressionRenderResult(
            sql=f"{func_name}({arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def render_date_part(self, date_part: DatePart) -> str:
        """Render DATE PART for an EXTRACT expression.

        ClickHouse's toDayOfWeek returns ISO Monday=1, Sunday=7 by default, which is what we want.
        """
        if date_part is DatePart.DOW:
            return "dayofweek"

        return date_part.value

    @override
    def visit_cast_to_timestamp_expr(self, node: SqlCastToTimestampExpression) -> SqlExpressionRenderResult:
        """Casts the time value expression to DateTime64 with millisecond precision.

        ClickHouse uses toDateTime64 for timestamp conversion with configurable precision.
        Using precision 3 for millisecond accuracy.
        """
        arg_rendered = self.render_sql_expr(node.arg)
        return SqlExpressionRenderResult(
            sql=f"toDateTime64({arg_rendered.sql}, 3)",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_subtract_time_interval_expr(self, node: SqlSubtractTimeIntervalExpression) -> SqlExpressionRenderResult:
        """Render time delta subtraction for ClickHouse."""
        arg_rendered = node.arg.accept(self)

        count = node.count
        granularity = node.granularity
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            count *= 3

        # ClickHouse uses date_sub or subtraction with INTERVAL
        return SqlExpressionRenderResult(
            sql=f"date_sub({granularity.value}, {count}, {arg_rendered.sql})",
            bind_parameter_set=arg_rendered.bind_parameter_set,
        )

    @override
    def visit_add_time_expr(self, node: SqlAddTimeExpression) -> SqlExpressionRenderResult:
        """Render time delta addition for ClickHouse."""
        granularity = node.granularity
        count_expr = node.count_expr
        if granularity is TimeGranularity.QUARTER:
            granularity = TimeGranularity.MONTH
            from metricflow_semantics.sql.sql_exprs import SqlArithmeticExpression, SqlArithmeticOperator, SqlIntegerExpression
            count_expr = SqlArithmeticExpression.create(
                left_expr=node.count_expr,
                operator=SqlArithmeticOperator.MULTIPLY,
                right_expr=SqlIntegerExpression.create(3),
            )

        arg_rendered = node.arg.accept(self)
        count_rendered = count_expr.accept(self)
        count_sql = f"({count_rendered.sql})" if count_expr.requires_parenthesis else count_rendered.sql

        # ClickHouse uses date_add
        return SqlExpressionRenderResult(
            sql=f"date_add({granularity.value}, {count_sql}, {arg_rendered.sql})",
            bind_parameter_set=SqlBindParameterSet.merge_iterable(
                (arg_rendered.bind_parameter_set, count_rendered.bind_parameter_set)
            ),
        )

    @override
    def visit_generate_uuid_expr(self, node: SqlGenerateUuidExpression) -> SqlExpressionRenderResult:
        """Generate a UUID using ClickHouse's generateUUIDv4 function."""
        return SqlExpressionRenderResult(
            sql="generateUUIDv4()",
            bind_parameter_set=SqlBindParameterSet(),
        )

    @override
    def visit_percentile_expr(self, node: SqlPercentileExpression) -> SqlExpressionRenderResult:
        """Render a percentile expression for ClickHouse.

        ClickHouse uses quantile(p)(column) syntax for percentiles.
        - quantile: continuous approximation
        - quantileExact: exact discrete
        - quantileExactExclusive: exact continuous
        """
        arg_rendered = self.render_sql_expr(node.order_by_arg)
        params = arg_rendered.bind_parameter_set
        percentile = node.percentile_args.percentile

        if node.percentile_args.function_type is SqlPercentileFunctionType.CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"quantileExactExclusive({percentile})({arg_rendered.sql})",
                bind_parameter_set=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.DISCRETE:
            return SqlExpressionRenderResult(
                sql=f"quantileExact({percentile})({arg_rendered.sql})",
                bind_parameter_set=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_CONTINUOUS:
            return SqlExpressionRenderResult(
                sql=f"quantile({percentile})({arg_rendered.sql})",
                bind_parameter_set=params,
            )
        elif node.percentile_args.function_type is SqlPercentileFunctionType.APPROXIMATE_DISCRETE:
            return SqlExpressionRenderResult(
                sql=f"quantile({percentile})({arg_rendered.sql})",
                bind_parameter_set=params,
            )
        else:
            assert_values_exhausted(node.percentile_args.function_type)


class ClickhouseSqlQueryPlanRenderer(DefaultSqlPlanRenderer):
    """Plan renderer for the ClickHouse engine."""

    EXPR_RENDERER = ClickhouseSqlExpressionRenderer()

    @property
    @override
    def expr_renderer(self) -> SqlExpressionRenderer:
        return self.EXPR_RENDERER
