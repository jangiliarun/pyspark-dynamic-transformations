from pyspark.sql.functions import *
from functools import reduce

class Transformations:
    def __init__(self, source, logger=None):
        self.logger = logger
        if isinstance(source, str):
            if self.logger:
                self.logger.info(f"Reading data from: {source}")
            self.df1 = spark.read.csv(source, header=True, inferSchema=True)
        else:
            if self.logger:
                self.logger.info(f"Initializing with existing DataFrame")
            self.df1 = source

    def list_columns(self):
        if self.logger:
            self.logger.info("Listing available columns")
        print("Available columns:", self.df1.columns)
        return self

    def selectcolumns(self, *cols):
        if isinstance(cols, str):
            cols = [cols]
        self.df1 = self.df1.select(*cols)
        if self.logger:
            self.logger.info(f" Selected columns: {cols}")
        return self

    def filter_data(self, column, operation, value):
        op_map = {
            "==": lambda c, v: col(c) == v,
            "!=": lambda c, v: col(c) != v,
            ">": lambda c, v: col(c) > v,
            "<": lambda c, v: col(c) < v,
            ">=": lambda c, v: col(c) >= v,
            "<=": lambda c, v: col(c) <= v,
            "in": lambda c, v: col(c).isin(v if isinstance(v, list) else [v]),
            "not in": lambda c, v: ~col(c).isin(v if isinstance(v, list) else [v]),
            "is null": lambda c, _: col(c).isNull(),
            "is not null": lambda c, _: col(c).isNotNull(),
            "contains": lambda c, v: col(c).contains(v)
        }

        if operation not in op_map:
            raise ValueError(f"Unsupported operation: {operation}")
        if column not in self.df1.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")

        self.df1 = self.df1.filter(op_map[operation](column, value))
        if self.logger:
            self.logger.info(f"Filtered where {column} {operation} {value}")
        return self

    def add_column(self, col_name, expression):
        self.df1 = self.df1.withColumn(col_name, expr(expression))
        if self.logger:
            self.logger.info(f" Added column: {col_name} using {expression}")
        return self

    def column_rename(self, old_name, new_name):
        if old_name not in self.df1.columns:
            raise ValueError(f"Column '{old_name}' not found.")
        self.df1 = self.df1.withColumnRenamed(old_name, new_name)
        if self.logger:
            self.logger.info(f"Renamed column: {old_name} → {new_name}")
        return self

    def apply_join(self, join_df, join_cols, join_type="inner"):
        join_condition = reduce(
            lambda x, y: x & y,
            [self.df1[c] == join_df[c] for c in join_cols]
        )
        joined_df = self.df1.join(join_df, join_condition, join_type)
        if self.logger:
            self.logger.info(f" Applied {join_type} join on {join_cols}")
        return Transformations(joined_df, logger=self.logger)

    def perform_agg(self, group_by_cols, action, action_column="*"):
        if isinstance(group_by_cols, str):
            group_by_cols = [group_by_cols]
        agg_df = self.df1.groupBy(*group_by_cols).agg(action(action_column))
        if self.logger:
            self.logger.info(f"Aggregated using {action.__name__} on {action_column}, grouped by {group_by_cols}")
        return Transformations(agg_df, logger=self.logger)



