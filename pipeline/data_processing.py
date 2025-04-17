
from pipeline.logger import get_logger
from pipeline.transformations import select, filter_data, add_column, rename_column, perform_agg, join
from pipeline.actions import count, display, write
from pipeline.transformations_class import Transformations
from pipeline.actions_class import Actions

class dataProcessing(Transformations, Actions):

    def __init__(self, path):
        self.logger = get_logger("DataProcessingLogger")
        self.logger.info("Initializing dataProcessing pipeline")

        self.apply_runtime_optimizations()

        Transformations.__init__(self, path, self.logger)
        Actions.__init__(self, self.df1, self.logger)

        self.TRANSFORMATIONS = {
            "select": select,
            "filter": filter_data,
            "add_column": add_column,
            "rename_column": rename_column,
            "agg": perform_agg,
            "join": join
        }

        self.ACTIONS = {
            "count": count,
            "display": display,
            "write": write
        }

        self.logger.info("dataProcessing pipeline is ready")

    def apply_runtime_optimizations(
        self,
        broadcast_threshold="10MB",
        advisory_partition_size=134217728,
        max_partition_bytes=134217728,
        delta_target_file_size=134217728
    ):
        self.logger.info("⚙️ Applying Spark runtime optimizations with user-defined settings...")

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", broadcast_threshold)
        spark.conf.set("spark.sql.shuffle.partitions", "auto")
        spark.conf.set("spark.sql.files.maxPartitionBytes", max_partition_bytes)

        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", advisory_partition_size)
        spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")

        spark.conf.set("delta.autoOptimize.autoCompact", "true")
        spark.conf.set("delta.autoOptimize.optimizeWrite", "true")
        spark.conf.set("delta.targetFileSize", delta_target_file_size)
        spark.conf.set("delta.tuneFileSizesForRewrite", "true")


        self.logger.info("Runtime optimizations applied.")
        return self

    def apply_config(self, config):
        for step in config.get("transformations", []):
            op = step["operation"]
            args = step.get("args", [])
            if op in self.TRANSFORMATIONS:
                self.logger.info(f"🔧 Applying transformation: {op}")
                self.TRANSFORMATIONS[op](self, args)
            else:
                self.logger.warning(f"Unknown transformation: {op}")

        for action in config.get("actions", []):
            op = action["operation"]
            args = action.get("args", [])
            if op in self.ACTIONS:
                self.logger.info(f"Executing action: {op}")
                self.ACTIONS[op](self, args)
            else:
                self.logger.warning(f"⚠️ Unknown action: {op}")
        return self
