class Actions:
    def __init__(self, df):
        self.df = df

    def display_data(self, rows=5):
        self.df.display(rows)
        return self

    def count(self):
        result = self.df.count()
        print(f"Row count: {result}")
        return result

    def write(self, path, op_format="parquet", mode="overwrite", partitionBy=None):
        writer = self.df.write.format(op_format).mode(mode)
        if partitionBy:
            writer = writer.partitionBy(*partitionBy)
        writer.save(path)
        return self
