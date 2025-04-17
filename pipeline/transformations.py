
def select(obj, args): return obj.selectcolumns(*args)
def filter_data(obj, args): return obj.filter_data(*args)
def add_column(obj, args): return obj.add_column(*args)
def rename_column(obj, args): return obj.column_rename(*args)
def perform_agg(obj, args): return obj.perform_agg(*args)
def join(obj, args): return obj.apply_join(*args)
