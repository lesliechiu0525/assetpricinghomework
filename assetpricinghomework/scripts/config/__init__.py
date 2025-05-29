import polars as pl
token = "your_token"
factors_config = [
    "momentum",
    "reverse",
    "size",
    "turnover",
    # "dividend",
    "amihud"
]
index_filter = False # 如果这里写False则默认不适用中证800