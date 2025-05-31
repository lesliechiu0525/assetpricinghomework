import polars as pl
token = "your_token"
factors_config = [
    "momentum",
    "reverse",
    "size",
    "turnover",
    "dividend",
    "amihud",
    "bm", # 默认这里需要加入bm价值因子和roe因子
    "roe",
    "test", # 配置了一个测试因子
]
index_filter = False # 如果这里写False则默认不适用中证800