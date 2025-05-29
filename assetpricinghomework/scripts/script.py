import warnings
warnings.filterwarnings('ignore')
import polars as pl
from assetpricinghomework.factors.factors_api import Factors


if __name__ == "__main__":
    from assetpricinghomework.scripts.config import (
        factors_config
    )
    fa = Factors(
        config=factors_config
    )
    # load kline & basic
    kline = pl.read_parquet("static/kline.parquet").join(
        pl.read_parquet("static/basic.parquet"),
        on=["ts_code","trade_date"]
    ).with_columns(
        trade_date=pl.col("trade_date").str.to_date(format="%Y%m%d"),
    ).sort(
        "trade_date",
    )
    # calculate factors
    factors = fa.factor_calculate(
        kline=kline,
    )
    check = factors.filter(
        pl.col("trade_date")==pl.col("trade_date").max()
    ).to_pandas()
    print()
