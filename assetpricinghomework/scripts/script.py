import warnings
warnings.filterwarnings('ignore')
import polars as pl
from assetpricinghomework.factors.factors_api import Factors
from assetpricinghomework.backtest.backtest import vector_backtest


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
    factors,factors_name = fa.factor_calculate(
        kline=kline,
    )
    # uss pred
    pred = factors.with_columns(
        pred=pl.col("size")*-1
    )
    # backtest & analysis
    vector_backtest(
        pred="pred",
        kline=pred,
        num_symbol=300,
        strategy_name="small_size"
    )
