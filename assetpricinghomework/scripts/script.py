import warnings
warnings.filterwarnings('ignore')
import polars as pl
from assetpricinghomework.factors.factors_api import Factors
from assetpricinghomework.backtest.backtest import vector_backtest
import tushare as ts


if __name__ == "__main__":
    from assetpricinghomework.scripts.config import (
        factors_config
    )
    fa = Factors(
        config=factors_config
    )
    # load kline & basic
    stock_cols = [f"{i}" for i in range(800)]
    index = pl.read_parquet("static/index.parquet").with_columns(
        trade_date=pl.col("date").cast(pl.Date)
    ).melt(
        id_vars="trade_date",
        value_vars=stock_cols,
        variable_name="ts_code",
    ).select(
        ["trade_date", "value"],
    ).rename(
        {
            "value":"ts_code"
        }
    )
    kline = pl.read_parquet("static/kline.parquet").with_columns(
        trade_date=pl.col("trade_date").str.to_date(format="%Y%m%d"),
    ).join(
        index,
        on=["trade_date","ts_code"]
    ).sort(
        "trade_date",
    )
    kline = pl.read_parquet("static/basic.parquet").with_columns(
        trade_date=pl.col("trade_date").str.to_date(format="%Y%m%d"),
    ).join(
        kline,
        on=["trade_date","ts_code"]
    ).sort(
        "trade_date",
    )
    # ffill
    kline = kline.with_columns(
        [
            pl.col(c).fill_null(strategy="forward").over("ts_code") for c in kline.columns
        ]
    )
    check = kline.filter(
        pl.col("trade_date") == pl.col("trade_date").max()
    ).to_pandas()
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
        num_symbol=100,
        strategy_name="size"
    )
