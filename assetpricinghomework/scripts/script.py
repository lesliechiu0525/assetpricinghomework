import warnings
warnings.filterwarnings('ignore')
import polars as pl
from assetpricinghomework.factors.factors_api import Factors
from assetpricinghomework.backtest.backtest import vector_backtest
import tushare as ts


def kline_process(
        kline_loc:str,
        index_loc:str,
        basic_loc:str,
        index_filter:bool
):
    # load kline & basic
    stock_cols = [f"{i}" for i in range(800)]
    index = pl.read_parquet(index_loc).with_columns(
        trade_date=pl.col("date").cast(pl.Date)
    ).melt(
        id_vars="trade_date",
        value_vars=stock_cols,
        variable_name="ts_code",
    ).select(
        ["trade_date", "value"],
    ).rename(
        {
            "value": "ts_code"
        }
    )
    kline = pl.read_parquet(kline_loc).with_columns(
        trade_date=pl.col("trade_date").str.to_date(format="%Y%m%d"),
    )
    if index_filter:
        pass
    else:
        kline = kline.join(
            index,
            on=["trade_date","ts_code"],
        )
    kline = pl.read_parquet(basic_loc).with_columns(
        trade_date=pl.col("trade_date").str.to_date(format="%Y%m%d"),
    ).join(
        kline,
        on=["trade_date", "ts_code"]
    ).sort(
        "trade_date",
    )
    # ffill
    kline = kline.with_columns(
        [
            pl.col(c).fill_null(strategy="forward").over("ts_code") for c in kline.columns
        ]
    )
    return kline

if __name__ == "__main__":
    from assetpricinghomework.scripts.config import (
        factors_config, index_filter
)
    fa = Factors(
        config=factors_config
    )

    # 生成需要的kline数据
    kline = kline_process(
        kline_loc="static/kline.parquet", # 要确认把数据正确放在了static文件夹里面
        index_loc="static/index.parquet",
        basic_loc="static/basic.parquet",
        index_filter=index_filter
    )

    # calculate factors
    factors,factors_name = fa.factor_calculate(
        kline=kline,
    )
    # uss pred
    pred = factors.with_columns(
        pred=pl.col("amihud")+pl.col("size")*-1
    )
    # backtest & analysis
    vector_backtest(
        pred="pred",
        kline=pred,
        num_symbol=100,
        strategy_name="amihud"
    )
