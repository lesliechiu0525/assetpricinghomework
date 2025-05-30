import warnings
warnings.filterwarnings('ignore')
import polars as pl
from assetpricinghomework.factors.factors_api import Factors
from assetpricinghomework.backtest.backtest import vector_backtest
import tushare as ts
from loguru import logger


def kline_process(
        kline_loc:str,
        index_loc:str,
        basic_loc:str,
        index_filter:bool,
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

def filter_pool(
        mode:str,
        factors:pl.DataFrame
):
    # 复刻第五组的初步筛选
    factors = factors.sort(
        "trade_date",
    ).with_columns(
        y=(pl.col("close").shift(-1) / pl.col("close") - 1).over("ts_code")
    )
    if mode == "big":
        factors = factors.filter(
            pl.col("turnover_rate_f") > pl.col("turnover_rate").quantile(0.2).over("trade_date")
        ).with_columns(
            mv_rank=pl.col("total_mv").rank(descending=True).over("trade_date")
        ).filter(
            pl.col("mv_rank")<=300 # 这里使用的是300 也可以修改成100
        )
        logger.info(f"大盘股池")
    if mode == "small":
        factors = factors.filter(
            pl.col("turnover_rate_f") > pl.col("turnover_rate").quantile(0.2).over("trade_date")
        ).with_columns(
            mv_rank=pl.col("total_mv").rank(descending=False).over("trade_date")
        ).filter(
            pl.col("mv_rank") <= 300
        )
        logger.info(f"小盘股池")
    if mode == "value":
        factors = factors.filter(
            pl.col("turnover_rate_f") > pl.col("turnover_rate").quantile(0.2).over("trade_date")
        ).with_columns(
            value_rank=pl.col("bm").rank(descending=True).over("trade_date")
        ).filter(
            pl.col("value_rank") <= 300
        )
        logger.info(f"价值股池")
    if mode == "growth":
        factors = factors.filter(
            pl.col("turnover_rate_f") > pl.col("turnover_rate").quantile(0.2).over("trade_date")
        ).sort(
            "trade_date",
        ).with_columns(
            growth=pl.when(
                pl.col("roe")!=pl.col("roe").shift(1), # 使用roe的增速代表成长
            ).then(
                pl.col("roe")/pl.col("roe").shift(-1)-1
            ).otherwise(
                None
            ).fill_null(
                strategy="forward"
            ).over(
                "ts_code",
            )
        ).with_columns(
            growth_rank=pl.col("growth").rank(descending=False).over("trade_date")
        ).filter(
            pl.col("growth_rank") <= 300
        )
        logger.info(f"成长股池")
    else:
        pass
    return factors

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
    # 可以使用第五组的大小盘初步股池
    factors = filter_pool(
        mode="growth",
        factors=factors
    )
    # use pred
    pred = factors.with_columns(
        pred=pl.col("amihud")
    )
    # backtest & analysis
    vector_backtest(
        pred="pred",
        kline=pred,
        num_symbol=100,
        strategy_name="amihud"
    )
