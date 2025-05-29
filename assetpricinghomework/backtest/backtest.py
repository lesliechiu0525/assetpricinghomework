import time

import numpy as np
import polars as pl
import matplotlib.pyplot as plt
from loguru import logger
from pandas.core.computation.common import result_type_many
from sqlalchemy.dialects.postgresql import plainto_tsquery

plt.rcParams["font.family"] = "SimHei"
plt.rcParams["font.sans-serif"] = ["Arial"]
plt.rcParams["axes.unicode_minus"] = False
plt.style.use('seaborn-v0_8')

def rtn_analysis(
        data:pl.DataFrame,
        col:str,
        strategy_name:str,
):
    data = data.with_columns(
        cr=(pl.col(col)+1).cum_prod()
    ).with_columns(
        dd=pl.col("cr")/pl.col("cr").cum_max()-1
    )
    avg,std = data[col].mean(),data[col].std()
    ar = avg * 252
    sr = avg/std * np.sqrt(252)
    logger.info(
        f"strategy:{strategy_name}, ar:{ar:.2%}, sr:{sr:.2f},mdd:{abs(data['dd'].min()):.2%},std:{std:.2%}"
    )


def vector_backtest(
        pred:str,
        kline:pl.DataFrame,
        strategy_name:str,
        num_symbol:int=100,
):
    t = time.time()
    start_date,end_date = kline['trade_date'].min(),kline['trade_date'].max()
    result = kline.sort(
        "trade_date"
    ).with_columns(
        y=(pl.col("close").shift(-1)/pl.col("close")-1).over("ts_code")
    ).group_by(
        "trade_date",
    ).agg(
        strategy=pl.col("y").sort_by(pred,descending=True).head(num_symbol).mean(),
        benchmark=pl.col("y").mean()
    ).sort(
        "trade_date"
    )
    # plot
    plt.plot(
        result["trade_date"],
        (result["strategy"]+1).cum_prod(),
        label="strategy"
    )
    plt.plot(
        result["trade_date"],
        (result["benchmark"]+1).cum_prod(),
        label="benchmark"
    )
    plt.ylabel(
        "cumulative return"
    )
    plt.xlabel(
        "date"
    )
    plt.legend()
    plt.title(
        f"{strategy_name} strategy performance"
    )
    plt.savefig(f"strategy_return_performance")
    # analysis
    rtn_analysis(
        data=result,
        col="strategy",
        strategy_name=strategy_name,
    )
    rtn_analysis(
        data=result,
        col="benchmark",
        strategy_name="benchmark",
    )
    logger.success(
        f"vector backtest from {start_date} to {end_date}, time-cost:{time.time()-t:.2f}s"
    )