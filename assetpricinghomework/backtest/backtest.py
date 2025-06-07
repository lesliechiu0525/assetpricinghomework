import time

import numpy as np
import polars as pl
import matplotlib.pyplot as plt
from loguru import logger

plt.rcParams["font.family"] = "SimHei"
plt.rcParams["font.serif"] = ["Times New Roman"]
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
    if col != "benchmark":
        e_avg,e_std = data["er"].mean(),data["er"].std()
        icir = e_avg/e_std * np.sqrt(252)
    else:
        icir = 0
    cr = data['cr'].to_list()[-2]
    ar = np.power(np.power(cr,1/len(data)),252)-1
    sr = avg/std * np.sqrt(252)
    logger.info(
        f"strategy:{strategy_name}, cr:{cr:.2%},date-count:{len(data)},ar:{ar:.2%},sr:{sr:.2f},mdd:{abs(data['dd'].min()):.2%},std:{std*np.sqrt(252):.2%},icir:{icir:.2f}"
    )


def vector_backtest(
        pred:str,
        kline:pl.DataFrame,
        strategy_name:str,
        index_rtn: pl.DataFrame,
        index_filter=True,
        num_symbol:int=100,
):
    t = time.time()
    start_date,end_date = kline['trade_date'].min(),kline['trade_date'].max()
    result = kline.sort(
        "trade_date"
    ).drop_nulls().group_by(
        "trade_date",
    ).agg(
        strategy=pl.col("y").sort_by(pred,descending=True).head(num_symbol).mean(),
        benchmark=(pl.col("y")*pl.col("total_mv")).sum()/pl.col("total_mv").sum(),# 总市值加权
    ).sort(
        "trade_date"
    ).with_columns(
        er=pl.col("strategy")-pl.col("benchmark"),
    ).join(
        index_rtn.with_columns(
            trade_date=pl.col("date").cast(pl.Date),
            index_rtn=pl.col("rtn")/100
        ).select(
            ["trade_date","index_rtn"]
        ),
        on="trade_date",
    )
    if index_filter:
        result = result.with_columns(
            benchmark=pl.col("index_rtn"),
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
    plt.plot(
        result["trade_date"],
        (result["er"]+1).cum_prod(),
        label="excess_return"
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
    # write data
    result.write_parquet("static/result.parquet")
    logger.success(
        f"vector backtest from {start_date} to {end_date}, time-cost:{time.time()-t:.2f}s"
    )


def loop_backtest(
        kline:pl.DataFrame,
        index_rtn:pl.DataFrame,
        strategy_name:str,
        first_step:dict,
        second_step:dict,
        third_step:dict,
        index_filter:bool=False,
):
    t = time.time()
    kline = kline.join(
        index_rtn.with_columns(
            trade_date=pl.col("date").cast(pl.Date),
            index_rtn=pl.col("rtn")/100
        ).select(
            ["trade_date","index_rtn"]
        ),
        on="trade_date",
    )
    start_date,end_date = kline['trade_date'].min(),kline['trade_date'].max()
    result = []
    pool = pl.DataFrame({})
    for date,cs in kline.group_by("trade_date",maintain_order=True):
        if pool.is_empty():
            pool = cs.sort(first_step["factor"],descending=first_step["descending"]).head(first_step["num_symbol"])
            pool = pool.sort(second_step["factor"], descending=second_step["descending"]).head(second_step["num_symbol"])
            pool = pool.sort(third_step["factor"], descending=third_step["descending"]).head(third_step["num_symbol"])
        else:
            if cs['trade_date'].dt.month().max() != pool['trade_date'].dt.month().max():
                # 定义为换仓日
                pool = cs.sort(first_step["factor"], descending=first_step["descending"]).head(first_step["num_symbol"])
                pool = pool.sort(second_step["factor"], descending=second_step["descending"]).head(second_step["num_symbol"])
                pool = pool.sort(third_step["factor"], descending=third_step["descending"]).head(third_step["num_symbol"])
            else:
                pass # 不换仓
        strategy_rtn = pool.select(
            pl.exclude("y")
        ).join(
            cs.select(
                ["ts_code","y"]
            ),
            on="ts_code",
            how="left",
        )['y'].mean()
        if index_filter:
            benchmark_rtn = cs['index_rtn'].mean()
        else:
            benchmark_rtn = cs['y'].mean()
        result.append(
            {
                "trade_date": date[0],
                "strategy": strategy_rtn,
                "benchmark": benchmark_rtn,
            }
        )
    result = pl.DataFrame(
        result
    ).sort(
        "trade_date"
    ).with_columns(
        er=pl.col("strategy")-pl.col("benchmark"),
    )
    # plot
    plt.plot(
        result["trade_date"],
        (result["strategy"]+1).cum_prod()-1,
        label="strategy"
    )
    plt.plot(
        result["trade_date"],
        (result["benchmark"]+1).cum_prod()-1,
        label="benchmark"
    )
    plt.plot(
        result["trade_date"],
        (result["er"]+1).cum_prod()-1,
        label="excess_return"
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
    # write data
    result.write_parquet("static/result.parquet")
    logger.success(
        f"multi-factors third-step  backtest from {start_date} to {end_date}, time-cost:{time.time()-t:.2f}s"
    )