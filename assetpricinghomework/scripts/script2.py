import warnings
warnings.filterwarnings("ignore")
import polars as pl
import matplotlib.pyplot as plt
from loguru import logger

plt.rcParams["font.family"] = "SimHei"
plt.rcParams["font.serif"] = ["Times New Roman"]
plt.rcParams["axes.unicode_minus"] = False
plt.style.use('seaborn-v0_8')
from assetpricinghomework.backtest.backtest import rtn_analysis


def analysis(
        strategy:list,
        strategy_name:str,
):
    logger.info(
        f"{len(strategy)} bagging backtest, strategy_name:{strategy_name} "
    )
    result = strategy[0].rename(
        {
            "strategy":"strategy_0"
        }
    ).with_columns(
        rtn=pl.col("strategy_0")
    )
    for i in range(1,len(strategy)):
        result = result.join(
            strategy[i].select(
                ["trade_date","strategy"]
            ).rename(
                {
                    "strategy":f"strategy_{i}"
                }
            ),
            on=['trade_date']
        ).with_columns(
            rtn=pl.col("rtn")+pl.col(f"strategy_{i}")
        )
    result = result.with_columns(
        rtn=pl.col("rtn")/len(strategy)
    ).sort(
        "trade_date"
    ).with_columns(
        er=pl.col("rtn")-pl.col("benchmark"),
    )
    # plot
    plt.plot(
        result["trade_date"],
        result["rtn"].cum_sum(),
        label="rtn"
    )
    plt.plot(
        result["trade_date"],
        result["benchmark"].cum_sum(),
        label="benchmark"
    )
    plt.plot(
        result["trade_date"],
        result["er"].cum_sum(),
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
    plt.savefig(f"bagging_strategy_return_performance")
    # analysis
    rtn_analysis(
        data=result,
        col="rtn",
        strategy_name=strategy_name,
    )
    rtn_analysis(
        data=result,
        col="benchmark",
        strategy_name="benchmark",
    )

if __name__ == "__main__":
    strategy_0 = pl.read_parquet("static/result0.parquet")
    strategy_1 = pl.read_parquet("static/result.parquet")
    strategy = [strategy_0, strategy_1] # 这里需要等权重合并的策略收益率 放进来
    analysis(strategy=strategy,strategy_name="big&small") # 示例：大小盘策略收益率合并
