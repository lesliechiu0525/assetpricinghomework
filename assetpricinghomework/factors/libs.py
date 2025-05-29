import polars as pl
factors_libs = {
    "momentum":pl.col("rtn").rolling_sum(252)-pl.col("rtn").rolling_sum(25*3),
    "reverse":pl.col("rtn").rolling_sum(25),
    "size":pl.col("circ_mv").log(),
    "turnover":pl.col("turnover_rate_f").rolling_mean(25),
    "bm":pl.when(
        pl.col("pb")>0
    ).then(
        1/pl.col("pb")
    ).otherwise(
        None
    ),
    "dividend":pl.col("dv_ratio"),
    "roe":pl.col("pb")/pl.col("pe"),
    "amihud":pl.col("rtn").abs()/pl.col("amount")
}