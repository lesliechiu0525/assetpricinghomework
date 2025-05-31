import warnings
warnings.filterwarnings('ignore')
import polars as pl

def data_join(
        kline:pl.DataFrame,
        join_data:str
):
    kline = kline.with_columns(
        Stkcd=pl.col("ts_code").str.split(".").first().list[0],
        year=pl.col("trade_date").dt.year(),
        month=pl.col("trade_date").dt.month(),
    )
    join_data = pl.read_csv(join_data, dtypes={"Stkcd": pl.Utf8}).with_columns(
        ann_date=pl.col("Accper").str.strptime(pl.Date, format="%Y/%m/%d") + pl.duration(days=90) # 延后90天
    ).with_columns(
        year=pl.col("ann_date").dt.year(),
        month=pl.col("ann_date").dt.month(),
    )
    data = kline.join(
        join_data,
        on=["Stkcd", "year","month"],
        how="left"
    )
    data = data.sort(
        "trade_date"
    ).with_columns(
        [
            pl.col(c).fill_null(strategy="forward").over("Stkcd")
            for c in data.columns
        ]
    )
    return data



if __name__ == '__main__':
    pass

