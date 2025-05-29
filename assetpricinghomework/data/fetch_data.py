import tushare as ts
from datetime import datetime,timedelta
from loguru import logger
from tqdm import tqdm
import polars as pl
import time
class Download:
    def __init__(
            self,
            start_date,
            end_date,
            rate_limit,# 每分钟限制多少次
            output_loc, # 存储地址
    ):
        self.start_date = start_date
        self.end_date = end_date
        start,end =  datetime.strptime(self.start_date, '%Y%m%d'), datetime.strptime(self.end_date, '%Y%m%d')
        self.query = [(start + timedelta(days=x)).strftime('%Y%m%d') for x in range((end - start).days + 1)]
        self.rate_limit = rate_limit
        self.output_loc = output_loc
        self.result = []
    def download(self,api):
        t = time.time()
        # 换算每请求一次需要间隔多少秒
        interval = 60 / self.rate_limit + 0.05 # 留50ms的空余空间避免限流
        logger.info(
            f"start download, from:{self.start_date}, to:{self.end_date}"
        )
        download_proces = tqdm(self.query,desc="download process")
        for i,query in enumerate(download_proces):
            df = api(trade_date=query)
            df = pl.from_pandas(
                    df
            )
            if df.is_empty():
                pass
            else:
                self.result.append(
                    df
                )
            download_proces.set_postfix(
                {
                    "date":query,
                    "num_symbol":len(df),
                    "process":f"{i+1}/{len(self.query)}"
                }
            )
            time.sleep(interval)
        pl.concat(self.result).write_parquet(f"{self.output_loc}.parquet")
        logger.success(f"download process finish, output-loc:{self.output_loc}.parquet, time-cost:{time.time() - t:.2f}s")
if __name__ == "__main__":
    from assetpricinghomework.scripts.config import (
        token
    )
    # 配置tushare接口
    ts.set_token(token)
    pro = ts.pro_api()
    api = pro.daily_basic
    dl = Download(
        rate_limit=500,
        start_date="20170401",
        end_date="20250529",
        output_loc="static/basic",
    )
    dl.download(api)