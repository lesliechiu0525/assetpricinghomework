import warnings

warnings.filterwarnings('ignore')
import polars as pl
from assetpricinghomework.factors.libs import factors_libs

class Factors:
    def __init__(
            self,
            config
    ):
        self.libs = factors_libs
        self.time = "trade_date"
        self.symbol = "ts_code"
        self.func_list = [
            self.libs[key].over(self.symbol).alias(key) for key in config
        ] + [

        ]
        self.factor_name = [
            key for key in config
        ]
    def _winsorize(
            self,
            factors:pl.DataFrame,
            n=3,
    ):
        # 3 simga 去极值
        return factors.with_columns(
            [
                (
                        pl.col(c).clip(
                            upper_bound=(
                                            pl.col(c).mean()+pl.col(c).std()*n
                            ),
                            lower_bound=(
                                    pl.col(c).mean()-pl.col(c).std()*n
                            )
                        ).over(
                            self.time
                        )
                )
                for c in self.factor_name
            ]
        )
    def _zscore(
            self,
            factors:pl.DataFrame
    ):
        return factors.with_columns(
            [
                (
                        pl.col(c)-pl.col(c).mean().over(self.time)
                )/pl.col(c).std().over(self.time)
                for c in self.factor_name
            ]
        )

    def factor_calculate(
            self,
            kline:pl.DataFrame
    ):
        if "rtn" in kline.columns:
            pass
        else:
            kline = kline.with_columns(
                rtn=(pl.col("close")/pl.col("close").shift(1)-1).over(self.symbol)
            ).drop_nulls()
        factors = kline.sort(
            self.time
        ).with_columns(
            self.func_list
        ).drop_nulls()
        factors = self._winsorize(factors)
        factors = self._zscore(factors)
        return factors