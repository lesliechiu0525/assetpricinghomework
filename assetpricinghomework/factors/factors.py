import warnings

from statsmodels.sandbox.regression.ols_anova_original import factors

warnings.filterwarnings('ignore')
import polars as pl
from assetpricinghomework.factors.libs import libs

class Factors:
    def __init__(
            self,
            config
    ):
        self.libs = libs
        self.time = "trade_date"
        self.symbol = "ts_code"
        self.func_list = [
            self.libs[key] for key in config
        ]
        self.factor_name = []
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
                            ).over(self.time),
                            lower_bound=(
                                    pl.col(c).mean()-pl.col(c).std()*n
                            ).over(self.time)
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
        factors = kline.sort(
            self.time
        ).group_by(
            self.symbol
        ).agg(
            self.func_list
        )
        return factors