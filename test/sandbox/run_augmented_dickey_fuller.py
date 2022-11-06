import yaml
import logging.config
from statsmodels.tsa.stattools import adfuller
import plotly.express as px
import plotly

from utils.file_handler import FileHandler
from utils.datagen import FakeDataGen


if __name__ == "__main__":
    with open('../../discovery/logging_conf.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)

    file_handler = FileHandler()
    fake_data = FakeDataGen()

    fake_files = fake_data.build_df_to_file(100, path="output/run_dataframe_matcher", index_type="counter",
                                            continuous_data=1, categoric_data=0, file_spread=1)
    file_handler.load_file(fake_files[0])

    df = file_handler.loaded_files[fake_files[0]]

    for column in df.columns:
        series = df.loc[:, column]

        adft = adfuller(series)

        logger.debug(f"\n{column}:\n"
                     f"ADF statistic: {adft[0]}\n"
                     f"p-value: {adft[1]}\n"
                     f"Critical values:\n"
                     f"\t1%: {adft[4]['1%']}\n"
                     f"\t5%: {adft[4]['5%']}\n"
                     f"\t10%: {adft[4]['10%']}")

        # We accept the null hypothesis that the series is non-stationary if the p-value is higher than 5%
        # (if the ADF statistic is higher than the critical value at 5%, we can also accept the null hypothesis)
        if adft[1] > 0.05:
            logger.debug(f"Accept null-hyp: '{column}' is non-stationary")
        else:
            logger.debug(f"Reject null-hyp: '{column}' is stationary")

    fig = px.line(df, title='Whatever')
    plotly.offline.plot(fig)
