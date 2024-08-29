from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from plotly import express


class TopNPlotBase:
    def get_top_n(self, sdf, max_rows):
        sdf_top_n = sdf.limit(max_rows + 1)
        pdf = sdf_top_n.toPandas()

        if len(pdf) > max_rows:
            pdf = pdf.iloc[:max_rows]

        return pdf

class DataFramePlotAccessor:
    def __init__(self, df):
        self.df = df

    def pie(self, names_column, values_column=None, max_rows=1000, **kwargs):
        top_n_plotter = TopNPlotBase()

        if values_column is None:
            sdf = self.df.groupBy(names_column).agg(F.count("*").alias("values"))
            values_column = "values"
        else:
            sdf = self.df

        pdf = top_n_plotter.get_top_n(sdf.select(names_column, values_column), max_rows)
        fig = express.pie(pdf, values=values_column, names=names_column, **kwargs)
        return fig

def plot_accessor(df):
    return DataFramePlotAccessor(df)

# need to import pyspark.sql.plot.prototype to enable plotting
DataFrame.plot = property(plot_accessor)