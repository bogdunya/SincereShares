import csv

import numpy as np
import pandas as pd
import requests
import plotly.graph_objects as go
import plotly.express as px
import datetime as dt
import os
from dateutil.relativedelta import relativedelta
import apimoex
import logging
import psycopg2
from sqlalchemy import create_engine
import tempfile
class MoexAPI:
    ISS_URL = 'https://iss.moex.com/iss/'

    @staticmethod
    def download_history_data(ticker, timeframe, start, end, columns, market='shares', engine='stock'):
        with requests.Session() as session:
            try:
                data = apimoex.get_market_candles(session, ticker, timeframe, start, end,
                                                  columns, market, engine)
            except Exception as e:
                data = []
                logging.exception(e)
        return data

    @classmethod
    def query(cls, request_url: str, arguments=None):
        if arguments is None:
            arguments = {}
        with requests.Session() as session:
            try:
                iss = apimoex.ISSClient(session, cls.ISS_URL + request_url, arguments)
                response = iss.get()
            except Exception as e:
                data = []
                logging.exception(e)
        return response


class FinTimeSeries:

    STD_COLUMNS = ('begin', 'open', 'high', 'low', 'close', 'volume')

    def __init__(self, ticker, timeframe, start, end):
        data = MoexAPI.download_history_data(ticker, timeframe, start, end, self.STD_COLUMNS)
        self.data = pd.DataFrame(data)
        self.ticker = ticker
        self.timeframe = timeframe
        self.start = start
        self.end = end

    @classmethod
    def from_trade_days(cls, ticker, num_last_days, timeframe=24, curr_date=dt.date.today(), include_today=False):
        start = curr_date - relativedelta(days=num_last_days*2)
        if include_today:
            end = curr_date
        else:
            yesterday = curr_date - dt.timedelta(days=1)
            end = yesterday
        year_tm = cls(ticker, 24, str(start), str(end))
        year_tm.data = year_tm.data.tail(num_last_days)
        year_tm.data = year_tm.data.reset_index(drop=True)
        if timeframe != 24:
            start = year_tm.data['begin'].iloc[0]
            end = cls.add_delta(year_tm.data['begin'].iloc[-1], delta_duration=1, delta_mode='d')
            year_tm = cls(ticker, timeframe, start, end)
        return year_tm

    @classmethod
    def from_weeks(cls, ticker, num_weeks, timeframe=24, curr_date=dt.date.today()):
        start = curr_date - dt.timedelta(weeks=num_weeks)
        end = curr_date
        return cls(ticker, timeframe, start, end)

    @classmethod
    def from_months(cls, ticker, num_months, timeframe=24, curr_date=dt.date.today()):
        start = curr_date - relativedelta(months=num_months)
        end = curr_date
        return cls(ticker, timeframe, start, end)

    @classmethod
    def from_years(cls, ticker, num_years, timeframe=24, curr_date=dt.date.today()):
        start = curr_date - relativedelta(years=num_years)
        end = curr_date
        return cls(ticker, timeframe, start, end)

    @classmethod
    def from_last(cls, ticker, period_type, period_num, timeframe, curr_date=dt.date.today()):
        if period_type == 'd':
            return cls.from_trade_days(ticker, period_num, timeframe, curr_date)
        elif period_type == 'w':
            return cls.from_weeks(ticker, period_num, timeframe, curr_date)
        elif period_type == 'm':
            return cls.from_months(ticker, period_num, timeframe, curr_date)
        elif period_type == 'y':
            return cls.from_years(ticker, period_num, timeframe, curr_date)
        else:
            return cls.from_trade_days(ticker, period_num, timeframe, curr_date)

    def column(self, name):
        return self.data[name]

    def columns(self, column_names):
        return self.data[column_names]

    def candle_chart(self, without_slider=True):
        fig = go.Figure(data=[go.Candlestick(x=self.data['begin'],
                                             open=self.data['open'],
                                             high=self.data['high'],
                                             low=self.data['low'],
                                             close=self.data['close'])])
        if without_slider:
            fig.update_layout(xaxis_rangeslider_visible=False)
        fig.show()

    def line_chart(self, column='close', with_slider=False):
        fig = px.line(self.data, x='begin', y=column)
        if with_slider:
            fig.update_xaxes(rangeslider_visible=True)
        fig.show()

    def mean(self, column='close'):
        return self.data[column].mean()

    def var(self, column='close'):
        return self.data[column].var()

    def median(self, column='close'):
        return self.data[column].median()

    def std(self, column='close'):
        return self.data[column].std()

    def corr(self, columns=STD_COLUMNS):
        if len(columns) == 2:
            df_column1 = self.data[columns[0]]
            df_column2 = self.data[columns[1]]
            return df_column1.corr(df_column2)
        elif len(columns) > 2:
            df = self.data[list(columns)]
            return df.corr()
        else:
            return None

    def export_csv(self):
        project_dir = os.path.dirname(os.path.dirname(__file__))
        filename = "out_"+dt.datetime.now().strftime("%d.%m.%Y_%H.%M.%S")+'.csv'
        path = os.path.join('F:\myProjects\proMOEX\VideosSources\output.csv')
        self.data.to_csv(path, index=False)
        return path

    def export_xlsx(self):
        project_dir = os.path.dirname(os.path.dirname(__file__))
        filename = "out_"+dt.datetime.now().strftime("%d.%m.%Y_%H.%M.%S")+'.xlsx'
        path = os.path.join(project_dir, 'data', 'export', filename)
        self.data.to_excel(path, index=False)
        return path

    def has_nulls(self):
        return self.nulls_count() > 0

    def nulls_count(self, by_columns=False):
        if by_columns:
            return dict(self.data.isna().sum())
        else:
            return self.data.isna().sum().sum()

    def fill_na(self, method='bfill'):
        self.data.fillna(method=method, inplace=True)

    def drop_na(self):
        self.data.dropna(inplace=True)

    def is_empty(self):
        return self.data.empty

    @staticmethod
    def add_delta(date: str, delta_duration, delta_mode='d', sub=False):
        if delta_mode == 'd':
            delta = dt.timedelta(days=delta_duration)
        elif delta_mode == 'w':
            delta = dt.timedelta(weeks=delta_duration)
        elif delta_mode == 'm':
            delta = relativedelta(months=delta_duration)
        elif delta_mode == 'y':
            delta = relativedelta(years=delta_duration)
        else:
            delta = dt.timedelta(days=delta_duration)
        dt_date = dt.date.fromisoformat(date.split(' ')[0])
        if not sub:
            date = dt_date + delta
        else:
            date = dt_date - delta
        return str(date)


    @staticmethod
    def transform_timeframe_for_api(timeframe):
        if timeframe == 'd':
            return 24
        elif timeframe == 'w':
            return 7
        elif timeframe == 'm':
            return 31
        elif timeframe == 'h':
            return 60
        elif timeframe == '10min':
            return 10
        elif timeframe == '1min':
            return 1
        else:
            return 24

    def __str__(self):
        return self.data.to_string()

    def __len__(self):
        return len(self.data['begin'])
def main():
    ts = FinTimeSeries('SBER', 24, '2022.05.23', '2023.05.22')
    ts.export_csv();
    filename = 'F:\myProjects\proMOEX\VideosSources\output.csv'
    df = pd.read_csv('F:\myProjects\proMOEX\VideosSources\output.csv')
    df.rename(columns={'begin': 'date'}, inplace=True)
    df.rename(columns={'close': 'price'}, inplace=True)

    # Укажите значения для колонок id, currency и share_id
    id_value = [i + 1 for i in range(len(df))]  # Пример: порядковый номер строки
    currency_value = "RUB"  # Пример: фиксированное значение "RUB"
    share_id_value = 1  # Пример: фиксированное значение 1
    # Добавьте колонки id, currency и share_id в DataFrame
    df["id"] = id_value
    df["currency"] = currency_value
    df["share_id"] = share_id_value
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    # Преобразование столбца в тип datetime
    df['date'] = pd.to_datetime(df['date'])

    # Преобразование в тип timestamptz с часовым поясом UTC
    df['date'] = df['date'].dt.tz_localize('UTC')
    # Вывести полностью DataFrame
    print(df)
    data_types = df.dtypes
    print(data_types)

    # Строка подключения
    connection_string = "postgresql://bogdanSh:v2_44cmT_dDbP4KK5RRLzky9Hhvvhn4Q@db.bit.io:5432/nadejdakna2002/trial"
    # Подключение к базе данных
    conn = psycopg2.connect(connection_string)

    # Создайте объект Engine для использования метода to_sql()
    engine = create_engine('postgresql://bogdanSh:v2_44cmT_dDbP4KK5RRLzky9Hhvvhn4Q@db.bit.io:5432/nadejdakna2002/trial')

    # Укажите имя целевой таблицы
    table_name = "db_price_new"

    # Сохраните данные из DataFrame в базу данных
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

    # Закройте соединение с базой данных
    conn.close()









    # Закрытие курсора и соединения
    # cur.close()
    # conn.close()
    # Список бумаг торгуемых на московской бирже
    # https://iss.moex.com/iss/reference/5
    # j = query("securities")
    # j = query("securities", q="SBER")
    # j = query("securities", group_by="type", group_by_filter="corporate_bond", limit=10)
    # j = query("securities", q="SBER", group_by="type", group_by_filter="corporate_bond", limit=10)
    # f = flatten(j, 'securities')

    # Спецификация инструмента
    # https://iss.moex.com/iss/reference/13
    # secid = 'RU000A102QJ7'
    # method = "securities/%s" % secid
    # j = query(method)
    # f = flatten(j, 'description')

    # Купоны по облигациям
    # ** описания нет
    # secid = 'RU000A102QJ7'
    # method = "securities/%s/bondization" % secid
    # j = query(method)
    # f = flatten(j, 'coupons')

    # Дивиденды по акциям
    # ** описания нет
    # secid = 'MTSS'
    # method = "securities/%s/dividends" % secid
    # j = query(method)
    # f = flatten(j, 'dividends')

    # print(pd.DataFrame(f))
    # print(pd.DataFrame(f, columns=['secid','shortname' ,'primary_boardid', 'type']))
    # print(json.dumps(j, ensure_ascii=False, indent=4, sort_keys=True))


if __name__ == '__main__':
    main()




