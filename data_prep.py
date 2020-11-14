import pandas as pd
import numpy as np

data_path = 'data/'

sales = pd.read_parquet(data_path + 'sales.parquet')
shops = pd.read_parquet(data_path + 'shops.parquet')

# Приводим данные от длинного формата к широкому
sales_pivoted = pd.pivot_table(sales[['date', 'shop_id', 'goods_type', 'total_items_sold']],
                               index = ['date', 'shop_id'],
                               columns = 'goods_type')
# Исправляем название колонок и индексы
sales_pivoted.columns = sales_pivoted.columns.get_level_values(1)
sales_pivoted = sales_pivoted.reset_index()
sales_pivoted.columns.name = ''
# Добавляем информацию о количестве прилавков
sales = sales[['date', 'shop_id', 'number_of_counters']].drop_duplicates(['date', 'shop_id']).reset_index(drop = True)
sales_pivoted = pd.merge(sales_pivoted, sales, on = ['date', 'shop_id'])
# Ограничиваем данные о продажах этим промежутком
# В этот промежуток есть информация о продажах всех товаров
sales_pivoted = sales_pivoted[(sales_pivoted['date']>=pd.to_datetime('2146-01-01 00:00:00'))\
                              & (sales_pivoted['date']<=pd.to_datetime('2147-11-30 00:00:00'))]
def filling_gaps(x):

    ''' Возвращает датасет с заполненными датами '''

    dates = list(pd.date_range('2146-01-01 00:00:00', '2147-11-30 00:00:00'))
    shop_id = x['shop_id'].unique()[0]
    dates = pd.DataFrame({'date': dates, 'shop_id': shop_id})
    x = pd.merge(x, dates, on = ['date', 'shop_id'], how = 'right')

    return x

sales_pivoted = sales_pivoted.groupby('shop_id').apply(filling_gaps).reset_index(drop = True)
sales_pivoted = sales_pivoted.sort_values(['shop_id', 'date'])

sales_pivoted.to_csv('sales_pivoted.csv', index = False)