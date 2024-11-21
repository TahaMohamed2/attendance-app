from attendance import  *
from att_flask import *
import pandas as pd
from datetime import datetime, timedelta

def valid_dates (df) :
    # Check if the dates are valid
    att = df[['Date', 'ID', 'Hour','WeekNumber','Week_day']].copy()
    
    att = att.drop_duplicates(subset=['WeekNumber', 'ID', 'Hour', 'Week_day'])
    result = att.groupby(['WeekNumber','Week_day', 'Hour']).size().reset_index(name='cnt')
    filtered_result = result[result['cnt'] > 10]
    filtered_result.drop(columns=['cnt'], inplace=True)

    return filtered_result

def filter_att(df, valid_result):
    # Create valid_att by merging df with valid_result on WeekNumber, Week_day, and Hour
    valid_att = df.merge(valid_result, how='inner', left_on=['WeekNumber', 'Week_day', 'Hour'], right_on=['WeekNumber', 'Week_day', 'Hour'])

    # Create invalid_att by merging df with valid_result on WeekNumber, Week_day, and Hour and keeping the rest
    invalid_att = df.merge(valid_result, how='outer', left_on=['WeekNumber', 'Week_day', 'Hour'], right_on=['WeekNumber', 'Week_day', 'Hour'], indicator=True)
    invalid_att = invalid_att[invalid_att['_merge'] == 'left_only'].drop(columns=['_merge'])
    #print(invalid_att,valid_att)
    return valid_att,invalid_att


input_file = 'attsheet.csv'
df = pd.read_csv(input_file)
#def clean_data(df):
#Clean and process the DataFrame
df.drop_duplicates(inplace=True)
columns_to_drop = ['Date', 'Email Address', 'Time']
df.drop(columns=columns_to_drop, inplace=True)

# Convert 'Timestamp' to datetime
df['Timestamp'] = pd.to_datetime(df['Timestamp'])
df['Date'] = df['Timestamp'].dt.date
df.rename(columns={'Your ID number is:': 'ID'}, inplace=True)

# Convert the 'Date' column
df['Date'] = df['Date'].apply(convert_date)

# Extract week number and weekday from the date
df['WeekNumber'] = df['Date'].dt.isocalendar().week
df['Week_day'] = df['Date'].dt.dayofweek

#choosing valid dates
valid_result = valid_dates(df)

#filtering te invalid attendance entries
filtered_df,filteredout_df = filter_att(df, valid_result)
print('filtered out data:\n',filteredout_df,'\n')
df = filtered_df
df.drop(columns=['WeekNumber','Week_day'], inplace=True)

# Create every week columns
weeks = df['Date'].dt.isocalendar().week.unique()
for week in weeks:
    df[f'Week_{week}'] = (df['Date'].dt.isocalendar().week == week).astype(int)


# Remove unnecessary columns before aggregation
df.drop(columns=['Timestamp', 'Date', 'Hour'], inplace=True)


cleaned_df=df
result = aggregate_attendance(cleaned_df)
result.to_excel('result_attsheet.xlsx', index=False)
print(result)
         
