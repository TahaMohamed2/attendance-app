import gdown
import pandas as pd
from datetime import datetime, timedelta
import re

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


def convert_date(date_str):
    #Convert various date formats to datetime
    try:
        return pd.to_datetime(date_str, errors='raise')
    except (ValueError, TypeError):
        try:
            # Handle Unix timestamp conversion
            reference_date = datetime(1900, 1, 1)
            seconds = int(date_str) * 86400
            return pd.to_datetime(reference_date + timedelta(seconds=seconds))
        except (ValueError, TypeError):
            try:
                return pd.to_datetime(date_str, format='%m/%d/%Y', errors='raise')
            except (ValueError, TypeError):
                return pd.NaT

def extract_file_id(url_input):
    """Extract file ID from Google Drive URL."""
    pattern = r'/d/([a-zA-Z0-9_-]+)/'
    match = re.search(pattern, url_input)
    return match.group(1) if match else None

def download_file(file_id):
    """Download the Google Drive file as a CSV."""
    url = f'https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv'
    output_file = 'attsheet.csv'
    try:
        gdown.download(url, output_file, fuzzy=True, quiet=False)
        return output_file
    except Exception as e:
        print(f"Error downloading the file: {e}")
        return None

def clean_data(df):
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
    df = filtered_df
    print (filteredout_df)
    df.drop(columns=['WeekNumber','Week_day'], inplace=True)

    # Create every week columns
    weeks = df['Date'].dt.isocalendar().week.unique()
    for week in weeks:
        df[f'Week_{week}'] = (df['Date'].dt.isocalendar().week == week).astype(int)


    # Remove unnecessary columns before aggregation
    df.drop(columns=['Timestamp', 'Date', 'Hour'], inplace=True)


    return df

def aggregate_attendance(df):
    """Aggregate attendance data by ID."""
    result = df.groupby('ID').agg('sum').reset_index()
    result['attendance'] = 0
    
    for i in range(result.shape[0]):    #itrate over IDs
        for j in range(1, result.shape[1]-1): #iterate over weeks
            if result.iloc[i, j] > 0:
                result.iloc[i, j] = 1
                #explain error on next line as values never exceed 2 ?
                result.at[i, 'attendance'] = result.at[i, 'attendance']+1
                #print(i,j,result.at[i, 'attendance'] )
    
    return result.sort_values(by='ID')


def attendance_main(url_input):  
    ##url_input = request.form.get('url')  # Assuming this is set from a form
    file_id = extract_file_id(url_input)
    
    if file_id:
        output_file = download_file(file_id)
        if output_file:
            df = pd.read_csv(output_file)
            cleaned_df = clean_data(df)
            result = aggregate_attendance(cleaned_df)
            result.to_excel('result_attsheet.xlsx', index=False)
            print(result)
            return result

# Main execution flow
# if __name__ == '__main__':
#     app.run(debug=True)
