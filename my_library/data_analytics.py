import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Dataset Initial Analytics
def dataset_initial_analytics(df):
    """"
    Objective :
    Function for preprocessing data, known missing, duplicated values and basic statistics for every column in the dataframe and duplicated rows
    df is a dataframe
    """
    try:
        variables = pd.DataFrame(columns=['Variable','Number of unique values','Number of Null','Percent of Null(%)','Type','Values'])
        for i, var in enumerate(df.columns):
            variables.loc[i] = [var, df[var].nunique(),df[var].isnull().sum(),df[var].isnull().sum()/df.shape[0]*100,df[var].dtypes,df[var].unique()]
        return variables.set_index('Variable')
    except Exception as e:
        print(f'Error occurred: {e}')
#Outlier Detection
def outlier_detection(df,numeric):
    """
    Objective :
    Function for detecting outliers in the dataset
    df is a dataframe
    numeric is a list of numeric columns
    """
    try:
        outliers = pd.DataFrame(columns=['Variable','Q1','Q3','IQR','Lower Bound','Upper Bound','Number of Outliers'])
        for i, var in enumerate(numeric):
            Q1 = df[var].quantile(0.25)
            Q3 = df[var].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5*IQR
            upper_bound = Q3 + 1.5*IQR
            number_of_outliers = df[(df[var]<lower_bound) | (df[var]>upper_bound)].shape[0]
            outliers.loc[i] = [var,Q1,Q3,IQR,lower_bound,upper_bound,number_of_outliers]
        return outliers.set_index('Variable')
    except Exception as e:
        print(f'Error occurred: {e}')
#zscore outlier detection
def zscore_outlier_detection(df,numeric):
    """
    Objective :
    Function for detecting outliers in the dataset using zscore
    df is a dataframe
    numeric is a list of numeric columns
    """
    try:
        outliers = pd.DataFrame(columns=['Variable','Mean','Std Dev','Lower Bound','Upper Bound','Number of Outliers'])
        for i, var in enumerate(numeric):
            mean = df[var].mean()
            std = df[var].std()
            lower_bound = mean - 3*std
            upper_bound = mean + 3*std
            number_of_outliers = df[(np.abs((df[var]-mean)/std)>3)].shape[0]
            outliers.loc[i] = [var,mean,std,lower_bound,upper_bound,number_of_outliers]
        return outliers.set_index('Variable')
    except Exception as e:
        print(f'Error occurred: {e}')



# Univariat Analysis
def kde_plot(df,numeric):
    for j in range (0,len(numeric)):
        num = numeric[j]
        plt.figure(figsize=(10,5))
        sns.kdeplot(data=df,x=num)

def count_plot(df,categorical):
    for j in range (0,len(categorical)):
        cat = categorical[j]
        plt.figure(figsize=(10,5))
        sns.countplot(data=df,x=cat)
        
def box_plot(df,numeric):
    for j in range (0,len(numeric)):
        num = numeric[j]
        plt.figure(figsize=(10,5))
        sns.boxplot(data=df,x=num)

# Timeseries Analytics
# Function to plot daily sales
def daily_sales_sum(df,sales_date):
    df[sales_date] = pd.to_datetime(df[sales_date])   
    daily_sales = df.groupby(sales_date)['total'].sum().reset_index()
    plt.figure(figsize=(20,10))
    sns.lineplot(x=sales_date, y='total', data=daily_sales)
    plt.xticks(rotation=45)  # Rotate x-axis labels by 45 degrees
    plt.tight_layout()  # Adjust layout to prevent overlapping
    plt.title('Daily Sales')
    plt.show()

# Function to plot weekly sales
def weekly_sales_sum(df,sales_date):
    df[sales_date] = pd.to_datetime(df[sales_date])       
    weekly_sales = df.resample('W-Mon', on=sales_date)['total'].sum().reset_index()
    plt.figure(figsize=(20,10))
    sns.lineplot(x=sales_date, y='total', data=weekly_sales)
    plt.xlabel('Week')
    plt.ylabel('Total Sales')
    plt.title('Weekly Sales')
    plt.xticks(rotation=45)  # Rotate x-axis labels by 45 degrees for better readability
    plt.tight_layout()  # Adjust layout to prevent overlapping
    plt.show()

#Function to plot monthly sales
def monthly_sales_sum(df,sales_date):
    df[sales_date] = pd.to_datetime(df[sales_date])       
    monthly_sales = df.resample('M', on=sales_date)['total'].sum().reset_index()
    plt.figure(figsize=(20,10))
    sns.lineplot(x=sales_date, y='total', data=monthly_sales)
    plt.xlabel('Month')
    plt.ylabel('Total Sales')
    plt.title('Monthly Sales')
    plt.xticks(rotation=45)  # Rotate x-axis labels by 45 degrees for better readability
    plt.tight_layout()  # Adjust layout to prevent overlapping
    plt.show()

def holiday_date():
    holiday_data = {
    'Date': ['2023-01-01', '2023-01-22', '2023-02-18', '2023-03-22', '2023-04-07', 
             '2023-04-22', '2023-04-23', '2023-05-01', '2023-05-18', '2023-06-01', 
             '2023-06-04', '2023-06-29', '2023-07-19', '2023-08-17', '2023-09-28', 
             '2023-12-25'],
    'Holiday': ['Tahun Baru 2023', 'Tahun Baru Imlek 2575 Kongzili', 'Isra Mikraj Nabi Muhammad SAW', 
                'Hari Suci Nyepi Tahun Baru Saka 1945', 'Wafat Isa Almasih', 'Hari Raya Idul Fitri 1444 Hijriyah', 
                'Hari Raya Idul Fitri 1444 Hijriyah', 'Hari Buruh Internasional', 'Kenaikan Isa Almasih', 
                'Hari Lahir Pancasila', 'Hari Raya Waisak 2566', 'Hari Raya Idul Adha 1444 Hijriyah', 
                'Tahun Baru Islam 1445 Hijriyah', 'Hari Kemerdekaan RI', 'Maulid Nabi Muhammad SAW', 
                'Hari Raya Natal']
    }

    cuti_bersama_data = {
        'Date': ['2023-01-23', '2023-03-22', '2023-04-21', '2023-04-23', '2023-04-25', 
                '2023-04-26', '2023-06-02', '2023-12-26'],
        'Holiday': ['Libur Bersama Tahun Baru Imlek 2575 Kongzili', 'Libur Bersama Hari Raya Nyepi', 
                    'Libur Bersama Hari Raya Idul Fitri', 'Libur Bersama Hari Raya Idul Fitri', 
                    'Libur Bersama Hari Raya Idul Fitri', 'Libur Bersama Hari Raya Idul Fitri', 
                    'Libur Bersama Waisak', 'Libur Bersama Hari Raya Natal']
    }

    holiday_df = pd.DataFrame(holiday_data)
    cuti_bersama_df = pd.DataFrame(cuti_bersama_data)

    # Concatenate both DataFrames
    combined_df = pd.concat([holiday_df, cuti_bersama_df], ignore_index=True)

    # Convert 'Date' column to datetime type
    combined_df['Date'] = pd.to_datetime(combined_df['Date'])
    return combined_df

