import pandas as pd

def run_transformation():
    data = pd.read_csv(r'zipco_transaction.csv')

    # Remove duplicates
    data.drop_duplicates(inplace=True)

    # Handle missing values ( fillinf missing numeric values with the mean or median)
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace=True)

    # Handle missing values (fill missing string/object values with 'Unknown')
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col: 'Unknown'}, inplace=True)

    # Cleaning date column: assigning the right data type
    data['Date'] = pd.to_datetime(data['Date'])

    # Creating fact and dimension tables
    # Create the product table
    products = data[['ProductName']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'ProductID'
    products = products.reset_index()

    # Create Customers Table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()

    # staff table['Staff_Name', 'Staff_Email']
    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()

    # Creating the transaction table
    transaction = data.merge(products, on=['ProductName'], how='left') \
                    .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left') \
                    .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left') 

    transaction.index.name = 'TransactionID'
    transaction = transaction.reset_index() \
                            [['Date', 'TransactionID', 'ProductID',  'Quantity', 'UnitPrice', 'StoreLocation', 'PaymentType', 'PromotionApplied', 'Weather', \
                                'Temperature', 'StaffPerformanceRating', 'CustomerFeedback', 'DeliveryTime_min', 'OrderType', 'CustomerID', 'StaffID', \
                                'DayOfWeek', 'TotalSales']]

    # Save data as csv files
    data.to_csv('clean_data.csv', index=False)
    products.to_csv('products.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    staff.to_csv('staff.csv', index=False)
    transaction.to_csv('transaction.csv', index=False)

    print('Data Cleaning and Transformation completed successfully')
