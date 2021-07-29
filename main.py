import pandas as pd
import requests as r
import os
import csv

csv_path = "C:\\Users\\jocerdas\\OneDrive - Microsoft\Documents\\AI+DS\\Python Projects\\earthquakes\\scraping app\\scraping-app\\Data\\cr_earthquakes.csv"

def writeToCsv(earthquake_data):
    earthquake_data.to_csv(csv_path, index=False)

def appendDataToCsvFileCheckForDuplicatesInRows(earthquake_data, csv_path):
    #First, we append the new data
    print("Appending new data to existing CSV file:cr_earthquakes.csv")
    earthquake_data.to_csv(csv_path, mode='a',header=False, index=False)

    #Second, we read the CSV file and drop the duplicate rows
    temp_dataframe = pd.read_csv(csv_path)
    temp_dataframe.drop_duplicates(subset=['Localizacion','Fecha','Hora'], keep='first', inplace=True)

    #Third, re-write the dataframe
    print("Writting new data without duplicates to the existing CSV file")
    temp_dataframe.sort_values(by='Fecha', axis=0, inplace=True, na_position='last')
    temp_dataframe.to_csv(csv_path, index=False)

    return True

def main():
    #Send a HTTP get request for the website
    response = r.get("http://www.ovsicori.una.ac.cr/sistemas/sentidos_map/indexleqs.php")

    #Check the status code 200:OK
    if response.status_code == 200:
        print(f"HTTP - Status code:  {response.status_code}")

        #Reads from the website and creates the data frame
        table = pd.read_html("http://www.ovsicori.una.ac.cr/sistemas/sentidos_map/indexleqs.php")
        earthquake_data = pd.DataFrame(table[0])
    
        #Checking if the file already exist,if does not exist then we write the data tp the csv file
        if os.path.isfile(csv_path):
            #If the file already exist it means we have already pulled data from the website,
            #In this case we need to append the latest data to the csv dataframe
            print("earthquake_data.csv - file already exist")

            if(appendDataToCsvFileCheckForDuplicatesInRows(earthquake_data, csv_path)):
                print("Appending data and removing duplicates: ERROR_SUCCESS")
            else:
                print("Appending operation failed")
        else:
            #The CSV file does not exist so, let's create one using the data frame
            print("Writing data to CSV file:cr_earthquakes.csv")
            writeToCsv(earthquake_data)
    else:
        print(f"HTTP - Status code {response.status_code}")

    return 0

if __name__ == '__main__':
    #Main routine
    main()