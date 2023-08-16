from bs4 import BeautifulSoup
import requests 
import pandas as pd 
import itertools
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
pd.set_option('display.max_rows', None)
from joblib import Parallel, delayed

# Methods to Scrape Data from the AirBNB Website.
def get_urls(first_page_url):
    
    # Intialize url and url list to store all urls found by web scraper.
    urls= [first_page_url]
    url = first_page_url
    
    # Send a get request to the AirBNB REST API, check if the status is 200.
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    
    # Retrieve all the urls from the many different listing pages on the AirBNB site. 
    # Intialize an empty string variable for the next page url.
    np = ""
    
    # Use a loop from 1 to 14 to loop through all the different listing pages. 
    for i in range(1,15):
        # Use the beautiful soup find function to get the links from the Next symbol html tag.
        np = soup.find('a', class_ = "l1ovpqvx c1ytbx3a dir dir-ltr").get("href")
        
        #create a new link with AirBNB.com as the host and concatenate the next page link.
        cnp = "https://www.airbnb.com" + np
        url = cnp
        
        # Append each next page url to the urls list.
        urls.append(url)
        
        # At the end of each loop, check if the link works by sending a get response to the AirBNB REST API.
        r = requests.get(url)
        soup = BeautifulSoup(r.text,"lxml")

    return urls

def substring_after(s, delim):
    return s.partition(delim)[2]

# A method that extracts price, name, and description from AirBnb's listing pages.  
def extract_listings(first_page_url,location):
    
    # Intialize the urls list to store the listing page urls. 
    urls = []
    urls = get_urls(first_page_url)
    
    # Initialize Names, Price, Prices, Desc list to store data extracted from each AirBNB listing.   
    Names= []
    Price = []
    Prices = []
    Desc = []
    
    # The for loop loops through all the urls stored in the URL list, which is 15 urls for 15 pages. 
    for url in urls:
        
        # Submitting a get request for each url and checking if the reponse is 200. 
        r = requests.get(url)
        soup = BeautifulSoup(r.text,'lxml')
        
        #Scraping each listing and finding the name html tag in the AirBnb listing card
        names = soup.find_all('div',class_='t1jojoys dir dir-ltr')
        
        # Looping through the all of the Name html tags  
        for i in names: 
            
            #Storing the name of each listing in the Names list.
            Names.append(i.text)
            
        #Scraping each listing and finding the price html tag in the AirBnb listing card.
        prices = soup.find_all('span', class_ ="a8jt5op dir dir-ltr")
        
        #Looping through all of the Price html tags. 
        for i in prices: 
            
            #Storing the price of each listing in the Price list.
            Price.append(i.text) 
            
        #Scraping each listing and finding the description html tag in the AirBnb listing card.
        descriptions = soup.find_all('span',class_='t6mzqp7 dir dir-ltr')
        
        #Looping through all of the Description html tags.
        for i in descriptions:
            #Storing the Desc of each listing in the Price list.
            Desc.append(i.text)
    
    # Cleaning the price list by removing all of the non price values extracted from the price html tag.                
    # Looping through all of the elements in the Price list.
    for p in Price: 
        
        # Checking if each element contains the characters Rp.
        if 'Rp' in p: 
            # Appending all the price Rp valuse to the prices list
            Prices.append(p)
        elif 'originally' in p:
            substring_after(p, 'originally')
    
    # Creating a new pandas df object and storing all of the data in the Names, Prices, and Desc list in the df.
    bnb_df = pd.DataFrame({'Property_Name':Names, 'Price/Night': Prices, "Property_Description":Desc})
    
    # Adding a new Location column to the dataset that shows the location of each property.
    bnb_df['Location'] = list(itertools.repeat(location, len(Names)))
    
    # Returnd the pandas df object at the end of the method
    return bnb_df

def extract_all_listings():
    links = ['https://www.airbnb.com/s/Ubud--Bali--Indonesia/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&flexible_trip_lengths%5B%5D=one_week&monthly_start_date=2023-09-01&monthly_length=3&price_filter_input_type=2&price_filter_num_nights=5&channel=EXPLORE&query=Ubud%2C%20Bali&place_id=ChIJt8NOlg090i0RMC19yvsLAwQ&date_picker_type=calendar&source=structured_search_input_header&search_type=autocomplete_click',
                       'https://www.airbnb.com/s/Canggu--Badung-Regency--Bali--Indonesia/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&flexible_trip_lengths%5B%5D=one_week&monthly_start_date=2023-09-01&monthly_length=3&price_filter_input_type=2&price_filter_num_nights=5&channel=EXPLORE&query=Canggu%2C%20Badung%20Regency%2C%20Bali&place_id=ChIJZZZY9GE40i0RMP2CyvsLAwU&date_picker_type=calendar&source=structured_search_input_header&search_type=autocomplete_click',
                       'https://www.airbnb.com/s/Kuta--Bali--Indonesia/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&flexible_trip_lengths%5B%5D=one_week&monthly_start_date=2023-09-01&monthly_length=3&price_filter_input_type=2&price_filter_num_nights=5&channel=EXPLORE&query=Kuta%2C%20Bali&place_id=ChIJN3P2zJlG0i0RACx9yvsLAwQ&date_picker_type=calendar&source=structured_search_input_header&search_type=autocomplete_click',
                       'https://www.airbnb.com/s/Seminyak--Bali--Indonesia/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&flexible_trip_lengths%5B%5D=one_week&monthly_start_date=2023-09-01&monthly_length=3&price_filter_input_type=2&price_filter_num_nights=5&channel=EXPLORE&query=Seminyak%2C%20Bali&place_id=ChIJ_8h84N9G0i0R0P2CyvsLAwU&date_picker_type=calendar&source=structured_search_input_header&search_type=autocomplete_click']
    locations = ['Ubud, Bali', 'Canggu, Bali', 'Kuta, Bali', 'Seminyak, Bali']
    vals = Parallel(n_jobs=-1)(delayed(extract_listings)(link,location) for link, location in zip(links,locations))
    vals = pd.concat(vals)
    vals = vals.reset_index(drop=True)
    return vals
    
    
def store_df_firebase():
    # Find the credential file to your firebase storage
    cred = credentials.Certificate('rental-property-dataset-firebase-credentials.json')
    
    # Intializes a firebase app to perform crud operations on your firebase storage
#     app = firebase_admin.initialize_app(cred)
    
    # Intializes the database object to add collections to your database
    db = firestore.client()
    
    # The extract listings method scrapes data from all AirBNB listings from the give link
    # and returns a pandas df containing all the scraped information.
    data = extract_all_listings()   
    
    # Convert all the values in the dataframe to strings because after scraping all the values are objects.
    data = data.astype('string')
    
    # Convert all rows in the scraped df into seperate dictionaries, feel free to change the orientation 
    data = data.to_dict(orient = 'records')
    
    # The for loop loops through all the dictionaries in the data list.
    for record in data: 
        # Create a collection by passing a collection name. 
        # Creates a unique identifier for each document from each property name in the dataset.
        doc_ref = db.collection("Properties_In_Bali").document(record['Property_Name'])
        
        # Adds all the data contained in the dictionaries from the data list.
        doc_ref.set(record)
    # Used a print statement to check if we successfully added the collection to our firebase storage.
    print('Collection is sucessfully saved in firebase')
    
    # Delete the firebase app because you are no longer using the intialized firebase app. 
    # If we keep the same firebase app running, it will lead to an intialized app error when we are running 
    # the method multiple times. 
#     firebase_admin.delete_app(app)

def read_collection_firebase_data():
    # Find the credential file to your firebase storage
    cred = credentials.Certificate('rental-property-dataset-firebase-credentials.json')

    # Intializes a firebase app to perform crud operations on your firebase storage
    app = firebase_admin.initialize_app(cred)

    # Intializes the database object to add collections to your database
    db = firestore.client()
    
    # Create the list to store dictionaries
    list_of_docs = []
    
    #Extracting the collection by title 
    doc_ref = db.collection("Properties_In_Bali")

    # Converting the collection into a stream
    docs = doc_ref.stream()

    # Checks if the stream list is null. 
    if bool(docs) == False:
            print("Document does not exist.")
    
    # Runs the following code when db is not null
    else:
        # The for loop loop through the docs list 
            for doc in docs:
                # Append the each doc as a dictionary into a list 
                list_of_docs.append(doc.to_dict())

    # Convert the list of dictionaries into a dataframe
    df = pd.DataFrame(list_of_docs)

    # Delete the firebase app because you are no longer using the intialized firebase app. 
    firebase_admin.delete_app(app)
    
def main():
    store_df_firebase()