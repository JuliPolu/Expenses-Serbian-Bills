import pyppeteer
from pyppeteer import launch
import logging
from bs4 import BeautifulSoup
from googletrans import Translator
from datetime import datetime
import pandas as pd

logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)

versionn = eval(pyppeteer.__chromium_revision__)


# Function to scrape data from rachun page
async def get_page_content(browser, url, data):
    logging.info(f"{versionn}")
    page = await browser.newPage()
    logging.info(f"Page opened")
    await page.goto(url)
    # Use XPath to find the element
    element1 = await page.Jx("//*[@id='sdcDateTimeLabel']/text()")
    property = await element1[0].getProperty('nodeValue')
    date_time = await property.jsonValue()
    # date_time = await (await element1[0].getProperty('nodeValue')).jsonValue()
    date = datetime.strptime(date_time.strip(), '%d.%m.%Y. %H:%M:%S').date()
    element2 = await page.Jx("//*[@id='shopFullNameLabel']/text()")
    shop = await (await element2[0].getProperty('nodeValue')).jsonValue()
    element = await page.xpath("/html/body/div/div/form/div[3]/div/div/div[1]/h5/a")   
    # Click the first matching element
    await element[0].click()
    logging.info(f"click")
    html_content = await page.evaluate('''() => {
            return new Promise((resolve, reject) => {
                setTimeout(() => {
                    const result = document.querySelectorAll('table[class="table invoice-table"]')[0].innerHTML;
                    resolve(result);
                }, 3000);  // delay of 3000 milliseconds (3 seconds)
            });
        }''')
    await page.close()
    # Create a BeautifulSoup object and specify the parser
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all table rows in the body of the table
    rows = soup.find_all('tr')

    # Iterate over each row
    for row in rows:
        # Find all columns in the row
        cols = row.find_all('td')
        
        # Only process rows with columns (to exclude header)
        if cols:
            # Get the text from each column
            item_data = [col.get_text(strip=True) for col in cols]

            # Convert the list of item data into a dictionary
            item_dict = {
                "Name": item_data[0],
                "Quantity": item_data[1],
                "UnitPrice": item_data[2],
                "TotalPrice": item_data[3],
                # "TaxBaseAmount": item_data[4],
                # "VAT": item_data[5],
                "Date": date,
                "Shop_name": shop,
                'URL': url
            }
            # Add the dictionary to the list
            data.append(item_dict)


# Main Function to open and close browser
async def scrape_main(url, data):
    
    browser = await launch(executablepath='/usr/bin/google-chrome-stable', headless=True, args=['--no-sandbox','--enable-logging=stderr',  '--v=1'])
    await get_page_content(browser, url, data)
    await browser.close()


CATEGORIES = {
    'Personal Care': ['Nevena', 'TONIK', 'Dontentent', 'Pas.za Zube', 'mask', 'KPA MAGICNA', 'Kinesiology', 'Rep.Snail', 'Avene', 'KERATIN', 'Aloe', 'Centrum', 'Mbeauty', 'Femibion', 'WC', 'Hyalurogel', 'L-Carnit', 'Rollon', 'Sensodyne', 'Hair', 'bath', 'Aloa', 'soap', 'candles', 'gel', 'ziaja', 'Nature Republic', 'cisc', 'drain', 'shampoo', 'colgate', 'wet', 'cleaning'],
    'Household': ['hook', 'Teflon tape', 'window', 'KESA', 'CEDILJE', 'sheet', 'Bucket', 'Canadier', 'Domestic', 'Pillow', 'Sudoper mesh', 'Napkin', 'sprayer', 'candles', 'cotton', 'kitchen', 'bag', 'towel', 'etharsk oil', 'bed linen'],
    'Fish': ['Sardine', 'Orada', 'shrimp', 'Tuna', 'Tunj.in SOP.SOS', 'sushi', 'dorado', 'trout', 'sea bass'],
    'Fruits & Veg': ['Biokoluhla', 'Garlic', 'Potato', 'Papeleto', 'Celery', 'Djumbir', 'ginger', 'sargarepa',  'Mlin', 'Raspberry', 'Blackberry', 'pepper', 'olive', 'Carrot', 'BEET', 'Bow white', 'Mushrooms', 'Blueberry', 'fruit', 'Zucchini', 'Peaches', 'Grapes', 'Strawberries', 'Nectarine', 'Lubenica', 'Lemon', 'LUK', 'Champignons', 'berries', 'grapefruit',  'paprika', 'celer,' 'blueberry', 'mlin,' 'watermelon', 'rosemary', 'tomato', 'avocado', 'tomatos', 'apricot', 'orange', 'frozen mix', 'cucumber', 'cherry', 'olives', 'pear', 'apple', 'bananas', 'banana'],        
    'Meat & Eggs': ['pork', 'Kuršta', 'Pasteta', 'Egg', 'Salami', 'breast', 'chicken', 'beef', 'tongue', 'turkey', 'trout', 'pate', 'sausage'],
    'Milk & cheese': ['Gorgonzola', 'Puter', 'butter', 'Emmentaler',  'sour cream', 'CREAM', 'yogurt', 'gouda', 'milk', 'sir', 'cheese', 'mozzarella'],
    'Oil & Souse': ['Pesto', 'Soda', 'Kecap', 'oil', 'Oregano', 'sal', 'Mustard', 'kečap', 'ketchup', 'Soja sauce'],
    'Nuts & Cereals': ['Pištata', 'Waffle', 'Musli', 'cashew', 'PASTENINA', 'rice', 'Granola', 'Hazelnut', 'walnut', 'pistać', 'seeds', 'nudle', 'grain', 'Crackers'],
    'Sweet & Bakery': ['candies', 'Princess', 'Dezert', 'Cookies', 'Snickers', 'Coconut', 'Grč.tip Jog.Borovn.Pcs', 'biscuits', 'Croissan', 'chocolate', 'Kreker', 'tortilla', 'sunflower', 'kroasan', 'bread', 'cake', 'ice cream', 'icecream'],
    'Coffee': ['cappuccino', 'coffee', 'kafa'],
    'Drink': ['Ajvar', 'Min.voda', 'Nectar', 'min.Water', 'water', 'aqua', 'viva', 'restart', 'juice','tea'], 
    'Alco': ['Malbec', 'South African Chardon', 'Gorki list', 'Bianco Terre Sicilia', 'shiraz', 'Gin'],
    'Cloths': ['PALM', 'T-shirt', 'HAMA K.PUNJAC', 'BP500 BLACK 25L', 'Busty Primorac',  'Khaki', 'sneakers', 'fit500', 'Raiders', 'TELESCOPS', 'Centrum'],
    'Electronics': ['Whiteshark', 'USB-C-HDMI', 'KACKET ULTRAILIGHT VISOR', 'Kingston', 'cable'],
}

translator = Translator()

def translate_text(text):
    return translator.translate(text, src='bs', dest='en').text

def categorize_purchases(name):
    # Make the name lower case
    name = name.lower()
    
    # Check each category
    for category, keywords in CATEGORIES.items():
        # If any of the keywords are in the name, return the category
        if any(keyword.lower() in name for keyword in keywords):
            return category
    
    # If no keywords matched, return 'Other'
    return 'Other'


def transform(df):
    """Трансформация данных"""

    df['Name'] = [translate_text(name) for name in df['Name']]

    df['Shop_name'] = df['Shop_name'].str.replace(r'\d+-', '').replace('FILIJALA', 'DM', regex=True)

    for col in df.columns[1:4]:
            df[col] = df[col].str.replace('.', '', regex=True).str.replace(',', '.', regex=True).astype(float)

    df['Date'] = pd.to_datetime(df['Date'])

    df['Category'] = df['Name'].apply(categorize_purchases)

    df.loc[df['Shop_name'].str.contains('Gigatron', case=False, na=False), 'Category'] = 'Electronics'

    return df
