Calculate Expenses from Bills (Serbia)
==============

## Tiny but useful pet project to calculate your day-to-day expenses just scanning your bills

### How it works
- Uses telegram bot (python) interface for data collection and quick data analysis
- Takes URL obtained by scanning QR code on the bill
- Scrapes information on purchased data from URL using PostgreSQL with Chromium headless browser
- Inserts data into PostgreSQL database

### How to deploy

- Create files for environmental variables for postgres database and telegram bot API

- Raise 2 docker containers (with database and telegram bot) <br>
`docker-compose up --build`

- Available Telegramm bot commands: <br>
`/add`       to add bill (URL) <br>
`/count`     to count all items in DB <br>
`/by_month`  total expenses by month <br>
`/by_category`  summary of expenses by categories by month <br>




