
# The Crawler 

This is a simple crawler that crawls data from two websites currently:

- https://www.ipo.gov.uk
- https://www.sec.gov
- https://www.bvdinfo.com/de-de/unsere-losungen/daten/international/orbis

for company and patent related data.

## Installation

Create a virtual environment and install the requirements:
```bash 
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

## Usage

To run the crawler, you can run the following command:

```bash
$ python crawl_data.py
```
Check the main function in crawler.py for more details.


- `crawl_data.py` can generate csv file with; name, city, country and CIK number of the companies. Name, city and country are scraped from sec.gov website.(CIK number of the companies provided by a xlsx file)
- `crawl_data.py` can also find publications of the companies from ipo.gov.uk website. 


Automation of Orbis database access and batch search on Orbis database

- `orbis-access.py` can access Orbis database, execute batch search by providing the csv file generated by `crawl_data.py`, add/remove columns to enchance the search results and export the results to csv file.

## Main Workflow 

Beside the given main workflow given below, there are other options which can be used with this repository. 

The workflow is subject to change in time. 


![IMG_F71485E8296E-1](https://user-images.githubusercontent.com/13614433/209945813-addf531e-acf0-43d9-accc-92cf0ae35d1a.jpeg)
