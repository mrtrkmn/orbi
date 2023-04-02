import os
import sys

root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)


"""
This file contains XPATH of the elements on the Orbis website to conduct the search and download the data.
Used by Orbi Class. 
"""
# ORBIS ELEMENT VARIABLES
LOGIN_BUTTON =                              "/html/body/div[2]/div/div[1]/div[1]/div[1]/form/fieldset/div[3]/input"
DRAG_DROP_BUTTON =                                              "/html/body/section[3]/div[3]/div/div[2]/form/div/div/label/a"
SELECT_FILE_BUTTON =            "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[1]/div[1]/input[1]"
UPLOAD_BUTTON =             "/html/body/section[3]/div[3]/div/div[2]/div/div/form/div[2]/p/a[2]"
FIELD_SEPERATOR =       "/html/body/section[2]/div[3]/div/form/div[1]/table/tbody/tr[2]/td[1]/input"
APPLY_BUTTON =                      "/html/body/section[2]/div[3]/div/form/div[3]/div[2]/input"
SEARCH_PROGRESS_BAR = "/html/body/section[2]/div[3]/div/form/div[1]/div[1]/div[1]"
PROGRESS_TEXT_XPATH = "/html/body/section[2]/div[3]/div/form/div[1]/div[2]/div[1]/div[2]/p"
VIEW_RESULTS_BUTTON =               "/html/body/section[2]/div[1]/div[2]/ul/li[1]/a"
ADD_REMOVE_COLUMNS_VIEW = '//*[@id="main-content"]/div/div[2]/div[1]/a'

FINANCIAL_DATA_BUTTON = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[7]/div'
KEY_FINANCIAL_DATA = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[7]/ul/li[2]'
OP_REVENUE_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.OPRE:UNIVERSAL"]/div[1]/span[2]'
PL_BEFORE_TAX_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.PLBT:UNIVERSAL"]/div[1]'
PL_FOR_PERIOD_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.PL:UNIVERSAL"]/div[1]'
CASH_FLOW_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.CF:UNIVERSAL"]/div[1]'
TOTAL_ASSETS_SETTINGS = '//*[@id="KEY_FINANCIALS*KEY_FINANCIALS.TOAS:UNIVERSAL"]/div[1]'
NUMBER_OF_EMPLOYEES_SETTINGS = "/html/body/section[2]/div[3]/div/div[2]/div[1]/div/div[3]/div/ul/li[2]/div[1]"
OPERATING_PL_SETTINS = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.OPPL:IND"]/div[1]'
GROSS_PROFIT = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.GROS:IND"]/div[1]'
SALES_SETTINGS = '//*[@id="PROFIT_LOSS_ACCOUNT*PROFIT_LOSS_ACCOUNT.TURN:IND"]/div[1]'
ABSOLUTE_IN_COLUMN_OP = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[1]/div/table/tbody/tr/td[2]/a'
# all selections from the left panel
USER_SELECTIONS_PANEL = '//*[@id="main-content"]/div/div[2]/div[2]/div[1]'
SCROLLABLE_XPATH = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[4]/div[1]/div'
SCROLLABLE_XPATH_IN_SECOND_OPTION = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[3]/div/div'

ANNUAL_DATA_LIST = '//*[@id="ClassicOption"]/div/div[1]/div/div[1]/div[4]/div[1]/div/ul'
MILLION_UNITS = '//*[@id="id-currency-menu-popup"]/ul[1]/li[4]'
OP_REVENUE_OK_BUTTON = "/html/body/section[2]/div[6]/div[3]/a[2]"
CURRENY_DROPDOWN = "/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[4]/a"
DROPDOWN_APPLY = '//*[@id="id-currency-menu-popup"]/div/a[2]'

EXCEL_EXPORT_NAME_FIELD = '//*[@id="component_FileName"]'
MAIN_DIV = '//*[@id="main-content"]'

SEARCH_INPUT_ADD_RM_COLUMNS = '//*[@id="Search"]'
SEARCH_ICON_ADD_RM_COLUMNS = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[1]/div[2]/div/span'
# Other company ID number
CIK_NUMBER_VIEW = '//*[@id="IDENTIFIERS*IDENTIFIERS.COMPANY_ID_NUMBER:UNIVERSAL"]/div[2]/span'
# when adding Other company ID number column to data view
POPUP_SAVE_BUTTON = "/html/body/section[2]/div[6]/div[3]/a[2]"
CITY_COLUMN = '//*[@id="CONTACT_INFORMATION*CONTACT_INFORMATION.CITY:UNIVERSAL"]/div[2]/span'
COUNTRY_COLUMN = '//*[@id="CONTACT_INFORMATION*CONTACT_INFORMATION.COUNTRY:UNIVERSAL"]/div[2]/span'
CONTACT_INFORMATION = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[3]/div'
BVD_SECTORS = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.BVD_SECTOR_CORE_LABEL:UNIVERSAL"]/div[2]/span'
US_SIC_PRIMARY_CODES =                      '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.USSIC_PRIMARY_CODE:UNIVERSAL"]/div[2]/span'
US_SIC_SECONDARY_CODES = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.USSIC_SECONDARY_CODE:UNIVERSAL"]/div[2]/span'
DELISTING_NOTE = '//*[@id="STOCKDATA*STOCKDATA.SD_DELISTED_NOTE:UNIVERSAL"]/div[2]/span'
DESCRIPTION_HISTORY = '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.DESCRIPTION_HISTORY:UNIVERSAL"]/div[2]/span'
HISTORY = '//*[@id="OVERVIEW*OVERVIEW.OVERVIEW_HISTORY:UNIVERSAL"]/div[2]/span'
IDENTIFICATION_NUMBER_VIEW = "/html/body/section[2]/div[3]/div/div[2]/div[1]/div/div[2]/div/ul/li[5]/div"
TRADE_DESC =                '//*[@id="INDUSTRY_ACTIVITIES*INDUSTRY_ACTIVITIES.TRADE_DESCRIPTION_EN:UNIVERSAL"]/div[1]/span'
BVD_ID_NUMBER_ADD = '//*[@id="IDENTIFIERS*IDENTIFIERS.BVD_ID_NUMBER:UNIVERSAL"]'
ORBIS_ID_NUMBER_ADD = '//*[@id="IDENTIFIERS*IDENTIFIERS.ORBISID:UNIVERSAL"]'
OWNERSHIP_COLUMN = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/div'
SHAREHOLDERS_COLUMN = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/div'
GUO_NAME_INFO = '//*[@id="GUO*GUO.GUO_NAME:UNIVERSAL"]/div[2]/span'
GUO_COLUMN = '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/ul/li[4]/div'
IMMEDIATE_PARENT_COMPANY_NAME = (
    '//*[@id="main-content"]/div/div[2]/div[1]/div/div[2]/div/ul/li[15]/ul/li[1]/ul/li[3]/div'
)
ISH_NAME = '//*[@id="ISH*ISH.ISH_NAME:UNIVERSAL"]'
APPLY_CHANGES_BUTTON = '//*[@id="main-content"]/div/div[3]/form/div/input[2]'
EXCEL_BUTTON = "/html/body/section[2]/div[1]/div[2]/div[2]/div[2]/ul/li[3]/a"
EXPORT_BUTTON =                 "/html/body/section[2]/div[5]/form/div[2]/a[2]"
POPUP_DOWNLOAD_BUTTON = "/html/body/section[2]/div[6]/div[3]/a"

POPUP_CLOSE_BUTTON = "/html/body/section[2]/div[6]/div[1]/img"
NUMBER_OF_ROWS_DROP_DOWN = '//*[@id="pageSize"]'
COMPANIES_TABLE = '//*[@id="main-content"]/div/form/table/tbody'
INPUT_FIELD_VALUE =                     "/html/body/section[2]/div[3]/div/form/div[2]/ul/li[2]/input"
CONTINUE_SEARCH_BUTTON = "/html/body/section[2]/div[3]/div/form/div[1]/div[1]/div[2]"
TOTAL_PAGE_XPATH = '//*[@id="main-content"]/div/form/div[2]/ul/li[2]/input'
PROCESSING_DIV =                "//div[@class='processing-overlay']"
BATCH_WIDGET_XPATH = "/html/body/section[2]/div[3]/div/form/div[1]"
CONTINUE_LATER_BUTTON = "/html/body/section[2]/div[3]/div/form/div[1]/div[2]/div[2]/p/a"
SEARCHING_POP_UP = "/html/body/section[2]/div[3]/div/form/div[1]/div[2]"
WARNING_MESSAGE_HEADER = "/html/body/section[2]/div[3]/div/form/div[1]/div[1]"

NOT_MATCHED_COMPANIES_FILE_NAME = "not_matched_companies.txt"
