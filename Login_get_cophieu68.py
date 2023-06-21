##################################################################
# Create by Quy
# version 22 Apr 2021
##################################################################
# Import Module
from selenium import webdriver
# Open Chrome
driver = webdriver.Chrome(r'E:\project\20210914 stock data\Pycharm\support\chromedriver.exe')
# Open URL
driver.get('https://www.cophieu68.vn/account/login.php')
#wait for loading about 5s
driver.implicitly_wait(10)
# Enter user
driver.find_element_by_xpath('//*[@id="begin_header"]/table/tbody/tr/td[5]/form/table/tbody/tr[4]/td[2]/input').send_keys("quydd56@gmail.com")
#Enter pass
driver.find_element_by_xpath('//*[@id="begin_header"]/table/tbody/tr/td[5]/form/table/tbody/tr[5]/td[2]/input').send_keys("151993")
# submit
driver.find_element_by_xpath('//*[@id="begin_header"]/table/tbody/tr/td[5]/form/table/tbody/tr[7]/td[2]/input').click()
# change the <path_to_place_downloaded_file> to your directory where you would like to place the downloaded file
download_dir = "r'E:\project\20210422 stock_price\daily_data\downloaded'"
# function to handle setting up headless download
enable_download_headless(driver, download_dir)
# Download file
driver.get('https://www.cophieu68.vn/export/metastock_all.php')
#Close
driver.close