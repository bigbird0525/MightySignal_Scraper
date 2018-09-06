#Notes:
#https://mightysignal.com/app/app/#/login 
#https://mightysignal.com/app/app/#/timeline
#https://mightysignal.com/app/app/#/search
#Contact table: table id= "companyDetailsTable"
#Notes: Added App Type Column to output - Jan 4


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
import time, os, csv, sys, queue, threading
from bs4 import BeautifulSoup as bsoup

lock = threading.Lock()
pub_queue = queue.Queue()
inFile = sys.argv[1]
outFile = sys.argv[2]

class Crawler(object):
    
    def __init__(self, email, password):
        super().__init__()
        self.email = email
        self.password = password
        self.options = Options()
        self.options.add_argument('-headless')
        self.browser = webdriver.Firefox(executable_path='geckodriver', firefox_options=self.options)
        self.wait = WebDriverWait(self.browser, 30)
        
    def getGoogleSession(self):
        print("Logging into Google Account...")
        self.url = "https://www.google.com/accounts/Login"
        self.browser.get(self.url)
        self.wait.until(EC.visibility_of_element_located((By.ID, 'identifierId'))).send_keys(self.email)
        self.browser.find_element_by_id("identifierNext").click()
        self.wait.until(EC.visibility_of_element_located((By.NAME, 'password'))).send_keys(self.password)
        self.browser.find_element_by_id("passwordNext").click()     
        
    def loginMS(self):
        print("Logging into Mighty Signal")
        self.url = 'https://mightysignal.com/app/app/#/login'
        self.browser.get(self.url)
        time.sleep(2)
        self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="content"]/div/div[2]/div/div/div[1]/a'))).click()
        time.sleep(2)
    
    def iterateURLs(self, urls):
        print("Beginning Scraping operations...")
        results_dict = {}
        global outFile
        with open(outFile, 'a') as results:
            dict_writer = csv.DictWriter(results, fieldnames=['AppName','AppType','Categories','Pub Name','url','name','title','email','Countries'])
            results_list = []
            try:
                if urls['Pub Url'] is not 'MightySignal Publisher Page':
                    self.browser.get(urls['Pub Url'])
                    time.sleep(2)
                contacts = BSoup_Scrape().scrape(self.browser,urls['Pub Url'])
                if contacts is not None:
                    for contact in contacts:
                        results_dict['AppName'] = urls['AppName']
                        results_dict['AppType'] = urls['AppType']
                        results_dict['Categories'] = urls['Categories']
                        results_dict['Pub Name'] = urls['Pub Name']
                        results_dict['url'] = urls['Pub Url']
                        results_dict['name'] = contact['name']
                        results_dict['title'] = contact['title']
                        results_dict['email'] = contact['email']
                        results_dict['Countries'] = urls['Countries']
                        results_list.append(results_dict.copy())
                    for result in results_list:
                        dict_writer.writerow(result)
            except KeyError:
                print("Check key names")
            except ValueError:
                print("Check variable value names")
                    
        
class BSoup_Scrape(object):
    
    def scrape(self, browser,url):
        scrape_list = []
        scrape_dict = {}
        soup = bsoup(browser.page_source, 'html.parser')
        tables = soup.findAll('table', {'id':'companyDetailsTable'})
        try:
            if len(tables) > 0:
                for table in tables:
                    header = table.findAll('th')
                    if 'Contact' in header[0].text.strip():
                        for row in table.findAll("tr")[1:]:
                            tds = row.find_all('td')
                            if tds[1].text.strip() not in 'Get Email':
                                scrape_dict['name'] = tds[0].text.strip().split(' - ')[0].split(' ')[0]
                                try:
                                    scrape_dict['title'] = tds[0].text.strip().split(' - ')[1]
                                except IndexError:
                                    scrape_dict['title'] = 'None'
                                scrape_dict['email'] = tds[1].text.strip()
                                scrape_list.append(scrape_dict.copy())
                            else:
                                continue
                    else:
                        continue
        except AttributeError:
            return print("No table found")
        return scrape_list

class FileReader(object):
    
    def __init__(self, file):
        super().__init__()
        self.file = file
        
    def readFile(self):
        pubInfo_list = []
        dict_keys = ['AppName','AppType','Categories','Pub Name', 'Pub Url','Countries']
        with open(self.file) as pubInfo:
            reader = csv.reader(pubInfo)
            for row in reader:
                pub_dict = dict(zip(dict_keys,row))
                if pub_dict['Countries'] == '':
                    pub_dict['Countries'] = 'Not Listed'
                pubInfo_list.append(pub_dict)
        return pubInfo_list


class Runnable(threading.Thread):
    
    def __init__(self):
        super().__init__()
        
    def __call__(self):
        
        global pub_queue
        username = '{0}' # Insert username
        password = '{1}' # Insert password
        crawler = Crawler(username, password)
        crawler.getGoogleSession()
        crawler.loginMS()   
        try:
            while not pub_queue.empty():
                lock.acquire()
                pub_url = pub_queue.get()
                lock.release()
                if not pub_url:
                    break

                else:
                    print("Pulling data for {}".format(pub_url['AppName']))
                    crawler.iterateURLs(pub_url)
        finally:
            crawler.browser.quit()
            pub_queue.task_done()
            

class Threader(Runnable):
    
    def __init__(self):
        super().__init__()
        
    def launch(self, pub_urls):
        global pub_queue
        threads = []
        
        for row in pub_urls[1:]:
            pub_queue.put(row)
        
        print("Launching threads....")
        for i in range(1):
            threads.append(threading.Thread(target=Runnable()))
            threads[-1].daemon = True
            threads[-1].start()
        
        for thread in threads:
            thread.join()
        
        print("Closing threads....")
    
def main():
    global inFile
    global outFile
    urls = FileReader(inFile).readFile()
    with open(outFile, 'w') as results:
        dict_writer = csv.DictWriter(results, fieldnames=['AppName', 'AppType', 'Categories','Pub Name','url','name','title','email','Countries'])
        dict_writer.writeheader()
    Threader().launch(urls)
    
if __name__ == '__main__':
    main()
