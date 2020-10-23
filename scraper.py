import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup

def scraper(url, resp):
    if not is_valid(url):
        return []
    links = extract_next_links(url, resp)
    print(links)
    return [link for link in links if should_visit(link)]

def should_visit(link):
    pass

def extract_next_links(url, resp):
    # Implementation requred.
    # print(resp.raw_response.text)
    urls = set()
    soup = BeautifulSoup(resp.raw_response.text, 'html.parser')
    for url in soup.find_all('a'):
        urls.add(url.get('href'))
    return list(urls) 

def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        domain_match = re.match(
            r"(today\.uci\.edu\/department\/information_computer_sciences\/?).*"
            + r"|.*(\.ics\.uci\.edu\/?).*|.*(\.cs\.uci\.edu\/?).*"
            + r"|.*(\.informatics\.uci\.edu\/?).*|.*(\.stat\.uci\.edu\/?).*", parsed.netloc.lower())
        type_match = re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
        return (domain_match and not type_match) 

    except TypeError:
        print ("TypeError for ", parsed)
        raise
