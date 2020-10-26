import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from database import Database
from collections import defaultdict 

def scraper(url, resp, db, cache):
    #TODO: check response status code

    # TEXT PARSING
    #TODO: remove stop words
    links, text = extract_next_links(url, resp)
    print(links)
    freqs = compute_word_frequencies(text)
    print(freqs)
    input("Hit enter when ready: ")

    db.upsert_word_counts(freqs)
    db.get_word_counts()
    input("Hit enter when ready: ")

    # RETURN NEW LINKS
    # TODO: toss out fragments from links
    links = remove_visited_links(links, db, cache)
    #we do this in a batch and not one at a time in should_visit because it is more efficient to let sqlite do logic
    #print the newly filtered list of links
    return [link for link in links if should_visit(link)] #do any more last minute filtering on links one at a time

def remove_visited_links(links, db, cache):
    unvisited_links = filter(lambda x: x in cache, links)
    #TODO: check links against database
    return unvisited_links

def compute_word_frequencies(text):
    tokens = re.split("[^a-zA-Z0-9]+", text.lower()) #split on non-alphanumeric chars

    freqs = defaultdict(int)
    for token in tokens:
        if token in freqs:
            freqs[token] += 1
        else:
            freqs[token] = 1

    return freqs

def should_visit(link): 
    return is_valid(link)

def extract_next_links(url, resp):
    urls = set()
    soup = BeautifulSoup(resp.raw_response.text, 'html.parser')
    for url in soup.find_all('a'):
        urls.add(url.get('href'))

    text = soup.get_text(" ", strip = True)
    
    return list(urls), text 

def is_valid(url):
    try:
        if not url: return False
        # Append '//' to beginning of url in order for urlparse to detect netloc
        split_url = url.split('//')
        if len(split_url) > 2:
            return False 
        elif len(split_url) == 1:
            url = '//' + url

        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        domain_match = re.match(
            r"(today\.uci\.edu\/department\/information_computer_sciences\/?).*"
            + r"|.*(\.ics\.uci\.edu\/?).*|.*(\.cs\.uci\.edu\/?).*"
            + r"|.*(\.informatics\.uci\.edu\/?).*|.*(\.stat\.uci\.edu\/?).*", url.lower())
        type_match = re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv" 
            + r"|r|c|cpp|java|python|m"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        return (domain_match and not type_match) 
     

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def has_visited(url, visited_urls):
    pass
    