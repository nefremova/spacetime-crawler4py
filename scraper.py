import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from database import Database
import hashlib
from utils import split_url, get_urlhash
from nltk.util import ngrams
import requests

def scraper(url, resp, db, url_cache, fingerprint_cache):
    url_cache[url] = 0 # add url to visited cache no matter what

    if resp.status >= 600: # invalid domain (this should not happen)
        print("Status", resp.status, ":", resp.error)
        return ([], 0)
    elif resp.status >= 400: # not ok
        print("Status", resp.status, ":", resp.raw_response)
        # 408 Request Timeout = you can try again 
        # at a later time with the same request headers
        if resp.status == 408:
            return ([url], 0)

        # all other 4xx codes, you cannot resend 
        # request without modifying request headers,
        # which we don't control
        return ([], 0)
    elif resp.status >= 300:    #redirection
        url_hash = get_urlhash(url)

        redirect_loc = resp.raw_response.headers['location']
        if not redirect_loc:    #this shouldn't happen
            return ([], 0)

        print("Redirect to", redirect_loc)
        return ([urljoin(url, redirect_loc)], 0)

    elif resp.status < 200: # not ok
        print("Status", resp.status, ":", resp.raw_response)
        return ([], 0)

    if 'Content-Type' in resp.raw_response.headers and resp.raw_response.headers['Content-Type'].find("text") == -1: # content type of document isn't text
        return ([], 0)
    
    if 'Content-Length' in resp.raw_response.headers:
        content_length = int(resp.raw_response.headers['Content-Length'])
        if content_length < 1500 or content_length > 6000000: 
            # numbers based roughly on http://www.blankwebsite.com/ for lower bound and https://www.seoptimer.com/blog/webpage-size/ for upper bound
            return ([], 0)

    # TEXT PARSING
    links, text = extract_next_links(url, resp) # get links + text
    tokens = get_tokens(text)
    tokens_no_stop = remove_stop_words(tokens)
    page_len = len(tokens_no_stop)

    # COMPUTE FINGERPRINT 
    fingerprint = create_fingerprint(tokens)
    if similar_page_exists(fingerprint, fingerprint_cache):
        return ([], page_len)

    url_hash = get_urlhash(url)
    fingerprint_cache[url_hash] = [fingerprint, 0]

    # print(links)

    # GET FREQUENCIES
    freqs = compute_word_frequencies(tokens)
    db.upsert_word_counts(freqs)
    
    # RETURN NEW LINKS
    to_visit = [link for link in links if should_visit(link, db, url_cache)]
    # print(t)
    # input("Hit enter when ready: ")
    return (to_visit, page_len)

def similar_paths(url, url_cache):
    domain1, subdomain1, path1 = split_url(url)
    if not path1:
        return False
    if path1.find("?") != -1:
        return False

    for link in url_cache:
        domain2, subdomain2, path2 = split_url(link)
        if not path2:
            continue
        if domain1 != domain2 and subdomain1 != subdomain2:
            continue

        path1 = path1[(path1.rfind('/')):]
        path2 = path2[(path2.rfind('/')):]

        intersection = 0
        smaller = path1 if len(path1) < len(path2) else path2
        if (len(smaller)) == 0:
            continue

        for i in range(len(smaller)):
            if path1[i] == path2[i]:
                intersection += 1

        similarity = (intersection)/(len(smaller))

        if similarity > 0.75:
            print("==========similar paths============")
            print(url)
            print(link)
            return True

def similar_page_exists(fingerprint, cache):
    print_set = set(fingerprint)

    for url_hash in cache:
        print_list = cache[url_hash][0]
        if dup_check(print_set, print_list):
            print("PRINT MATCH")  
            cache[url_hash][1] += 1
            return True

    return False

def is_trap(url):
    domain, subdomain, path = split_url(url)
    if subdomain.find("archive") != -1:
        return True
    if subdomain.find("calendar") != -1:
        return True
    if path.find("calendar") != -1:
        return True
    if path.find("stayconnected") != -1:
        return True
    if path.find("hall_of_fame") != -1:
        return True
    if path.find("sidebyside") != -1:
        return True
    if path.find("replytocom") != -1:
        return True 
    if subdomain.find("wics") != -1 and path.find("event") != 1 and path.find("list") == -1:
        return True
    return False

def is_visited(link, db, cache):
    if link in cache:
        cache[link] += 1
        return True
    elif db.url_exists(split_url(link)):
        return True
    else:
        return False

def create_fingerprint(words):
    n = 3    # going to create 3-grams
    hash_vals = []
    n_grams = ngrams(words, n)

    for gram in n_grams:
        gram = " ".join(gram)
        hex_hash = hashlib.sha256(gram.encode()).hexdigest()
        hex_hash = int(hex_hash, 16)
        if hex_hash % (n + 10) == 0:
            hash_vals.append(hex_hash)

    return hash_vals

def dup_check(prints1, prints2):
    if (len(prints1) + len(prints2)) == 0:
        print("BOTH PRINT LISTS EMPTY")
        return False

    threshold = 0.75

    set2 = set(prints2)
    intersection = prints1 & set2
    union = prints1 | set2
    similarity = len(intersection)/(len(union))

    if similarity > 0.50:
        print("==============similarity", similarity, "=======================") 
        print("len(prints1) = ", len(prints1))
        print("len(set2) = ", len(set2))

    return similarity > threshold

def remove_stop_words(tokens):
    stop_words = [
        "a", "about", "above", "after", "again", "against", 
        "all", "am", "an", "and", "any", "are", "aren't", "as", "at", 
        "be", "because", "been", "before", "being", "below", "between", 
        "both", "but", "by", "can't", "cannot", "could", "couldn't", 
        "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", 
        "each", "few", "for", "from", "further", "had", "hadn't", "has", 
        "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", 
        "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", 
        "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", 
        "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", 
        "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", 
        "ought", "our", "ours", "ourselves", "out", "over", "own", 
        "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", 
        "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", 
        "them", "themselves", "then", "there", "there's", "these", "they", "they'd", 
        "they'll", "they're", "they've", "this", "those", "through", "to", "too", 
        "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", 
        "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", 
        "where", "where's", "which", "while", "who", "who's", "whom", "why", 
        "why's", "with", "won't", "would", "wouldn't", 
        "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]

    return [token for token in tokens if token not in stop_words]
            

def get_tokens(text):
    # tokenize on 2+ characters, must contain at least 1 letter, can contain periods & apostrophes but not at the start or end
    return re.findall(r"[a-z0-9]+[a-z][a-z0-9]*[\.']*[a-z0-9\.]*\b", text.lower())

def compute_word_frequencies(tokens):
    freqs = dict()
    for token in tokens:
        if token in freqs:
            freqs[token] += 1
        else:
            freqs[token] = 1

    return freqs 

def should_visit(link, db, url_cache): 
#    return is_valid(link) and not is_visited(link, db, url_cache) and not is_trap(link)
    return is_valid(link) and not is_visited(link, db, url_cache)

def extract_next_links(link, resp):
    urls = set()
    soup = BeautifulSoup(resp.raw_response.text, 'html.parser')
    for url in soup.find_all('a'):
        url = url.get('href')
        if not url  or url == "/" or url[0] == "#":    #will get rid of some fragments
            continue
        else:
            url = urldefrag(url)[0] # remove the rest of the fragments
            urls.add(urljoin(link, url))   #if url is relative, will create absolute
            # https://docs.python.org/3/library/urllib.parse.html?highlight=urllib%20urljoin#urllib.parse.urljoin
            # reference for above just in case because that's crazy

    text = soup.get_text(" ", strip = True)
    return list(urls), text 

def is_valid(url):
    try:
        if not url: return False

        if url.find("%5B%5D") != -1 or url.find("?share=") != -1 or url.find("?replytocom=") != -1:
            return False

        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        edu_domain = re.match(".*\.edu.*", url.lower())
        if not edu_domain:
            return False
        outside_domain = re.match(
                        r".*\.com.*|.*\.co.*|.*\.org.*|.*\.pt.*"
                        + r"|.*\.net.*|.*\.gov.*|.*\.info.*", url.lower())
        if outside_domain:
            return False
        domain_match = re.match(
            r".*(\/\/)(today\.uci\.edu\/department\/information_computer_sciences\/?).*"
            + r"|.*(\.ics\.uci\.edu\/?).*|.*(\.cs\.uci\.edu\/?).*"
            + r"|.*(\.informatics\.uci\.edu\/?).*|.*(\.stat\.uci\.edu\/?).*", url.lower())

        type_match = re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico|img|webp"
            + r"|png|tiff?|mid|mp2|mp3|mp4|ds_store"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|ppsx|doc|docx|xls|xlsx|names|txt|scm"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|ss|ics|apk"
            + r"|epub|dll|cnf|tgz|sha1|json|pub|ppk|log"
            + r"|thmx|mso|arff|rtf|jar|csv|sql|ova" 
            + r"|gctx|npy|gz|npz|bgz|pbtxt|model|hdf5|seq"
            + r"|bed|bw|bz2|bam|bai|fasta|mod|test"
            + r"|r|c|cpp|java|python|m|py|mat|war"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

        return True if (domain_match and not type_match) else False
     

    except TypeError:
        print ("TypeError for ", parsed)
        raise

    
