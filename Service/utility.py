import re
import itertools
import random
from datetime import datetime, timedelta

def spin(text):
    """
    Take a spin text, like "{Hi|Hello} I am - {Me|You|Us}", to a list of all possible combinations:
            'Hi I am - You'
            'Hello I am - Me'
            'Hi I am - Us'
            'Hello I am - You'
    The code below is referred from https://stackoverflow.com/questions/17634849/python-3-spinning-text
    :param text:
    :return: a list
    """
    def options(s):
        if len(s) > 0 and s[0] == '[':
            return [opt for opt in s[1:-1].split('|')]
        return [s]

    resulttext=[]
    p = re.compile('(\[[^\]]+\]|[^\[\]]*)')
    frags = p.split(text)
    opt_lists = [options(frag) for frag in frags]
    for spec in itertools.product(*opt_lists):
        resulttext.append(''.join(spec))
    return resulttext

def spinner(text):
    resulttext = spin(text)
    n = random.randint(0, len(resulttext) - 1)
    return resulttext[n]

def convertObjectArrayToDict(inputArray, attrNameList, keyDeliminator="--"):
    outputDict = {}
    for dataObject in inputArray:
        keyList = []
        for attrName in attrNameList:
            keyList.append(str(getattr(dataObject, attrName)))
        keyString = keyDeliminator.join(keyList)
        outputDict[keyString] = dataObject
    return outputDict

def string2Time(timeStr):
    # Covert a timeStr to time format
    # Assumption: the timeStr format is either "yyyy-mm-dd hh-mm-ss" format or "dd/mm/yyyy"
    try:
        time = datetime.fromisoformat(timeStr)
    except ValueError:
        temp = timeStr.split('/')
        if len(temp) == 3:  # check if the len is 3.
            timeStr = temp[2] + "-" + temp[1] + "-" + temp[0]
            time = datetime.fromisoformat(timeStr)
        else:
            time = None
    return time

def findASender(mappedSenders):
    loading = 100000000  # a very big sender
    senderId = ""
    # A full scan, not effecient, but assume the sender of core senders is very small
    for s in mappedSenders:
        if mappedSenders[s].loading <= loading:
            senderId = s
            loading = mappedSenders[s].loading
    mappedSenders[senderId].loading +=1
    return senderId, mappedSenders[senderId].userId


def getPrice(priceStr):
    priceFormatted = re.sub("\s|,|\+|(未補)|起|面議|餘|@\$?\d*", "", priceStr)
    a = b = c = d = e = f = 0
    match = re.search("\d+(\.\d+)?B", priceFormatted)
    if match:
        f = float(re.search("\d+(\.\d+)?", match.group()).group())
    match = re.search("\d+(\.\d+)?億", priceFormatted)
    if match:
        a = float(re.search("\d+(\.\d+)?", match.group()).group())
    match = re.search("\d+(\.\d+)?M", priceFormatted, re.IGNORECASE)
    if match:
        b = float(re.search("\d+(\.\d+)?", match.group()).group())
    match = re.search("\d+(\.\d+)?萬", priceFormatted, re.IGNORECASE)
    if match:
        c = float(re.search("\d+(\.\d+)?", match.group()).group())
    match = re.search("\d+(\.\d+)?K", priceFormatted, re.IGNORECASE)
    if match:
        d = float(re.search("\d+(\.\d+)?", match.group()).group())
    match = re.search("\d+(\.\d+)?$", priceFormatted, re.IGNORECASE)
    if match:
        e = float(re.search("\d+(\.\d+)?", match.group()).group())
    return int(f * 1000000000 + a * 100000000 + b * 1000000 + c * 10000 + d * 1000 + e)