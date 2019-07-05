import re


def domain_slug(domain):
    return re.sub(r"http(s)|:|\/|www.?|\.", "", domain)
