from rdflib import Literal, URIRef
from urllib.parse import urlparse

import re

def isRDFType(predicate):
    return predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def replace_non_alphanumeric_characters(string):
    return re.sub("[^0-9a-zA-Z:\./#_-]+", "_", string)


def uri_suffix_transformation(uri, lenSuffix):
    url = urlparse(str(uri))
    scheme = "http" if url.scheme == '' else url.scheme
    netloc = url.netloc
    if not url.fragment == '':
        suffix = url.fragment[-lenSuffix:]
    elif url.path.endswith('/'):
        suffix = url.path[-(lenSuffix + 1):]
    else:
        suffix = url.path[-lenSuffix:]
    suffix = replace_non_alphanumeric_characters(suffix)
    return f"{scheme}://{netloc}/{suffix}"


def uri_prefix_transformation(uri, lenPrefix):
    url = urlparse(str(uri))
    scheme = "http" if url.scheme == '' else url.scheme
    netloc = url.netloc
    if url.path == '':
        return f"{scheme}://{netloc}"
    path = url.path.rsplit('/', 1)
    prefix = ''
    if len(path[1]) == 0:
        if path[0].startswith('/'):
            prefix = path[0][:lenPrefix + 1]
        else:
            prefix = f"/{path[0][:lenPrefix]}"
        path = ''
    else:
        prefix = f"/{path[1][:lenPrefix]}"
        path = path[0]
    prefix = replace_non_alphanumeric_characters(prefix)
    return f"{scheme}://{netloc}{path}{prefix}"


def hashcode(s, m):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return (((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000) % m


def uri_hashing(uri, modulo):
    url = urlparse(str(uri))
    scheme = "http" if url.scheme == '' else url.scheme
    netloc = url.netloc
    if url.path == '':
        return f"{scheme}://{netloc}"
    path = hashcode(f"{url.path}{url.fragment}", modulo)
    return f"{scheme}://{netloc}/{path}"


def uri_hib_transformation(uri):
    url = urlparse(str(uri))
    return f"{url.scheme}://{url.netloc}"


def uri_simple_transformation(uri):
    return "http://any"


def literal_simple_transformation(literal):
    return "http://literal"


def literal_hashing(literal, modulo):
    value = hashcode(str(literal), modulo)
    return f"http://literal/{value}"


def psi_id(triple):
    (s, p, o) = triple
    return (f"<{s}> <{p}> <{o}> .")


def psi_hib(triple):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_hib_transformation(s)  
    if o.startswith("http"):
        o = uri_hib_transformation(o) if not isRDFType(p) else o
    else:
        o = literal_simple_transformation(o)
    return(f"<{s}> <{p}> <{o}> .")


def psi_suf(triple, lenSuffix):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_suffix_transformation(s, lenSuffix)
    if o.startswith("http"):
        o = uri_suffix_transformation(o, lenSuffix) if not isRDFType(p) else o
    else:
        o = literal_simple_transformation(o)
    return(f"<{s}> <{p}> <{o}> .")


def psi_suf_2(triple):
    return psi_suf(triple, 2)


def psi_pref(triple, lenPrefix):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_prefix_transformation(s, lenPrefix)
    if o.startswith("http"):
        o = uri_prefix_transformation(o, lenPrefix) if not isRDFType(p) else o
    else:
        o = literal_simple_transformation(o)
    return(f"<{s}> <{p}> <{o}> .")


def psi_pref_2(triple):
    return psi_pref(triple, 2)


def psi_hash(triple, uri_modulo, literal_modulo):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_hashing(s, uri_modulo)
    if o.startswith("http"):
        o = uri_hashing(o, uri_modulo) if not isRDFType(p) else o
    else:
        o = literal_hashing(o, literal_modulo)
    return (s, p, o)


def psi_hash_1K_1K(triple):
    return psi_hash(triple, 1000, 1000)


def psi_void(triple):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_simple_transformation(s)
    if o.startswith("http"):
        o = uri_simple_transformation(o)
    else:
        o = literal_simple_transformation(o)
    return(f"<{s}> <{p}> <{o}> .")


def psi_one(triple):
    (s, p, o) = triple
    s = uri_simple_transformation(s)
    p = uri_simple_transformation(p)
    o = uri_simple_transformation(o)
    return(f"<{s}> <{p}> <{o}> .")