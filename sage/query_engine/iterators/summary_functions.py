from rdflib import Literal, URIRef
from urllib.parse import urlparse

import re


def isRDFType(predicate):
    return predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def replace_non_alphanumeric_characters(string):
    return re.sub("[^0-9a-zA-Z:\./#_-]+", "_", string)


def hashcode(s, m):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return (((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000) % m


def uri_suffix_transformation(uri, path_modulo, len_suffix):
    url = urlparse(uri)
    scheme = "http" if url.scheme == '' else url.scheme
    netloc = url.netloc
    path = ''
    suffix = ''
    if not url.fragment == '' :
        suffix = f"/{url.fragment[-len_suffix:]}"
        path = f"/{hashcode(url.path, path_modulo)}"
    elif not url.path == '':
        path = url.path.rsplit('/', 1)
        if len(path[1]) == 0:
            if path[0].startswith('/') and len(path[0]) < len_suffix:
                suffix = path[0][-len_suffix:]
            else:
                suffix = f"/{path[0][-len_suffix:]}"
            path = ''
        else:
            suffix = f"/{path[1][-len_suffix:]}"
            path = f"/{hashcode(path[0], path_modulo)}"
    suffix = replace_non_alphanumeric_characters(suffix)
    return f"{scheme}://{netloc}{path}{suffix}"


def uri_prefix_transformation(uri, path_modulo, len_prefix):
    url = urlparse(uri)
    scheme = "http" if url.scheme == '' else url.scheme
    netloc = url.netloc
    path = ''
    prefix = ''
    if not url.fragment == '' :
        prefix = f"/{url.fragment[:len_prefix]}"
        path = f"/{hashcode(url.path, path_modulo)}"
    elif not url.path == '':
        path = url.path.rsplit('/', 1)
        if len(path[1]) == 0:
            if path[0].startswith('/'):
                prefix = path[0][:len_prefix + 1]
            else:
                prefix = f"/{path[0][:len_prefix]}"
            path = ''
        else:
            prefix = f"/{path[1][:len_prefix]}"
            path = f"/{hashcode(path[0], path_modulo)}"
    prefix = replace_non_alphanumeric_characters(prefix)
    return f"{scheme}://{netloc}{path}{prefix}"


def uri_hash_transformation(uri, path_modulo, resource_modulo):
    url = urlparse(uri)
    scheme = "http" if url.scheme == '' else url.scheme
    netloc = url.netloc
    path = ''
    resource = ''
    if not url.fragment == '' :
        resource = f"/{hashcode(url.fragment, resource_modulo)}"
        path = f"/{hashcode(url.path, path_modulo)}"
    elif not url.path == '':
        path = url.path.rsplit('/', 1)
        if len(path[1]) == 0:
            resource = f"/{hashcode(path[0], resource_modulo)}"
            path = ''
        else:
            resource = f"/{hashcode(path[1], resource_modulo)}"
            path = f"/{hashcode(path[0], path_modulo)}"
    return f"{scheme}://{netloc}{path}{resource}"


def uri_hib_transformation(uri):
    url = urlparse(uri)
    scheme = "http" if url.scheme == '' else url.scheme
    netloc = url.netloc
    return f"{scheme}://{netloc}"


def uri_simple_transformation(uri):
    return "http://any"


def literal_simple_transformation(literal):
    return "http://literal"


def literal_hash_transformation(literal, modulo):
    value = utils.hashcode(literal, modulo)
    return f"http://literal/{value}"


def literal_prefix_transformation(literal, len_prefix):
    value = replace_non_alphanumeric_characters(literal[:len_prefix])
    return f"http://literal/{value}"


def psi_id(triple):
    (s, p, o) = triple
    return (f"<{s}> <{p}> <{o}> .")


def psi_suf(triple, path_modulo=10, len_suffix=2):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_suffix_transformation(s, path_modulo, len_suffix)
    if o.startswith("http"):
        o = uri_suffix_transformation(o, path_modulo, len_suffix) if not isRDFType(p) else o
    else:
        o = literal_simple_transformation(o)
    return(f"<{s}> <{p}> <{o}> .")


def psi_pref(triple, path_modulo=10, len_prefix=2):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_prefix_transformation(s, path_modulo, len_prefix)
    if o.startswith("http"):
        o = uri_prefix_transformation(o, path_modulo, len_prefix) if not isRDFType(p) else o
    else:
        o = literal_simple_transformation(o)
    return(f"<{s}> <{p}> <{o}> .")



def psi_po(triple, path_modulo=10, len_prefix=6):
    (s, p, o) = triple
    s = uri_simple_transformation(s)
    if isinstance(o, URIRef):
        o = uri_prefix_transformation(o, path_modulo, len_prefix)
    elif isinstance(o, Literal):
        o = literal_prefix_transformation(o, len_prefix)
    return (s, p, o)


def psi_hib(triple):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_hib_transformation(s)  
    if o.startswith("http"):
        o = uri_hib_transformation(o) if not isRDFType(p) else o
    else:
        o = literal_simple_transformation(o)
    return(f"<{s}> <{p}> <{o}> .")


def psi_hash(triple, path_modulo=1, resource_modulo=500, literal_modulo=500):
    (s, p, o) = triple
    if s.startswith("http"):
        s = uri_hash_transformation(s, path_modulo, resource_modulo)
    if o.startswith("http"):
        o = uri_hash_transformation(o, path_modulo, resource_modulo) if not isRDFType(p) else o
    else:
        o = literal_hash_transformation(o, literal_modulo)
    return (s, p, o)


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