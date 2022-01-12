############
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os 
from copy import deepcopy
import pandas as pd
import xmltodict  

nci_version   = "21.11e"  
owl_folder    = "/Users/mpatel/terminology/NCIT/"
base_folder   = "/Users/mpatel/Desktop/NCIt"
output_folder = os.path.join(base_folder, nci_version)
json_folder   = os.path.join(output_folder, "json")

os.makedirs(output_folder, exist_ok=True)
os.makedirs(json_folder, exist_ok=True)

flatten = lambda l: [item for sublist in l for item in sublist]
def pintersect(L, R):
    x = set(L).intersection(set(R))
    print('(L ∩ R)/L :',str(len(x)) + "/" + str(len(set(L))) ,round(len(x)/len(set(L)),3))
    print('(L ∩ R)/R :',str(len(x)) + "/" + str(len(set(R))) , round(len(x)/len(set(R)),3))

def save(v, n, folder = json_folder):
    #simplify saving
    with open(os.path.join(folder,n+'.json'), 'w') as f:
        json.dump(v, f, indent = '\t')

def load(n, folder = json_folder):
    #simplify saving
    with open(os.path.join(folder,n+'.json'), 'r') as f:
        return json.load(f)

# OWL to JSON        
# read in both base and inferred
# note inferred leaves out all retired concepts, so there is an n diffential
# Inferred inherits relationships
print("Loading OWL files...", flush = True)
with open(os.path.join(owl_folder,"ThesaurusInferred.owl"), "rb") as f:
    ncit = xmltodict.parse(f)
print("  Inferred OWL successfully loaded!", flush = True)

with open(os.path.join(owl_folder, "Thesaurus.owl"), "rb") as f:
    ncit_no_inheritence = xmltodict.parse(f)
print("  Non-Inferred OWL successfully loaded!", flush = True)


print("Processing data into JSON...", flush = True)

ncit_no_inheritence = ncit_no_inheritence['rdf:RDF']['owl:Class']
ncit_anp = ncit['rdf:RDF']['owl:AnnotationProperty']
print("  AnnotationProperty complete!", flush = True)
ncit_axioms = ncit['rdf:RDF']['owl:Axiom']
print("  Axiom complete!", flush = True)
ncit_datatypes = ncit['rdf:RDF']['rdfs:Datatype']
print("  Datatype complete!", flush = True)
ncit_objp = ncit['rdf:RDF']['owl:ObjectProperty']
print("  ObjectProperty complete!", flush = True)
ncit_classes = ncit['rdf:RDF']['owl:Class']
print("  Class complete!", flush = True)

#ncit_classes_unmapped = ncit['rdf:RDF']['owl:Class']

# save each section:
print("Saving JSON files...", flush = True)
save(ncit_anp,"ncit_anp")
save(ncit_axioms,"ncit_axioms")
save(ncit_datatypes,"ncit_datatypes")
save(ncit_objp,"ncit_objp")
save(ncit_classes,"ncit_classes")
print("  JSON files saved successfully!", flush = True)



print("Cleaning up JSON...", flush = True)
# ncit_classes = load('ncit_classes')

# AnnotationProperties get anp dict, accounting for 
# their fuckups on legacy concept name
get_pythonic_name = lambda x: x['P108'] if x.get('P108') and " " not in x['P108'] else x['rdfs:label']
anp_dict = {i['NHC0']:get_pythonic_name(i) for i in ncit_anp if i.get('P108') and i.get('NHC0')}

# object properties, these are the relationship properties. get a dict for names
# they fucked up R39|Gene_Is_Biomarker_Of, so use the label for that                          
objp_dict = {i['@rdf:about']:get_pythonic_name(i) for i in ncit_objp if i.get('NHC0')}
 

# NCIT Enums, just in case for later. they are listed recursively weirdly
# def get_enum_members(node, members):
#     first = node["rdf:Description"]["rdf:first"]
#     members = members + [first]
#     if node["rdf:Description"].get('rdf:rest') and node["rdf:Description"]['rdf:rest'].get("@rdf:resource","") != "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil":
#         members = get_enum_members(node["rdf:Description"].get('rdf:rest'), members)
#     return members
   
# ncit_enums = []
# for i in ncit_datatypes:
#     if i["@rdf:about"].endswith('-enum'):
#         members = get_enum_members(i["owl:equivalentClass"]["rdfs:Datatype"]["owl:oneOf"],[])
#         ncit_enums.append({'id': i["@rdf:about"],
#                            'name': i['@rdf:about'].split('#')[1],
#                            'members': members})


# next determine the pattern on relationship properties.
# see parse_owl.py for the laborious process, and property_patterns and schema_store.json for results                           
# we force all these properties into list elements so a lambda function for convenience
force_list = lambda x: x if isinstance(x,list) else [x]
def add_properties_to_existing(existing, new):
    # combines property dicts by resolving duplicates and loading the lists
    #print("add_properties_to_existing", existing, new)
    for k, v in new.items():
        if k not in existing.keys():
            existing[k] = v
        else:
            for v_item in v:
                if v_item not in existing[k]:
                    existing[k] = existing[k] + [v_item]
    return existing

def give_disjoint_to(v):
    # creates a disjointWith property
    # print("give_disjoint_to", v)
    return {"disjointWith": [{"concept": i['@rdf:resource']} for i in force_list(v)]}

def give_group(v):
    # creates a group of properties to pass up the recursion
    # print("give_group", v)
    this_properties = {}
    for entry in force_list(v):
        for k, v in entry.items():
            if k in function_map.keys():
                new_properties = function_map[k](v)
                this_properties = add_properties_to_existing(this_properties, new_properties)
    return this_properties


def give_subclass_of(v):
    # gives a subclass property using the two discovered patterns
    # print("give_subclass_of", v)
    if isinstance(v, str):
        return {"subClassOf": [{"concept": v}]}
    else:
        return {"subClassOf": [{"concept": i["@rdf:about"]} for i in force_list(v)]}

def give_object_property(v):
    # gives an object property using the obj_dict
    # print("give_object_property", v)
    all_props = {}
    for i in force_list(v):
        property_name = objp_dict.get(i['owl:onProperty']["@rdf:resource"])            
        c_ref = {"concept": i["owl:someValuesFrom"]["@rdf:resource"]}
        all_props[property_name] = all_props.get(property_name,[]) + [c_ref]
    return all_props

# patterns in the object properties call specific functions
function_map = {"owl:Class": give_group,
                "owl:equivalentClass": give_group,
                "rdfs:subClassOf": give_group,
                "owl:intersectionOf": give_group,
                "owl:unionOf": give_group,
                "owl:disjointWith": give_disjoint_to,
                "owl:Restriction": give_object_property,
                "rdf:Description": give_subclass_of,
                "@rdf:resource": give_subclass_of
                }


#the three root level keys that suggest a concept relationship
complex_subs = ['owl:disjointWith', 'owl:equivalentClass', 'rdfs:subClassOf']

###### Concept ref model ########################################################
# {"concept": concept_id, "type": in [annotation, asserted, inherited]} #
#################################################################################
       
       
# now we get novel properties such that we can determine inherited vs asserted
novel_props = {}

for i in ncit_no_inheritence:
    which = i.get("@rdf:about", "fuckup")
    this_properties = {}
    for n in complex_subs:
        if i.get(n):
            new_properties = function_map[n](i[n])
            this_properties = add_properties_to_existing(this_properties, new_properties)
    rel_strings = []
    for k,v in this_properties.items():
        for i in v:
            rel_strings.append(k +"|" + i['concept'])
   
    novel_props[which] = rel_strings
   



# fix the silly dict comments, there are 2
for i in ncit_classes:
    if i.get("rdfs:comment") and isinstance(i["rdfs:comment"],dict):
        concept = i["rdfs:comment"]["@rdf:resource"].split("#")[-1]
        i["rdfs:comment"] = "See concept: " + concept
       
       
# AnnotationProperties, fixed their type to conform to our concept ref model
# remove that one problematic string
       
# annotation_property list, these all result in concept relationships
a_props = [i for i in anp_dict.keys() if i.startswith('A')]        
for c in ncit_classes:
    for p in a_props:
        if c.get(p):
            v = force_list(c[p])
            if "caDSR General" in v:
                v.remove("caDSR General")
            v = [{"concept": i["@rdf:resource"], "type": "annotation"} for i in v]
            c[p] = v
           

# now lets create the concept relationships, disjoint, subclass, and obj_props
# also tagging them as asserted or inherited using novel_props from above  

for i in ncit_classes:
    this_properties = {}
    for n in complex_subs:
        if i.get(n):
            new_properties = function_map[n](i[n])
            this_properties = add_properties_to_existing(this_properties, new_properties)
            i.pop(n)
   
    this_novel_props = novel_props.get(i["@rdf:about"], [])
    for k, v in this_properties.items():
        for item in v:
            if k + "|" + item['concept'] in this_novel_props:
                item['type'] = "asserted"
            else:
                item['type'] = "inherited"
    i["Relationships"] = this_properties
   


   
# finally, lets map all properties to their proper name
# fixing the three other root rdf props
correct_rdf_props = {"@rdf:about": "concept_id", "rdfs:label": "label", "rdfs:comment": "comment"}
   
map_class = lambda c: dict((anp_dict.get(key, correct_rdf_props.get(key,key)), val) for key, val in c.items())

ncit_classes = [map_class(i) for i in ncit_classes]

# move all annotation properties to relationships
   
a_prop_names = [anp_dict[i] for i in a_props]
for i in ncit_classes:
    if not i.get("Relationships"):
        i["Relationships"] = {}
    for k in a_prop_names:
        if i.get(k):
            i["Relationships"][k] = deepcopy(i[k])
            i.pop(k)

for i in ncit_classes:
    if type(i['FULL_SYN']) is not list:
        i['FULL_SYN'] = [i['FULL_SYN']]


save(ncit_classes, "ncit_cleaned")
print("  JSON cleaned and saved successfully!", flush = True)


print("Processing JSON into nodes and edges...", flush = True)
ncit_nodes = pd.DataFrame(ncit_classes)
ncit_nodes = ncit_nodes.drop('Relationships', axis = 1)

list_map = (ncit_nodes.applymap(type) == list).any()
force_list = lambda x: x if isinstance(x,list) else [x]

for k, v in list_map.items():
    if v:
        ncit_nodes[k] = ncit_nodes[k].apply(lambda s: force_list(s))
        ncit_nodes[k] = ncit_nodes[k].str.join("|")

def fix_col_header(h):
    if h == 'code':
        return 'code:ID'
    elif list_map.get(h):
        return h + ":string[]"
    else:
        return h
ncit_nodes[':LABEL'] = 'NCIT|' + ncit_nodes.Semantic_Type.str.replace(",","").str.replace(" ", "_")
ncit_nodes_headers = [fix_col_header(k) for k in ncit_nodes.columns]


def convert_to_edge(c):
    these_rels = []
    source = c['code']
    for k, v in c['Relationships'].items():
        for targ in v:
            these_rels.append({'source': source, 'rel_type': k, 'target': targ['concept'].split("#")[-1], "rel_cat": targ['type']})
    return these_rels
       

        
ncit_edges = [convert_to_edge(c) for c in ncit_classes]
ncit_edges = flatten(ncit_edges)
ncit_edges = pd.DataFrame(ncit_edges)
probs = ncit_edges[(~ncit_edges.target.str.startswith('C')) | (~ncit_edges.source.str.startswith('C'))]
ncit_edges= ncit_edges[ncit_edges.target.isin(ncit_nodes.code)]

# check out probs


ncit_edges['target'] = ncit_edges['target'].apply(lambda s: "C" + s if not s.startswith("C") else s)
print("   JSONS succesfully converted!", flush = True)

print("Saving nodes and edges as csvs...", flush = True)
ncit_nodes.to_csv(os.path.join(output_folder,"nodes.csv"), index = False, header = True)
ncit_edges.to_csv(os.path.join(output_folder,"edges.csv"), index = False, header = True) 
print("   Nodes and edges csvs successfully saved!", flush = True)
