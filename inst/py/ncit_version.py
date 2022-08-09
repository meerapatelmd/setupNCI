############
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os 
import xmltodict  

owl_folder    = "{{{owl_folder}}}"


# Deriving the NCI Thesaurus version from the owl version information        
print("Getting NCI Thesaurus version from Thesaurus.owl...", flush = True)
with open(os.path.join(owl_folder, "Thesaurus.owl"), "rb") as f:
    ncit_no_inheritence = xmltodict.parse(f)
print("Getting NCI Thesaurus version from Thesaurus.owl... complete!", flush = True)
ncit_version = ncit_no_inheritence['rdf:RDF']['owl:Ontology']['owl:versionInfo']
print("  NCI Version: " + ncit_version, flush = True)
with open("dev/tmp_nci_version.txt", "w") as f: 
   f.write(ncit_version)

