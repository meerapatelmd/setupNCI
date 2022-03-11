# setupNCI 1.3.0   

* Added NCI Version 22.02d  


## New Features  

* Process NCI Thesaurus OWL files into OMOP Vocabulary tables  

* `check_owl_version()` that compares FTP version to what is in the installation directory.  

## Bug Fixes  

* Added `conn_fun` evaluation  

* Separated `run_setup()` to `nci` schema to 
NCI Metathesaurus (`ncim`) and NCI Thesaurus (`ncit`) with 
separate logs at scale of 1 row per update.  

* Removed instantiation of Code CUI Map  


# setupNCI 1.2.0    

* MRHIER fails to copy due to unknown reasons. The error 
message is as follows:  
```
ERROR:  extra data after last expected column
CONTEXT:  COPY mrhier, line 1234648: "C0220965|A0832257|1||PDQ||||||"
```
A `copy_mrhier()` is added that loads into a single field in a 
`RAW_MRHIER` table and transformed into the final `MRHIER` 
table using crosstab. A QA report gives a comparison of the 
row counts, first and last lines between the two tables.  

* MRHIER processing SQL script is added.  


# setupNCI 1.1.0  

* Fixed load error by expanding limit for CUI fields to 
10 characters because some NCI CUIs are prefixed with `CL`.  
* To prevent losing an existing `Thesaurus` and/or 
`Code To CUI Map` table/s, changed drop cascade of the 
target schema to dropping only NCI Metathesaurus tables.  


# setupNCI 0.1.0  

* Added first version  
