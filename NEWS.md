# setupNCI 1.1.0.9000  

* MRHIER fails to copy due to unknown reasons. The error 
message is as follows:  
```
ERROR:  extra data after last expected column
CONTEXT:  COPY mrhier, line 1234648: "C0220965|A0832257|1||PDQ||||||"
```
A `copy_mrhier()` is added to copy using a crosstab.  

* MRHIER processing SQL script is added.  


# setupNCI 1.1.0  

* Fixed load error by expanding limit for CUI fields to 
10 characters because some NCI CUIs are prefixed with `CL`.  
* To prevent losing an existing `Thesaurus` and/or 
`Code To CUI Map` table/s, changed drop cascade of the 
target schema to dropping only NCI Metathesaurus tables.  


# setupNCI 0.1.0  

* Added first version  
