# setupNCI 1.1.0.9000  

* MRHIER failed to copy due to unknown reasons. A `copy_mrhier()` 
is added to copy using a crosstab.  


# setupNCI 1.1.0  

* Fixed load error by expanding limit for CUI fields to 
10 characters because some NCI CUIs are prefixed with `CL`.  
* To prevent losing an existing `Thesaurus` and/or 
`Code To CUI Map` table/s, changed drop cascade of the 
target schema to dropping only NCI Metathesaurus tables.  


# setupNCI 0.1.0  

* Added first version  
