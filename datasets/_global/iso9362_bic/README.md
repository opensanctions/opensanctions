
Source: https://www.iso9362.org/isobic/overview.html 
PDF: https://www.iso9362.org/bicservice/public/v1/bicdata/_pdf 



```
java -jar tabula-1.0.5-jar-with-dependencies.jar ~/Data/isobic/ISOBIC.pdf -o iso.csv --pages all
```


Refine:
```
value.replace("\n", " ").replace("  ", " ").replace("  ", " ")
```