# Junior-Data-Engineer-Assignment
Ideea mea a fost initial sa import csv-urile in programul meu, am dat merge de tip "inner" la dataset-urile facebook si google dupa coloana "domain" din punctul meu de vedere fiind coloana relevanta pentru merge, dupa ce am transformat toate valorile in litere mici, cu dataframe-ul rezultat am facut merge si cu website_dataset in acelasi mod, dupa aceste merge-uri am observat ca apar linii dublicate din cauza faptului ca in csv-ul google sunt domenii care se repeta, dar unele neavand restul datelor asemanatoare cu cele din facebook si website.
Am folosit "fuzzywuzzy" pentru a compara asemanarile in functie de coloanele relevante si am dat drop la liniile care nu aveau legatura(ps. aveau legatura daca minim 3 aveau procent de peste 80% asemanare sau o comparatie dintre coloanele importante "name","category" aveau 100%).
Am creeat o functie care pana sa dea drop la coloanele duble de dupa merge si alege din cele 2 valoarea relevanta( am reusit doar daca una este null sa o aleaga pe cea care are valoare, am incercat sa verifice si daca una din cele 2 are caractere speciale sa o aleaga pe cealalata, dar nu mi-a iesit).
Daca tot raman linii multiple cu aceeasi valoare "domain" sterg copiile, iar rezultatul l-am salvat intr-un nou csv.


ps. Trebuie modificata calea catre seturile de date.
