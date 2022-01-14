import glob
from urllib.parse import unquote
import csv
import sys
import threading
import time

lines = 0

last_subject = None
last_triples = []
dois = {}

with open('sample_file.csv', 'r', encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        dois[row["doi"]] = {}

dois["10.1371/journal.pbio.0030172"] = {}

class Triple:

    def __init__(self, subject : str, predicate : str, object : str) -> None:
        self.parse(subject, predicate, object)

        pass

    def parse(self, subject : str, predicate : str, object: str):
        self.subject = subject.replace("<", "")
        self.subject = self.subject.replace(">", "")

        self.predicate = predicate.replace("<", "")
        self.predicate = self.predicate.replace(">", "")

        self.object = object.replace("<", "")
        self.object = self.object.replace(">", "")

        self.subject = unquote(self.subject)
        self.predicate = unquote(self.predicate)
        self.object = unquote(self.object)

        pass

    def get_subject(self) -> str:
        return self.subject

doicount = 0

def check_array(arr):
    doi = None
    date = None

    for triple in arr:
        if triple.predicate == "http://purl.org/spar/cito/hasCitationCreationDate":
            curdate = triple.object.replace("\"", "")
            substr = curdate.find("^^")

            if substr:
                date = curdate[:substr]

        if triple.predicate == "http://purl.org/spar/cito/hasCitedEntity":
            doi = triple.object.replace("http://dx.doi.org/", "")

    if doi:
        if doi in dois:

      

            if date:
                year = date.split("-")[0]
            else:
                year = "?"

            if year not in dois[doi]:
                dois[doi][year] = 0

            dois[doi][year] = dois[doi][year] + 1
    
    pass

for filepath in glob.glob("./*.ttl"):
    with open(filepath, "r", encoding="utf-8") as file:
        
        line = file.readline()

        while True:

            if not line:
                break
            
            parts = line.split(" ")

            subject = parts[0].replace("<", "").replace(">", "").strip()
            predicate = parts[1].replace("<", "").replace(">", "").strip()
            object = parts[2].replace("<", "").replace(">", "").strip()

            if last_subject != subject:
                if last_subject != None:
                    check_array(last_triples)
                    last_triples.clear()
                    last_triples.append(Triple(subject, predicate, object))

                last_subject = subject
            else:
                last_triples.append(Triple(subject, predicate, object))

            line = file.readline()

            lines = lines + 1


with open('citations_by_year.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(["doi", "year", "count"])

    for doi, years in dois.items():
        for year, value in dois[doi].items():
            print(doi)

            writer.writerow([doi, year, value])


print(lines)
print(doicount)