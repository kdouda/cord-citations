import glob
from urllib.parse import unquote
import pickle
import gzip
import csv
import sys

# todo - parameter
path = "G:\\*.ttl"
save_path = "G:\\results\\"

last_subject = ""
triples = []

keep_triple = False

dois = {}

# dois["10.1007/978-3-642-85159-9"] = True

with open('sample_file.csv', 'r', encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        dois[row["doi"]] = True


with open('subselect.ttl', 'w', encoding="utf-8") as writefile:

    def clear_buffer(new_subject):
        global last_subject, keep_triple, writefile

        if keep_triple:
            writefile.writelines(triples)

        triples.clear()
        last_subject = new_subject


    for path in glob.glob(path):
        print(path)

        i = 0

        with open(path, 'r') as file:
            line = file.readline()

            while line:
                # skip whitespace (possibly inefficient?)
                if not line.strip():
                    line = file.readline()
                
                    if not line:
                        break

                    continue

                i = i + 1
                
                if i % 100000 == 0:
                    print(i)

                parts = line.split(" ")

                subject = parts[0].replace("<", "").replace(">", "")
                predicate = parts[1].replace("<", "").replace(">", "")
                object = parts[2].replace("<", "").replace(">", "")


                if predicate == "http://purl.org/spar/cito/hasCitedEntity":
                    current_doi = unquote(object).replace("http://dx.doi.org/", "")

                    if current_doi in dois:
                        keep_triple = True
                    else:
                        keep_triple = False

                if subject != last_subject:
                    clear_buffer(subject)

                triples.append(line)

                line = file.readline()

                if not line:
                    clear_buffer(subject)
                    break