import glob
from urllib.parse import unquote
import csv
import sys
import threading
import time

# todo - parameter
path = "G:\\*.ttl"
save_path = "G:\\results\\"

last_subject = ""
triples = []

keep_triple = False
files_per_thread = 20

dois = {}

print("Processing DOI file...")

with open('sample_file.csv', 'r', encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        dois[row["doi"]] = True

print("DOI file processed")
print("Looking up .ttl files in specified folder")

files = glob.glob(path)

print("Found " + str(len(files)) + " files")

print("Will process " + str(files_per_thread) + " in each thread")

files_chunked = []

for i in range(0, len(files), files_per_thread):
    files_chunked.append(files[i:i+files_per_thread])

iterator = 0

class DoiThread(threading.Thread):

    def initialize(self, identifier, files_to_parse, dois):
        self.last_subject = None
        self.current_triples = {}
        self.through_files = 0
        self.files = files_to_parse
        self.identifier = identifier
        self.dois = dois

    def run(self):
        with open("subselect_" + str(self.identifier) + ".ttl", "w", encoding="utf-8") as outfile:

            def clear_buffer():
                if "http://purl.org/spar/cito/hasCitedEntity" in self.current_triples:
                    doi = self.current_triples["http://purl.org/spar/cito/hasCitedEntity"].replace("http://dx.doi.org/", "")

                    if doi in self.dois:
                        for predicate, object in self.current_triples.items():
                            outfile.write("<" + self.last_subject + "> <" + predicate + "> <" + object + ">" + "\n")
                    else:
                        self.current_triples = {}
                else:
                    self.current_triples = {}

            for file in self.files:
                start = time.time()
                self.current_triples = {}

                #outfile.write(file)

                with open(file, 'r') as infile:
                    print("Parsing file " + str(self.through_files) + " in thread " + str(self.identifier))

                    for line in infile:
                        # skip whitespace (possibly inefficient?)
                        if not line.strip():
                            continue

                        parts = line.split(" ")

                        subject = parts[0].replace("<", "").replace(">", "")
                        predicate = parts[1].replace("<", "").replace(">", "")
                        object = parts[2].replace("<", "").replace(">", "")

                        if subject != self.last_subject and len(self.current_triples):
                            if self.last_subject == None:
                                self.last_subject = subject
                            else:
                                clear_buffer()
                                self.last_subject = subject
                        
                        self.current_triples[predicate] = object

                    clear_buffer()
                    #outfile.write("---")


                self.through_files = self.through_files + 1
                                
                end = time.time()
                print("Parsing of file took: " + str(end - start))


for file_list in files_chunked:
    file = file_list.copy()

    thread = DoiThread(name = "Thread-" + str(iterator))
    thread.initialize(iterator, file, dois)
    thread.start()

    iterator = iterator + 1
