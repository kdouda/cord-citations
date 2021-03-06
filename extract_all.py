import glob
import urllib.parse
import pickle
import gzip
import csv
import sys

# todo - parameter
path = "G:\\*.ttl"
save_path = "G:\\results\\"

entities = {}
skip_entities = {}

dois = {}

with open('sample_file.csv', 'r', encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        dois[row["doi"]] = True

class Collection:

    def __init__(self) -> None:
        self.dictionary = {}
        self.entities = {}
        pass

    def get_key(self, key: str) -> int:
        if key not in self.dictionary:
            self.dictionary[key] = len(self.dictionary)

        return self.dictionary[key]

    def add_entity(self, key, entity) -> None:
        self.entities[self.get_key(key)] = entity

        pass

    def has_entity(self, key) -> bool:
        return self.get_key(key) in self.entities

    def remove_entity(self, key) -> None:
        if self.get_key(key) in self.entities:
            self.entities.pop(self.get_key(key))

collection = Collection()

rdf_type_key = collection.get_key("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
entity_filter_key = collection.get_key("http://purl.org/spar/cito/Citation")
filter_by_predicate = collection.get_key('http://purl.org/spar/cito/hasCitedEntity')

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

        self.subject = collection.get_key(self.subject)
        self.predicate = collection.get_key(self.predicate)
        self.object = collection.get_key(self.object)

        pass

    def get_subject(self) -> str:
        return self.subject

class Entity:

    def __init__(self, iri) -> None:
        self.iri = collection.get_key(iri)
        self.type = None
        self.triples_keyed = {}

        pass

    def add_triple(self, triple : Triple) -> None:
        #self.triples.append(triple) # - what for?

        if triple.predicate not in self.triples_keyed:
            self.triples_keyed[triple.predicate] = []

        self.triples_keyed[triple.predicate].append(triple.subject)

        # todo - parametrize?

        if triple.predicate == rdf_type_key:
            self.type = triple.object

        pass

    def get_type(self):
        return self.type

def reset():
    global entites, skip_entities, collection, rdf_type_key, entity_filter_key
    
    entities = {}
    skip_entities = {}
    collection = Collection()

    rdf_type_key = collection.get_key("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    entity_filter_key = collection.get_key("http://purl.org/spar/cito/Citation")

file_counter = 0

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

            subject = parts[0]
            predicate = parts[1]
            object = parts[2]

            triple = Triple(subject, predicate, object)

            if triple.subject in skip_entities:
                line = file.readline()

                if not line:
                    break

                continue

            subject_entity = None
            object_entity = None

            if collection.has_entity(subject_entity):
                subject_entity = collection.get_key(subject, subject_entity) 
            else:
                subject_entity = Entity(subject)
                collection.add_entity(subject, subject_entity)

            if triple.predicate == filter_by_predicate:
                if triple.object not in dois:
                    collection.remove_entity(subject_entity.iri)
                    skip_entities[subject_entity.iri] = True

                    line = file.readline()

                    if not line:
                        break

                    continue

            subject_entity.add_triple(triple)

            if subject_entity.get_type():
                if subject_entity.get_type() != entity_filter_key:
                    skip_entities[subject_entity.iri] = True
                    collection.remove_entity(subject_entity.iri)

            line = file.readline()

            if not line:
                break

    file_counter = file_counter + 1

    with gzip.open(save_path + str(file_counter) + '.gz', 'wb') as f:
        pickle.dump(collection, f)

    print(file_counter)
    reset()


