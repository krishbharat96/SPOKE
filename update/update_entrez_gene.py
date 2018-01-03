#!/usr/bin/python2
from __future__ import print_function

from paths import EntrezGene_CSV
from paths import SPOKE_URI, SPOKE_User, SPOKE_Password

CYPHER_Genes = """
    MATCH (n:Gene)
        WHERE exists(n.name)
        RETURN n.identifier       AS gene_id,
               n.name             AS name,
               n.description      AS description,
               n.chromosome       AS chromosome
"""
CYPHER_AddGene = """
    CREATE (n:Gene)
    SET n = {
        identifier: {gene_id},
        name: {name},
        description: {description},
        chromosome: {chromosome},
        license: "CC0 1.0",
        source: "Entrez Gene"
    }
"""
CYPHER_UpdateGene = """
    MATCH (n:Gene { identifier: {gene_id} })
    SET n += {
        name: {name},
        description: {description},
        chromosome: {chromosome}
    }
"""
CYPHER_VestigeGene = """
    MATCH (n:Gene { identifier: {gene_id} })
    SET n.vestige = TRUE
"""


def main():
    """Update SPOKE neo4j database from new Entrez Gene CSV data file.
    """
    import gzip
    from neo4j.v1 import GraphDatabase, basic_auth
    auth = basic_auth(SPOKE_User, SPOKE_Password)
    with gzip.open(EntrezGene_CSV) as gf, \
         GraphDatabase.driver(SPOKE_URI, auth=auth) as sdb:
        session = sdb.session()
        with session.begin_transaction() as tx:
            update_genes(tx, entrez_genes(gf))


def entrez_genes(gf):
    """Return gene_id->data map from opened CSV file.

    data = (name, description, chromosome)
    """
    import csv
    genes = {}
    reader = csv.DictReader(gf, delimiter='\t')
    for row in reader:
        if row["#tax_id"] != "9606" or row["type_of_gene"] != "protein-coding":
            # 9606 = Homo sapiens
            continue
        # GeneID is stored as an integer in SPOKE
        gene_id = int(row["GeneID"])
        name = row["Symbol"]
        description = row["description"]
        chromosome = row["chromosome"]
        genes[gene_id] = (name, description, chromosome)
    print("%d genes in new Entrez Gene" % len(genes))
    return genes


def update_genes(tx, new_genes, do_update=False):
    """Update SPOKE from gene_id->data map from Entrez Gene.

    data = (name, description, chromosome)
    Gene node fall into four categories:
      new: in Entrez Gene but not in SPOKE (add)
      same: in both and share same data (ignore)
      update: in both but have different data (update)
      vestige: in SPOKE but not in Entrez Gene (delete or ignore)
    """
    gene_new = 0
    gene_same = 0
    gene_update = 0
    gene_vestige = 0
    diff_name = 0
    diff_description = 0
    diff_chromosome = 0
    genes = {}
    result = tx.run(CYPHER_Genes)
    for r in result:
        gene_id = r["gene_id"]
        old_name = r["name"]
        try:
            new_gene_data = new_genes.pop(gene_id)
        except KeyError:
            gene_vestige += 1
            print("vestigial gene:", gene_id, old_name)
            if do_update:
                tx.run(CYPHER_VestigeGene, gene_id=gene_id)
        else:
            new_name, new_description, new_chromosome = new_gene_data
            old_description = r["description"]
            old_chromosome = r["chromosome"]
            old_gene_data = (old_name, old_description, old_chromosome)
            if old_gene_data == new_gene_data:
                gene_same += 1
            else:
                gene_update += 1
                if old_name != new_name:
                    diff_name += 1
                    #print("-- name", repr(old_name), repr(new_name))
                if old_description != new_description:
                    diff_description += 1
                    #print("-- description", repr(old_description),
                    #      repr(new_description))
                if old_chromosome != new_chromosome:
                    diff_chromosome += 1
                    #print("-- chromosome", repr(old_chromosome),
                    #      repr(new_chromosome))
                if do_update:
                    tx.run(CYPHER_UpdateGene, gene_id=gene_id, name=new_name,
                           description=new_description,
                           chromosome=new_chromosome)
    for new_id, new_gene_data in new_genes.items():
        gene_new += 1
        new_name, new_description, new_chromosome = new_gene_data
        if do_update:
            tx.run(CYPHER_AddGene, gene_id=new_id, name=new_name,
                   description=new_description, chromosome=new_chromosome)
    print("Genes:")
    print("  new:", gene_new)
    print("  same:", gene_same)
    print("  update:", gene_update)
    print("    name:", diff_name)
    print("    description:", diff_description)
    print("    chromosome:", diff_chromosome)
    print("  vestige:", gene_vestige)


if __name__ == "__main__":
    profile = False
    if not profile:
        main()
    else:
        import cProfile, pstats
        pr = cProfile.Profile()
        pr.runcall(main)
        stats = pstats.Stats(pr)
        stats.strip_dirs().sort_stats("cumulative").print_stats(20)
