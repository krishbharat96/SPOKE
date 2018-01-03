#!/usr/bin/python2
from __future__ import print_function

from paths import UniprotChEMBLMapping_Human as UCMapping
from paths import SPOKE_URI, SPOKE_User, SPOKE_Password

CYPHER_ProteinUniprot = """
    MATCH (n:Protein)
        RETURN n.identifier AS identifier,
               n.chembl_id  AS chembl
"""
CYPHER_GeneUniprot = """
    MATCH (n:Gene)
        WHERE exists(n.identifier)
        RETURN n.identifier AS identifier,
               n.chembl_id  AS chembl
"""
CYPHER_UpdateProteinChembl = """
    MATCH (n:Protein { identifier: {identifier} })
    SET n += {
        chembl_id: {chembl_id}
    }
"""
CYPHER_UpdateGeneChembl = """
    MATCH (n:Gene { identifier: {identifier} })
    SET n += {
        chembl_id: {chembl_id}
    }
"""


def main():
    import gzip
    from neo4j.v1 import GraphDatabase, basic_auth
    auth = basic_auth(SPOKE_User, SPOKE_Password)
    with GraphDatabase.driver(SPOKE_URI, auth=auth) as db:
        session = db.session()
        old_protein = old_protein_uniprot(session)
        old_gene = old_gene_geneid(session)
        print("found %d old uniprot-chembl mappings" % len(old_protein))
        #for uniprot, chembl in old_protein.items():
        #    print(uniprot, chembl, "old")
        print("found %d old geneid-chembl mappings" % len(old_gene))
        with gzip.open(UCMapping, "r") as f, session.begin_transaction() as tx:
            update_mappings(tx, f, old_protein, old_gene)


def old_protein_uniprot(session):
    result = session.run(CYPHER_ProteinUniprot)
    return dict([(r["identifier"], r["chembl"]) for r in result])


def old_gene_geneid(session):
    result = session.run(CYPHER_GeneUniprot)
    return dict([(r["identifier"], r["chembl"]) for r in result])


def update_mappings(session, f, old_protein, old_gene):
    # First we loop over the input to process Uniprot-ChEMBL
    # mappings that are used to update Protein nodes immediately.
    # We also keep track of Uniprot-GeneID mappings so that we
    # can update Gene nodes later.  (We cannot do it immediately
    # because we may see a Uniprot-GeneID mapping before we see
    # the corresponding Uniprot-ChEMBL mapping and will not have
    # enough data to update the Gene node properly.)
    uniprot2chembl = {}     # Uniprot->ChEMBL, 1:1
    geneid2uniprot = {}     # GeneID->Uniprot, 1:N
    protein_new = 0
    protein_same = 0
    protein_update = 0
    protein_vestige = 0
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) != 3:
            print("bad line", line)
        else:
            uniprot, db, db_id = parts
            if db == "ChEMBL":
                # Always keep in map for gene update later
                uniprot2chembl[uniprot] = db_id
                # If there is a protein with this Uniprot id,
                # remove it from the old list and potentially
                # update it.  We remove it so that old_protein
                # will contain Uniprot ids that had ChEMBL id
                # in SPOKE but have no ChEMBL id in the new
                # mapping from Uniprot.
                try:
                    old_chembl = old_protein.pop(uniprot)
                    if not old_chembl:
                        raise KeyError("empty protein ChEMBL id")
                except KeyError:
                    #print("protein new", uniprot, db_id)
                    protein_new += 1
                else:
                    if db_id == old_chembl:
                        #print("protein same", uniprot, db_id)
                        protein_same += 1
                    else:
                        #print("protein update", uniprot, old_chembl, "->", db_id)
                        protein_update += 1
                        session.run(CYPHER_UpdateProteinChembl,
                                    identifier=uniprot,
                                    chembl_id=db_id)
            elif db == "GeneID":
                # Just keep GeneID-Uniprot mapping because we may not
                # have seen the Uniprot-ChEMBL mapping yet.
                try:
                    # Apparently that is how SPOKE wants it
                    geneid = int(db_id)
                except ValueError:
                    pass
                else:
                    try:
                        geneid2uniprot[geneid].append(uniprot)
                    except KeyError:
                        geneid2uniprot[geneid] = [uniprot]
    for uniprot, old_chembl in old_protein.items():
        if old_chembl:
            #print("protein vestige", uniprot, old_chembl)
            protein_vestige += 1

    # Loop over all new GeneId-Uniprot pairs and see if there is
    # a corresponding ChEMBL id.  If so, update the Gene node.
    gene_new = 0
    gene_same = 0
    gene_update = 0
    gene_vestige = 0
    for geneid, uniprot_list in geneid2uniprot.items():
        old_chembl = old_gene.pop(geneid, None)
        # Find a ChEMBL id for this GeneID, preferentially
        # keeping the existing one if still valid
        new_chembl = None
        for uniprot in uniprot_list:
            try:
                new_chembl = uniprot2chembl[uniprot]
            except KeyError:
                # This GeneID maps to a Uniprot id that does not
                # have a corresponding ChEMBL id, so ignore it.
                pass
            else:
                # If old ChEMBL id is still valid, keep it
                if old_chembl == new_chembl:
                    break
        if not new_chembl:
            # Did not find a valid ChEMBL for this GeneID, skip
            continue
        if not old_chembl:
            #print("gene new", geneid, new_chembl)
            gene_new += 1
        else:
            if new_chembl == old_chembl:
                #print("gene same", geneid, new_chembl)
                gene_same += 1
            else:
                #print("gene update", geneid, old_chembl, "->", new_chembl)
                gene_update += 1
                session.run(CYPHER_UpdateGeneChembl,
                            identifier=geneid,
                            chembl_id=new_chembl)
    for geneid, old_chembl in old_gene.items():
        if old_chembl:
            #print("gene vestige", geneid, old_chembl)
            gene_vestige += 1

    # Report some stats
    print("Proteins:")
    print("  new:", protein_new)
    print("  same:", protein_same)
    print("  update:", protein_update)
    print("  vestige:", protein_vestige)
    print("Genes:")
    print("  new:", gene_new)
    print("  same:", gene_same)
    print("  update:", gene_update)
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
