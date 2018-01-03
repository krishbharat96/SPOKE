#!/usr/bin/python2
from __future__ import print_function

from paths import SPOKE_URI, SPOKE_User, SPOKE_Password

CYPHER_Compounds = """
    MATCH (n:Compound)
        WHERE exists(n.canonical_smiles)
        RETURN n.identifier          AS identifier,
               n.canonical_smiles    AS canonical_smiles,
               n.standardized_smiles AS standardized_smiles
"""
CYPHER_UpdateSmiles = """
    MATCH (n:Disease { identifier:{identifier} } )
        SET n += { standardized_smiles:{smiles} }
"""


def main():
    from neo4j.v1 import GraphDatabase, basic_auth
    auth = basic_auth(SPOKE_User, SPOKE_Password)
    with GraphDatabase.driver(SPOKE_URI, auth=auth) as db:
        session = db.session()
        old_smiles = old_mappings(session)
        print("found %d old SMILES mappings" % len(old_smiles))
        with session.begin_transaction() as tx:
            update_mappings(tx, old_smiles)


def old_mappings(session):
    result = session.run(CYPHER_Compounds)
    return dict([(r["identifier"], (r["canonical_smiles"].strip(),
                                    r["standardized_smiles"].strip()))
                 for r in result])


def update_mappings(tx, old_smiles, do_update=False):
    from standardiser import standardise
    from standardiser.utils import StandardiseException
    from rdkit import Chem
    smiles_stat = {
        "empty": 0,
        "same": 0,
        "failed": 0,
        "update": 0,
    }
    count = 0
    for ident, values in old_smiles.items():
        canonical_smiles, old_std_smiles = values
        if not canonical_smiles:
            smiles_stat["empty"] += 1
            continue
        mol = Chem.MolFromSmiles(canonical_smiles)
        try:
            p = standardise.run(mol)
        except (StandardiseException, TypeError):
            smiles_stat["failed"] += 1
            # print("Failed standardisation: %s" % canonical_smiles)
            continue
        new_std_smiles = Chem.MolToSmiles(p)
        if old_std_smiles == new_std_smiles:
            smiles_stat["same"] += 1
        else:
            if do_update:
                tx.run(CYPHER_UpdateSmiles,
                       identifier=ident,
                       smiles=new_std_smiles)
            smiles_stat["update"] += 1
        count += 1
        if (count % 1000) == 0:
            print(count)
            import sys
            sys.stdout.flush()

    # Report some stats
    print("Standardized SMILES:")
    print("  empty canonical:", smiles_stat["empty"])
    print("  same:", smiles_stat["same"])
    print("  update:", smiles_stat["update"])
    print("  failed:", smiles_stat["failed"])


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
