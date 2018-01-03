#!/usr/bin/python2
from __future__ import print_function

from paths import ChEMBL_Database
from paths import SPOKE_URI, SPOKE_User, SPOKE_Password

SQL_QueryCompounds = """
    SELECT m.chembl_id,
           cs.standard_inchi,
           cs.standard_inchi_key,
           cs.canonical_smiles,
           m.pref_name
        FROM molecule_dictionary AS m
        LEFT JOIN compound_structures As cs
            ON cs.molregno = m.molregno;
"""
CYPHER_Compounds = """
    MATCH (n:Compound)
        WHERE exists(n.chembl_id)
        RETURN n.identifier       AS identifier,
               n.chembl_id        AS chembl_id,
               n.inchi            AS inchi,
               n.inchikey         AS inchikey,
               n.pref_name        AS pref_name,
               n.canonical_smiles AS smiles
"""
CYPHER_AddCompound = """
    CREATE (n:Compound)
    SET n = {
        identifier: {identifier},
        chembl_id: {chembl_id},
        inchi: {inchi},
        inchikey: {inchikey},
        pref_name: {pref_name},
        canonical_smiles: {smiles},
        source: 'ChEMBL'
    }
"""
CYPHER_UpdateCompound = """
    MATCH (n:Compound { identifier: {identifier} })
    SET n += {
        chembl_id: {chembl_id},
        inchi: {inchi},
        inchikey: {inchikey},
        pref_name: {pref_name},
        canonical_smiles: {smiles}
    }
"""
SQL_QueryInteracts = """
    SELECT m.chembl_id,
           d.action_type,
           t.chembl_id
        FROM Drug_mechanism      AS d,
             Molecule_dictionary AS m,
             target_dictionary   AS t
        WHERE d.molregno = m.molregno
            AND d.tid = t.tid
            AND t.organism = 'Homo sapiens'
            AND t.target_type = 'SINGLE PROTEIN';
"""
CYPHER_Interacts = """
    MATCH (c:Compound)-[r:INTERACTS_CiP]-(p:Protein)
        WHERE r.source = "ChEMBL"
        RETURN c.chembl_id   AS compound,
               p.chembl_id   AS protein,
               r.action_type AS action_type
"""
CYPHER_AddInteracts = """
    MATCH (c:Compound),
          (p:Protein)
    WHERE c.chembl_id = {compound_chembl}
        AND p.chembl_id = {protein_chembl}
    CREATE (c)-[r:INTERACTS_CiP { action_type: {action_type} }]-(p)
"""
CYPHER_UpdateInteracts = """
    MATCH (c:Compound)-[r:INTERACTS_CiP]-(p:Protein)
        WHERE   c.chembl_id = {compound_chembl}
            AND p.chembl_id = {protein_chembl}
        SET r += {
            action_type: {action_type}
        }
"""


def main():
    """Update SPOKE neo4j database from new ChEMBL sqlite3 database.

    Updates Compound nodes and INTERACTS_CiP edges.
    """
    import sqlite3
    from neo4j.v1 import GraphDatabase, basic_auth
    auth = basic_auth(SPOKE_User, SPOKE_Password)
    with sqlite3.connect(ChEMBL_Database) as cdb, \
         GraphDatabase.driver(SPOKE_URI, auth=auth) as sdb:
        c = cdb.cursor()
        session = sdb.session()
        with session.begin_transaction() as tx:
            update_compounds(tx, chembl_compounds(c))
        with session.begin_transaction() as tx:
            update_interacts(tx, chembl_interacts(c))


def chembl_compounds(c):
    """Return chembl_id->data map from ChEMBL.

    data = (inchi,inchikey,smiles,pref_name)
    """
    compounds = {}
    for row in c.execute(SQL_QueryCompounds):
        chembl_id, inchi, inchi_key, smiles, pref_name = row
        if inchi is None:
            inchi = ''
        if inchi_key is None:
            inchi_key = ''
        if smiles is None:
            smiles = ''
        if pref_name is None:
            pref_name = ''
        compounds[chembl_id] = (inchi, inchi_key, smiles, pref_name)
        if False:
            print("%s: %s (%s)" % (chembl_id, id, inchi_key))
    print("%d compounds in new ChEMBL" % len(compounds))
    return compounds


def update_compounds(session, new_compounds):
    """Update SPOKE from chembl_id->data map from ChEMBL.

    data = (inchi,inchikey,smiles,pref_name)
    Compound node fall into four categories:
      new: in ChEMBL but not in SPOKE (add)
      same: in both and share same data (ignore)
      update: in both but have different data (update)
      vestige: in SPOKE but not in ChEMBL (delete or ignore)
    """
    compound_new = 0
    compound_same = 0
    compound_update = 0
    compound_vestige = 0
    diff_inchi = 0
    diff_inchikey = 0
    diff_smiles = 0
    diff_pref_name = 0
    result = session.run(CYPHER_Compounds)
    for r in result:
        identifier = r["identifier"]
        chembl_id = r["chembl_id"]
        try:
            new_chembl_data = new_compounds.pop(chembl_id)
        except KeyError:
            compound_vestige += 1
            print("vestigial compound:", chembl_id, identifier)
        else:
            new_inchi, new_inchikey, new_smiles, new_pref_name = new_chembl_data
            old_inchi = r["inchi"]
            old_inchikey = r["inchikey"].strip()
            old_smiles = r["smiles"]
            old_pref_name = r["pref_name"].replace('"', "'")
            old_chembl_data = (old_inchi, old_inchikey,
                               old_smiles, old_pref_name)
            if old_chembl_data == new_chembl_data:
                compound_same += 1
            else:
                compound_update += 1
                if old_inchi != new_inchi:
                    diff_inchi += 1
                    #print("-- inchi", repr(old_inchi), repr(new_inchi))
                if old_inchikey != new_inchikey:
                    diff_inchikey += 1
                    #print("-- inchikey", repr(old_inchikey),
                    #      repr(new_inchikey))
                if old_smiles != new_smiles:
                    diff_smiles += 1
                    #print("-- smiles", repr(old_smiles), repr(new_smiles))
                if old_pref_name != new_pref_name:
                    diff_pref_name += 1
                    #print("-- pref_name", repr(old_pref_name),
                    #      repr(new_pref_name))
                session.run(CYPHER_UpdateCompound,
                            identifier=identifier, chembl_id=chembl_id,
                            inchi=new_inchi, inchikey=new_inchikey,
                            pref_name=new_pref_name, smiles=new_smiles)
    for new_id, new_chembl_data in new_compounds.items():
        compound_new += 1
        new_inchi, new_inchikey, new_smiles, new_pref_name = new_chembl_data
        session.run(CYPHER_AddCompound, identifier=new_id, chembl_id=new_id,
                    inchi=new_inchi, inchikey=new_inchikey,
                    pref_name=new_pref_name, smiles=new_smiles)
    print("Compounds:")
    print("  new:", compound_new)
    print("  same:", compound_same)
    print("  update:", compound_update)
    print("    inchi:", diff_inchi)
    print("    inchikey:", diff_inchikey)
    print("    smiles:", diff_smiles)
    print("    pref_name:", diff_pref_name)
    print("  vestige:", compound_vestige)


def chembl_interacts(c):
    """Return (compound,protein)->action_type map from ChEMBL.

    data = (inchi,inchikey,smiles,pref_name)
    compound and protein are ChEMBL ids.
    """
    interacts = {}
    for row in c.execute(SQL_QueryInteracts):
        (compound_chembl_id, action_type, protein_chembl_id) = row
        if action_type is None:
            continue
        # TODO: More checking if needed
        interacts[(compound_chembl_id, protein_chembl_id)] = action_type
        if False:
            print("%s(%s) %s %s(%s)" % (compound_chembl_id,
                                        compound_id,
                                        action_type,
                                        protein_chembl_id,
                                        protein_id))
    print("%d interactions in new ChEMBL" % len(interacts))
    return interacts


def update_interacts(session, new_interacts):
    """Update SPOKE from (compound,protein)->action_type map from ChEMBL.

    compound and protein are ChEMBL ids.
    INTERACTS_CiP edges fall into four categories:
      new: in ChEMBL but not in SPOKE (add)
      same: in both and share same data (ignore)
      update: in both but have different data (update)
      vestige: in SPOKE but not in ChEMBL (delete or ignore)
    """
    import pprint
    interact_new = 0
    interact_same = 0
    interact_update = 0
    interact_vestige = 0
    interacts = {}
    result = session.run(CYPHER_Interacts)
    for r in result:
        key = (r["compound"], r["protein"])
        old_action_type = r["action_type"]
        try:
            new_action_type = new_interacts.pop(key)
        except KeyError:
            interact_vestige += 1
        else:
            if new_action_type == old_action_type:
                interact_same += 1
            else:
                interact_update += 1
                session.run(CYPHER_UpdateInteracts,
                            compound_chembl=key[0],
                            protein_chembl=key[1],
                            action_type=new_action_type)
    for key, new_action_type in new_interacts.items():
        interact_new += 1
        session.run(CYPHER_AddInteracts,
                    compound_chembl=key[0],
                    protein_chembl=key[1],
                    action_type=new_action_type)
    print("Interacts:")
    print("  new:", interact_new)
    print("  same:", interact_same)
    print("  update:", interact_update)
    print("  vestige:", interact_vestige)


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
