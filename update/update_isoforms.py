#!/usr/bin/python2
from __future__ import print_function

from paths import DiseaseOntologyOBO
from paths import SPOKE_URI, SPOKE_User, SPOKE_Password

CYPHER_Disease = """
    MATCH (n:Disease)
        WHERE exists(n.identifier)
        RETURN n.identifier AS identifier,
               n.mesh_id    AS mesh
"""
CYPHER_UpdateMeSH = """
    MATCH (n:Disease { identifier:{identifier} } )
        SET n += { mesh_id:{mesh} }
"""


def main():
    from neo4j.v1 import GraphDatabase, basic_auth
    auth = basic_auth(SPOKE_User, SPOKE_Password)
    with GraphDatabase.driver(SPOKE_URI, auth=auth) as db:
        session = db.session()
        old_mesh = old_disease_mesh(session)
        print("found %d old disease-MeSH mappings" % len(old_mesh))
        with open(DiseaseOntologyOBO, "r") as f, \
             session.begin_transaction() as tx:
            update_mappings(tx, f, old_mesh)


def old_disease_mesh(session):
    result = session.run(CYPHER_Disease)
    return dict([(r["identifier"], r["mesh"]) for r in result])


def update_mappings(session, f, old_mesh):
    mesh_stat = {
        "new": 0,
        "same": 0,
        "update": 0,
        "vestige": 0,
    }
    disease_mesh = {}
    updated = set()
    version = None
    is_a = {}
    current_mesh = set()
    for line in f:
        line = line.strip()
        if version is None:
            parts = [p.strip() for p in line.split(':')]
            if len(parts) == 2 and parts[0] == "format-version":
                if parts[1] != "1.2":
                    raise ValueError("unexpected OBO format version: %s != 1.2"
                                     % parts[1])
                else:
                    version = parts[1]
                    do_id = None
        elif line == "[Term]":
            if len(current_mesh) > 1:
                print("Multiple MeSH terms for %s: %s"
                      % (do_id, ", ".join(current_mesh)))
            do_id = None
            current_mesh = set()
        else:
            parts = [p.strip() for p in line.split(':', 1)]
            if parts[0] == "id":
                subparts = [p.strip() for p in parts[1].split(':')]
                if len(subparts) == 2 and subparts[0] == "DOID":
                    # Keep "DOID:" as part of the identifier
                    do_id = parts[1]
            elif parts[0] == "xref":
                subparts = [p.strip() for p in parts[1].split(':')]
                if len(subparts) == 2 and subparts[0] == "MESH":
                    mesh_id = subparts[1]
                    if do_id and mesh_id:
                        _update_mesh(session, do_id, mesh_id, old_mesh, updated,
                                     disease_mesh, mesh_stat)
                        current_mesh.add(mesh_id)
            elif parts[0] == "is_a":
                is_a[do_id] = parts[1].split()[0]
    if len(current_mesh) > 1:
        print("Multiple MeSH terms for %s: %s"
              % (do_id, ", ".join(current_mesh)))
    for do_id in is_a.keys():
        #print(do_id)
        orig_do_id = do_id
        while do_id not in updated:
            try:
                do_id = is_a[do_id]
            except KeyError:
                #print(" ", do_id, "no more is_a")
                break
            try:
                mesh_id = disease_mesh[do_id]
            except KeyError:
                #print(" ", do_id, "no mesh_id")
                pass
            else:
                #print(" ", do_id, "got mesh_id", mesh_id)
                _update_mesh(session, orig_do_id, mesh_id, old_mesh, updated,
                             disease_mesh, mesh_stat)
                break
    for do_id, old_mesh_id in old_mesh.items():
        if old_mesh:
            print("mesh vestige", do_id, old_mesh_id)
            mesh_stat["vestige"] += 1

    # Report some stats
    print("MeSH terms:")
    print("  same:", mesh_stat["same"])
    print("  update:", mesh_stat["update"])
    print("  disease not in SPOKE:", mesh_stat["new"])
    print("  vestige:", mesh_stat["vestige"])


def _update_mesh(session, do_id, mesh_id, old_mesh, updated,
                 disease_mesh, mesh_stat):
    updated.add(do_id)
    disease_mesh[do_id] = mesh_id
    try:
        old_mesh_id = old_mesh.pop(do_id)
        if not old_mesh_id:
            raise KeyError("empty disease MeSH id")
    except KeyError:
        #print("mesh new", do_id, mesh_id)
        session.run(CYPHER_UpdateMeSH, identifier=do_id, mesh=mesh_id)
        mesh_stat["new"] += 1
    else:
        if mesh_id == old_mesh_id:
            #print("mesh same", do_id, mesh_id)
            mesh_stat["same"] += 1
        else:
            #print("mesh update", do_id, old_mesh_id, "->", mesh_id)
            session.run(CYPHER_UpdateMeSH, identifier=do_id, mesh=mesh_id)
            mesh_stat["update"] += 1


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
