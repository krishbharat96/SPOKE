from __future__ import print_function

from paths import DrugCentral_DB
from paths import SPOKE_URI, SPOKE_User, SPOKE_Password

CYPHER_AddPC = """
    CREATE (p:PharmacologicClass)
    SET p.name = '{name}', p.identifier = '{identifier}',
    p.license = 'CC BY 4.0', p.source = '{source}',
    p.url = '{url}', p.class_type = '{class_t}'
"""

CYPHER_VestigePC = """
    MATCH (p:PharmacologicClass) WHERE p.identifier = '{pc_id}'
    SET p.vestige = True
"""

CYPHER_UpdatePC = """
    MATCH (p:PharmacologicClass) WHERE p.identifier = '{identifier}' SET p.name = '{name}',
    p.class_type = '{class_t}', p.source = '{source}'
"""

CYPHER_ObtainPC = """
    MATCH (p:PharmacologicClass) WHERE p.name contains ''
    RETURN p.name as p_name, p.identifier as p_id, p.class_type as p_ct, p.source as p_src
"""

CYPHER_GatherDB_Specific = """
    MATCH (c:Compound) where c.identifier contains 'DB' return c.identifier as c_id,
    c.vestige as c_vest, c.chembl_id as c_chembl
"""

CYPHER_GatherPC_Drug = """
    MATCH (p)-[r:INCLUDES_PCiC]->(c) WHERE c.identifier contains '' and p.identifier contains '' RETURN c.identifier as c_id, p.identifier as p_id
"""

CYPHER_VestigePC_Drug = """
    MATCH (c:Compound)-[r:INCLUDES_PCiC]-(p:PharmacologicClass) WHERE c.identifier = '{c_id}'
    AND p.identifier = '{p_id}' SET r.vestige = true
"""

CYPHER_AddPC_Drug = """
    MATCH (c:Compound), (p:PharmacologicClass) WHERE c.identifier = '{c_id}'
    AND p.identifier = '{p_id}' CREATE (p)-[r:INCLUDES_PCiC]->(c) SET r.unbiased = false,
    r.license = 'CC BY 4.0', r.source = 'DrugCentral' RETURN c.identifier as comp_id
"""

class_type_map = {
    'MoA': 'Mechanism of Action',
    'PE': 'Physiologic Effect',
    'CS': 'Chemical Structure',
    'EPC': 'FDA Established Pharmacologic Class',
    'PA': 'Pharmacological Action',
    'has role': 'Application',
    'Chemical/Ingredient': 'Chemical/Ingredient',
}

source_map = {
    'FDA':{'origin': 'FDA via DrugCentral', 'url':'http://purl.bioontology.org/ontology/NDFRT/{identifier}'},
    'MeSH':{'origin':'MeSH via DrugCentral', 'url':'https://www.ncbi.nlm.nih.gov/mesh/{identifier}'},
    'CHEBI':{'origin':'CHEBI via DrugCentral', 'url':'http://www.ebi.ac.uk/chebi/searchId.do?chebiId={identifier}'},
    'FDA via DrugCentral':{'origin': 'FDA via DrugCentral', 'url':'http://purl.bioontology.org/ontology/NDFRT/{identifier}'},
    'MeSH via DrugCentral':{'origin':'MeSH via DrugCentral', 'url':'https://www.ncbi.nlm.nih.gov/mesh/{identifier}'},
    'CHEBI via DrugCentral':{'origin':'CHEBI via DrugCentral', 'url':'http://www.ebi.ac.uk/chebi/searchId.do?chebiId={identifier}'}
}

def main():
    from neo4j.v1 import GraphDatabase, basic_auth
    import postpy_drugcentral as ppy
    import gzip
    import pandas as pd
    
    
    file1 = gzip.open(DrugCentral_DB, 'r')
    file2 = gzip.open(DrugCentral_DB, 'r') # FILL IN DRUGCENTRAL DUMP FILENAME/PATH
    
    structures = ppy.get_table(file1, 'structures')
    file_arr = ppy.get_tables(file2, ['pharma_class', 'identifier'])
    pharma_class = file_arr[0]
    identifier = file_arr[1]
    
    file1.close()
    file2.close()

    queried_dict_1 = ppy.left_join(structures, 'id', pharma_class, 'struct_id', suffixl = "_struct", suffixr = "_pc")
    queried_dict_2 = ppy.left_join(queried_dict_1, 'id_struct', identifier, 'struct_id', suffixl = '_spc', suffixr = '_ident')

    qd_slim_1 = queried_dict_2[queried_dict_2['type'].notnull()]
    qd_slim_2 = qd_slim_1[(qd_slim_1.id_type == 'ChEMBL_ID')|(qd_slim_1.id_type == 'DRUGBANK_ID')]
    qd_slim_3 = ppy.select(qd_slim_2, ['cd_id', 'id_struct', 'name_struct', 'type', 'name_pc', 'class_code', 'identifier'])
    
    pc_slim = get_pc_slim(pharma_class)

    print "Connecting to GraphDB"
    driver = GraphDatabase.driver(SPOKE_URI, auth=basic_auth(SPOKE_User, SPOKE_Password))
    session = driver.session()
    print "Connected to DB!"

    dc_processed = drugcentral_process_pdf(qd_slim_3, session)

    update_pharm_classes(session, pc_slim)
    update_pharm_drug(session, dc_processed)
    
def get_pc_slim(pc_df):
    dict_pc = dict()
    for index, row in pc_df.iterrows():
        if not row['id'] in dict_pc:
            dict_pc.update({row['class_code']: {"Name":row['name'], "Type":row['type'], "Source":source_map[row['source']]['origin']}})
    return dict_pc

def process_null(string):
    out_text = "None"
    if (string == "None"):
        out_text = "null"
    else:
        out_text = "'" + string + "'"
    return out_text

def drugcentral_process_pdf(rel_df, session):
    drugbank_to_chembl = session.run(CYPHER_GatherDB_Specific)
    
    db_c = dict()
    for dc in drugbank_to_chembl:
        db_c.update({dc["c_id"]:dc["c_chembl"]})
    chembl_arr = db_c.values()
    db_arr = db_c.keys()

    dc_processed = []
    # s.cd_id, s.id, s.name, p.type, p.name, p.class_code, i.identifier
    for index, row in rel_df.iterrows():
        if not (row['type'] == "nan") and not (row['name_pc'] == "nan"):
            if 'CHEMBL' in row['identifier'] and row['identifier'] not in chembl_arr:
                rel_c = str(row['identifier']) + "-" + str(row['class_code'])
                if not rel_c in dc_processed:
                    dc_processed.append(rel_c)
            elif 'DB' in row['identifier'] and row['identifier'] in db_arr:
                rel_d = str(row['identifier']) + "-" + str(row['class_code'])
                if not rel_d in dc_processed:
                    dc_processed.append(rel_d)

    return dc_processed

def update_pharm_classes(session, new_pc):
    print "Updating Pharmacologic Classes Nodes ..."
    prev_dict = dict()
    gather_pharm = session.run(CYPHER_ObtainPC)
    for pc in gather_pharm:
        prev_dict.update({pc["p_id"]:{"Name":pc["p_name"], "Type":pc["p_ct"], "Source":pc["p_src"]}})

    count_vestige = 0
    count_add = 0
    count_update = 0
    vestige_arr = list(set(prev_dict.keys()) - set(new_pc.keys()))
    add_arr = list(set(new_pc.keys()) - set(prev_dict.keys()))
    
    for v in vestige_arr:
        count_vestige = count_vestige + 1
        session.run(CYPHER_VestigePC.format(pc_id=v))
        prev_dict.pop(v)

    for a in add_arr:
        count_add = count_add + 1
        session.run(CYPHER_AddPC.format(name=new_pc[a]["Name"], identifier=a, source=new_pc[a]["Source"],
                                        url=(source_map[new_pc[a]["Source"]]["url"]).format(identifier=a), class_t=new_pc[a]["Type"]))
        new_pc.pop(a)

    count_name = 0
    count_source = 0
    count_type = 0
    for pc_u in new_pc.keys():
        if not (sorted(prev_dict[pc_u]) == sorted(new_pc[pc_u])):
            count_update = count_update + 1
            session.run(CYPHER_UpdatePC.format(identifier=pc_u, name=new_pc[pc_u]["Name"], source=new_pc[pc_u]["Source"], class_t=new_pc[pc_u]["Type"]))
            if not prev_dict[pc_u]["Name"] == new_pc[pc_u]["Name"]:
                count_source = count_source + 1
            if not prev_dict[pc_u]["Source"] == new_pc[pc_u]["Source"]:
                count_source = count_source + 1
            if not prev_dict[pc_u]["Type"] == new_pc[pc_u]["Type"]:
                count_source = count_source + 1

    print """
Number of PCs Added : {count_add}
Number of Vestiges Flagged : {count_vestige}
Number of PCs Updated : {count_update}
    Name Diff : {count_name}
    Source Diff : {count_source}
    Type Diff : {count_type}
""".format(count_add=count_add, count_vestige=count_vestige, count_update=count_update, count_name=count_name, count_source=count_source, count_type=count_type)

def update_pharm_drug(session, new_rels):
    print "Updating Pharmacologic Class-Drug Relationships..."
    extract_old_rels = session.run(CYPHER_GatherPC_Drug)
    old_rels = []
    for rel in extract_old_rels:
        r = str(rel["c_id"]) + "-" + str(rel["p_id"])
        if not r in old_rels:
            old_rels.append(r)

    count_vestige = 0
    count_add = 0
    vestige_arr = list(set(old_rels) - set(new_rels))
    add_arr = list(set(new_rels) - set(old_rels))
    print "OLD : " + str(len(old_rels))
    print "NEW : " + str(len(new_rels))
    print "VESTIGE : " + str(len(vestige_arr))

    for v in vestige_arr:
        c_id = v.split("-")[0]
        p_id = v.split("-")[1]
        count_vestige = count_vestige + 1
        session.run(CYPHER_VestigePC_Drug.format(c_id=c_id, p_id=p_id))

    chembl_not_exist = []

    for a in add_arr:
        c_id_a = a.split("-")[0]
        p_id_a = a.split("-")[1]
        session.run(CYPHER_AddPC_Drug.format(c_id=c_id_a, p_id=p_id_a))
        res = session.run("MATCH (c:Compound) WHERE c.identifier = '{c_id_a}' RETURN c.identifier as c_id".format(c_id_a=c_id_a))
        var = ""
        for r in res:
            var = str(r["c_id"])
        
        if (var == ""):
            chembl_not_exist.append(c_id_a)
        else:
            count_add = count_add + 1

    print """
Number of PC-Drug Relationships Added : {count_add}
Number of PC-Drug Vestiges Flagged : {count_vestige}
ChEMBL IDs that do not exist in New ChEMBL version : {chembl_ne}
""".format(count_add=count_add, count_vestige=count_vestige, chembl_ne=str(chembl_not_exist))

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
