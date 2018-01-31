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

PSQL_GatherPharmClassesToDrug = """
    SELECT s.cd_id, s.id, s.name, p.type, p.name, p.class_code, i.identifier FROM structures as s
    LEFT OUTER JOIN pharma_class as p on s.id = p.struct_id LEFT OUTER JOIN identifier as i on s.id = i.struct_id
    WHERE i.id_type = 'ChEMBL_ID' or i.id_type = 'DRUGBANK_ID';
"""

PSQL_GatherPharmClasses = """
    SELECT distinct(pc.id), pc."name", pc."type", pc.class_code, pc.source FROM pharma_class as pc;
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
    import psycopg2 as ps
    
    conn = ps.connect("host='localhost' dbname='drugcentral' user='kbharat'")
    cursor = conn.cursor()
    cursor.execute(PSQL_GatherPharmClassesToDrug)
    pharm_drug_records = cursor.fetchall()        
    
    cursor.execute(PSQL_GatherPharmClasses)
    pc_records = cursor.fetchall()
    
    print "Connecting to GraphDB"
    driver = GraphDatabase.driver("bolt://127.0.0.1/:7687", auth=basic_auth("neo4j", "neo4j2"))
    session = driver.session()
    print "Connected to DB!"

    double_dicts = drugcentral_process_psql(pharm_drug_records, session, pc_records)
    update_pharm_classes(session, double_dicts[1])
    update_pharm_drug(session, double_dicts[0])
    

def process_null(string):
    out_text = "None"
    if (string == "None"):
        out_text = "null"
    else:
        out_text = "'" + string + "'"
    return out_text

def drugcentral_process_psql(recs, session, pc_recs):
    drugbank_to_chembl = session.run(CYPHER_GatherDB_Specific)
    
    db_c = dict()
    for dc in drugbank_to_chembl:
        db_c.update({dc["c_id"]:dc["c_chembl"]})
    chembl_arr = db_c.values()
    db_arr = db_c.keys()

    dc_processed = []
    # s.cd_id, s.id, s.name, p.type, p.name, p.class_code, i.identifier
    for pr in recs:
        if not pr[3] is None and not pr[4] is None:
            if 'CHEMBL' in pr[6] and pr[6].strip() not in chembl_arr:
                rel_c = str(pr[6]) + "-" + str(pr[5])
                if not rel_c in dc_processed:
                    dc_processed.append(rel_c)
            elif 'DB' in pr[6] and pr[6].strip() in db_arr:
                rel_d = str(pr[6]) + "-" + str(pr[5])
                if not rel_d in dc_processed:
                    dc_processed.append(rel_d)

    pc_processed = dict()
    for pc in pc_recs:
        if not pc[3] in pc_processed.keys():
            pc_processed.update({pc[3].strip():{"Name": pc[1], "Type":pc[2], "Source": source_map[pc[4].strip()]["origin"]}})

    return dc_processed, pc_processed

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

        
main()
