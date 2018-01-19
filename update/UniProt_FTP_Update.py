CYPHER_AddProt = """
    CREATE (p:Protein)
    SET p.name = '{name}', p.identifier = '{identifier}',
    p.license = 'CC BY-NC 3.0', p.source = 'UniProt',
    p.description = '{desc}', p.chembl_id = {chembl_id},
    p.isoform = {isoform}, p.reviewed = '{revd}', p.vestige = False
"""

CYPHER_VestigeProt = """
    MATCH (p:Protein) WHERE p.identifier = '{prot_id}'
    SET p.vestige = True
"""

CYPHER_ObtainProt = """
    MATCH (p:Protein) WHERE p.identifier contains ''
    RETURN p.name as p_name, p.identifier as p_id, p.isoform as p_iso,
    p.description as p_desc, p.chembl_id as p_chembl, p.reviewed as p_rev
"""

CYPHER_UpdateProt = """
    MATCH (p:Protein) WHERE p.identifier = '{p_id}'
    SET p.name = '{p_name}', p.description = '{p_desc}', p.isoform = {p_isoform},
    p.chembl_id = {p_chembl}, p.reviewed = '{p_rev}', p.vestige = False
"""

CYPHER_ChangeIdentifier = """
    MATCH (p:Protein) WHERE p.identifier = '{p_id}'
    SET p.identifier = '{p_act}', p.vestige = False
"""

CYPHER_ToDeleteNode = """
    MATCH (p:Protein) WHERE p.identifier = '{p_id}'
    SET p.delete = True
"""

def elim_brackets(string1):
    import re
    string_a = string1.split("{")[0].strip()
    string_b = string_a.split("(")[0].strip()
    string = string_b.split("[")[0].strip()
    string = re.sub(r'\[.*\]', '', string).strip()
    string = re.sub(r'\([^()]*\)', '', string).strip()
    string = re.sub(r'\{[^()]*\}', '', string).strip()
    return string

def create_url(prot_id, fasta):
    url = ""
    if (fasta == True):
        url = "http://www.uniprot.org/uniprot/" + str(prot_id) + ".fasta"
    else:
        url = "http://www.uniprot.org/uniprot/" + str(prot_id)
    return url
    
def FASTA_maker(db_uniprot, prot_id, entry_name, prot_name, gene_name, org, is_canonical, isoform_name, iso_id, pe, sv):
    if (is_canonical == "true"):
        if not (gene_name == ""):
            fasta = ">" + str(db_uniprot) + "|" + str(prot_id) + "|" + str(entry_name) + " " + str(prot_name) + " " + "OS=" + str(org) +  " GN=" + str(gene_name) + " PE=" + pe + " SV=" + sv
        else:
            fasta = ">" + str(db_uniprot) + "|" + str(prot_id) + "|" + str(entry_name) + " " + str(prot_name) + " " + "OS=" + str(org) + " PE=" + pe + " SV=" + sv
    else:
        if not (gene_name == ""):
            fasta = ">" + str(db_uniprot) + "|" + str(iso_id) + "|" + str(entry_name) + " Isoform " + str(isoform_name) + " of " + str(prot_name) + " OS=" + str(org) + " GN=" + str(gene_name)
        else:
            fasta = ">" + str(db_uniprot) + "|" + str(iso_id) + "|" + str(entry_name) + " Isoform " + str(isoform_name) + " of " + str(prot_name) + " OS=" + str(org) 
    return fasta
        
def create_iso(iso_dict, pe, sv, gene_arr, os, prot_id, db, name, desc):
    if len(iso_dict.keys()) > 0:
        iso_arr = []
        for iso_key in iso_dict.keys():
            iso_name = iso_key
            if (db == "SwissProt") : iso_db = "sp"
            else : iso_db = "tr"
            if (len(gene_arr) > 0):
                iso_gene = gene_arr[0]
            else:
                iso_gene = ""
                print "Gene Info not exists for : " + prot_id
            iso_url = create_url(prot_id, True)
            iso_id = iso_dict[iso_key]["isoid"]
            if (iso_dict[iso_key]["isoseq"].lower().strip() == "displayed"):
                iso_is_canonical = "true"
            else:
                iso_is_canonical = "false"
            iso_fasta = FASTA_maker(db, prot_id, name, elim_brackets(desc), iso_gene, elim_brackets(os), iso_is_canonical, "Isoform " + str(iso_name), iso_id, pe, sv)
            iso_str = "Name:Isoform" + str(iso_key) + "~FASTA:" + str(iso_fasta) + "~FASTA_Source:" + str(iso_url) + "~Is_Canonical:" + str(iso_is_canonical)
            iso_arr.append(iso_str)
        return iso_arr
    else:
        return False

def update_uniprot(session, uniprot_dict):
    count_vestige = 0
    count_add = 0
    count_update = 0
    count_old_ids = 0
    count_todel = 0
    
    prot_records = session.run(CYPHER_ObtainProt)
    prev_dict = dict()

    print "Gathering old protein records..."
    for r in prot_records:
        if r["p_iso"] is None:
            p_iso_rn = str(r["p_iso"])
        else:
            p_iso_rn = sorted(r["p_iso"])
        prev_dict.update({r["p_id"]:{"p_name":r["p_name"], "p_desc":r["p_desc"], "p_isoform":p_iso_rn, "p_chembl":str(r["p_chembl"]), "p_rev":r["p_rev"]}})
    print "Gathered!" 
    vestige_arr = list(set(prev_dict.keys()) - set(uniprot_dict.keys()))
    add_arr = list(set(uniprot_dict.keys()) - set(prev_dict.keys()))

    print "Setting Vestiges..."
    vestige_dict = dict()
    for key in uniprot_dict.keys():
        if not (uniprot_dict[key]["Prev_ids"] == ""):
            for item in uniprot_dict[key]["Prev_ids"]:
                vestige_dict.update({item:key})
                
    for vestige in vestige_arr:
        if vestige in vestige_dict and not vestige_dict[vestige] in prev_dict:
            count_old_ids = count_old_ids + 1
            if vestige in add_arr:
                add_arr.pop(vestige)
            session.run(CYPHER_ChangeIdentifier.format(p_id=vestige, p_act=vestige_dict[vestige]))
            temp_dict = prev_dict[vestige]
            prev_dict.update({vestige_dict[vestige]:temp_dict})
            prev_dict.pop(vestige)
        elif vestige in vestige_dict and vestige_dict[vestige] in prev_dict:
            count_todel = count_todel + 1
            session.run(CYPHER_ToDeleteNode.format(p_id=vestige))
        else:
            count_vestige = count_vestige + 1
            session.run(CYPHER_VestigeProt.format(prot_id=vestige))
            prev_dict.pop(vestige)
    print "Vestiges Set!"

    print "Adding New Proteins"
    for add_prot in add_arr:
        count_add = count_add + 1
        # def create_iso(iso_dict, pe, sv, gene_arr, os, prot_id, db, name, desc):
        p_iso = create_iso(uniprot_dict[add_prot]["Isoform"], uniprot_dict[add_prot]["PE"], uniprot_dict[add_prot]["SV"], uniprot_dict[add_prot]["Gene"], uniprot_dict[add_prot]["Organism"], add_prot, uniprot_dict[add_prot]["DB"], uniprot_dict[add_prot]["Name"], uniprot_dict[add_prot]["Description"])
        str_iso = chembl_id = db_rev = ""
        if not (p_iso == False):
            str_iso = str(p_iso)
        else:
            str_iso = "Null"

        if (uniprot_dict[add_prot]["Chembl_id"].strip() == ""):
            chembl_id = "Null"
        else:
            chembl_id = "'" + uniprot_dict[add_prot]["Chembl_id"] + "'"

        if (uniprot_dict[add_prot]["DB"] == "SwissProt"):
            db_rev = "Reviewed, From SwissProt"
        else:
            db_rev = "Unreviewed, From TrEMBL"
            
        session.run(CYPHER_AddProt.format(name=uniprot_dict[add_prot]["Name"], identifier=add_prot, desc=uniprot_dict[add_prot]["Description"], isoform=str_iso, chembl_id=chembl_id, revd=db_rev))
        uniprot_dict.pop(add_prot)
    print "New Entries Added!"

    print "Updating Existing Proteins..."
    count_chembl = 0
    count_desc = 0
    count_isoform = 0
    count_rev = 0
    count_name = 0
    
    for key in uniprot_dict.keys():
        iso_arr = create_iso(uniprot_dict[key]["Isoform"], uniprot_dict[key]["PE"], uniprot_dict[key]["SV"], uniprot_dict[key]["Gene"], uniprot_dict[key]["Organism"], key, uniprot_dict[key]["DB"], uniprot_dict[key]["Name"], uniprot_dict[key]["Description"])

        if (iso_arr == False):
            iso_arr = "None"
        else:
            iso_arr = sorted(iso_arr)
            
        if (uniprot_dict[key]["Chembl_id"].strip() == ""):
            chembl_p = "None"
            chembl_prot = "Null"
        else:
            chembl_p = uniprot_dict[key]["Chembl_id"]
            chembl_prot = "'" + uniprot_dict[key]["Chembl_id"] + "'"

        if (uniprot_dict[key]["DB"].strip() == "SwissProt"):
            revd_p = "Reviewed, From SwissProt"
        else:
            revd_p = "Unreviewed, From TrEMBL"
            
        single_new_dict = {key:{"p_name":uniprot_dict[key]["Name"], "p_desc":uniprot_dict[key]["Description"], "p_isoform":iso_arr, "p_chembl":chembl_p, "p_rev":revd_p}}
        if not sorted(single_new_dict[key]) == sorted(prev_dict[key]):
            count_update = count_update + 1
            if not (single_new_dict[key]["p_name"] == prev_dict[key]["p_name"]):
                count_name = count_name + 1
            if not (single_new_dict[key]["p_desc"] == prev_dict[key]["p_desc"]):
                count_desc = count_desc + 1
            if not (single_new_dict[key]["p_isoform"] == prev_dict[key]["p_isoform"]):
                count_iso = count_iso + 1
            if not (single_new_dict[key]["p_chembl"] == prev_dict[key]["p_chembl"]):
                count_chembl = count_chembl + 1
            if not (single_new_dict[key]["p_rev"] == prev_dict[key]["p_rev"]):
                count_rev = count_rev + 1
            
            session.run(CYPHER_UpdateProt.format(p_id=key, p_name=single_new_dict[key]["p_name"], p_desc=single_new_dict[key]["p_desc"], p_isoform=str(single_new_dict[key]["p_isoform"]), p_chembl=chembl_prot, p_rev=revd_p))

    print """
    Number Added : {ca}
    Number of Vestiges Flagged : {cv}
    Number of Old Accession IDs Changed : {co}
    Number of Nodes to be Deleted : {cd}
    Number Updated : {cu}
        Different Description : {cud}
        Different Name : {cun}
        Different Isoform Information : {cui}
        Different ChEMBL ID : {cuc}
        Different Database Information : {cur}
    """.format(ca=str(count_add), cv=str(count_vestige), co = str(count_old_ids), cd = str(count_todel), cu=str(count_update), cud=str(count_desc), cun=str(count_name), cui=str(count_isoform), cuc=str(count_chembl), cur=str(count_rev))

def get_sprot():
    import urllib
    from urllib import urlretrieve
    sp_url = """
    ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_sprot_human.dat.gz
    """
    urllib.urlretrieve(sp_url, 'sp_prot.dat.gz')
    urllib.urlcleanup()

def get_trembl():
    import urllib
    from urllib import urlretrieve
    tr_url = """
    ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/uniprot_trembl_human.dat.gz
    """
    urllib.urlretrieve(tr_url, 'tr_prot.dat.gz')
    urllib.urlcleanup()

def main():
    import uniprot_dat_parser as udp
    import gzip
    from gzip import GzipFile
    import shutil
    import csv
    from neo4j.v1 import GraphDatabase, basic_auth

    get_sprot()
    get_trembl()

    with gzip.open('sp_prot.dat.gz', 'rb') as sp_in, open('sp_prot.dat', 'wb') as sp_out:
        shutil.copyfileobj(sp_in, sp_out)

    with gzip.open('tr_prot.dat.gz', 'rb') as tr_in, open('tr_prot.dat', 'wb') as tr_out:
        shutil.copyfileobj(tr_in, tr_out)
    
    print "Parsing uniprot files..."
    sp_dict = udp.parse("uniprot_sprot_human.dat")
    print len(sp_dict)
    tr_dict = udp.parse("uniprot_trembl_human.dat")
    print len(tr_dict)
    
    uniprot_dict = sp_dict.copy()
    uniprot_dict.update(tr_dict)
    print len(uniprot_dict)
    print "UniProt Files Parsed!"
    
    driver = GraphDatabase.driver("bolt://127.0.0.1/:7687", auth=basic_auth("neo4j", "neo4j2"))
    session = driver.session()
    update_uniprot(session, uniprot_dict)

main()
    

                       
