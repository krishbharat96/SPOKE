def db_path(filename):
    import os.path
    return os.path.join(DatabaseDir, filename)

#
# Path to where database updates for SPOKE are kept
#
DatabaseDir = "/databases/mol/spoke"

#
# ChEMBL sqlite3 database downloaded from
#   ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/
# TODO: need a way to handle version number
#
ChEMBL_URL = "ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/chembl_\*_sqlite.tar.gz"
ChEMBL_Output = db_path("chembl/chembl_*/chembl_*_sqlite/chembl_*.db")
ChEMBL_Database = db_path("chembl/chembl.db")

#
# Uniprot-ChEMBL mapping downloaded from
#   ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/
#
UniprotChEMBL_URL_HUMAN = "ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/by_organism/HUMAN_9606_idmapping.dat.gz"
UniprotChEMBLMapping_All = db_path("uniprot/idmapping.dat.gz")
UniprotChEMBLMapping_Human = db_path("uniprot/HUMAN_9606_idmapping.dat.gz")

# DiseaseOntology-MeSH mapping downloaded from
#   https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/master/src/ontology/doid-non-classified.obo
DiseaseOntology_URL = "https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/master/src/ontology/doid-non-classified.obo"
DiseaseOntologyOBO = db_path("DiseaseOntology/doid-non-classified.obo")

#
# Entrez Gene database downloaded from
#   ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz
#
EntrezGene_URL = "ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz"
EntrezGene_CSV = db_path("entrez/Homo_sapiens.gene_info.gz")

#
# Uniprot isoforms FASTA file downloaded from
#   ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot_varsplic.fasta.gz
#
UniprotIsoformFASTA = db_path("uniprot/uniprot_sprot_varsplic.fasta.gz")

#
# Development SPOKE neo4j network database
# TODO: need to handle user and password securely
#
SPOKE_URI = "bolt://spoke.cgl.ucsf.edu/:7687"
SPOKE_User = "neo4j"
SPOKE_Password = "SPOKEdev"
