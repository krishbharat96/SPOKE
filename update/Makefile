.SUFFIXES:	.out .py
.py.out:
	/usr/bin/time -v python2 $< >& $@

all:	update_chembl.out \
		update_uniprot_chembl.out \
		update_mesh.out \
		update_entrez_gene.out

update_chembl.out: \
	paths.py \
	/databases/mol/spoke/chembl_23/chembl_23_sqlite/chembl_23.db
update_uniprot_chembl.out: \
	paths.py \
	/databases/mol/spoke/uniprot/HUMAN_9606_idmapping.dat.gz
update_mesh.out: \
	paths.py \
	/databases/mol/spoke/DiseaseOntology/doid-non-classified.obo
update_entrez_gene.out: \
	paths.py \
	/databases/mol/spoke/entrez/Homo_sapiens.gene_info.gz

