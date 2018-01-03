This directory contains scripts for updating the SPOKE neo4j
database contents from data files downloaded from various databases.

An important goal is to simplify code maintenance, so all of
the update scripts follow this pattern:

- All the CYPHER code is at the beginning, so it's easy to tell what
  nodes and properties the code will read and write.

- A main program is there to open all the relevant data stores.
  SPOKE is opened there, as well as any input data files/databases.
  The main program then calls the new-data-reader function to get
  the new data as a dictionary, which is then passed to the
  SPOKE-updater function.  The updates are done in a single
  transaction so a failure in the SPOKE-updater function should
  leave SPOKE unchanged.

- The new-data-reader function is responsible for building a
  dictionary whose key uniquely identifies each entry in a way
  that is compatible with SPOKE labels and properties, and whose
  value is a tuple that contains all available new data that
  match any SPOKE properties.  The list of properties used is
  static (rather than built dynamically by querying SPOKE) because
  changes in schema (or whatever one calls the list of node
  properties) should be analyzed carefully before any associated
  data is "dumped in".

- The SPOKE-updater function queries for all existing SPOKE
  entries and classifies them as one of
  
  * "vestigial" (does not appear in new data),
  * "same" (all properties match entry in new data), or
  * "update" (at least one SPOKE property does not match  new data).
  
  Entries in the new data dictionary that match SPOKE entries are
  removed from the dictionary; any entries remaining after all SPOKE
  entries have been processed are considered "new" entries, which
  are added to SPOKE if appropriate.
